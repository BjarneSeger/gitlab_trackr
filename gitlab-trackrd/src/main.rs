//! `gitlab-trackrd` — GitLab time-tracking varlink daemon.

use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod boards;
mod cache;
mod config;
mod db;
mod error;
mod gitlab;
mod handlers;
mod history;
mod queue;
mod reconnect;
mod reload;
mod secrets;
mod server;
mod service;

use boards::BoardCache;
use cache::IssueCache;
use error::{DormancyReason, Result};
use gitlab::GitlabClient;
use handlers::{ConnState, Handlers, Session, SessionSlot};
use history::HistoryCache;
use queue::RetryQueue;
use service::ServiceHandler;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("GITLAB_TRACKRD_LOG")
                .unwrap_or_else(|_| EnvFilter::new("gitlab_trackrd=info")),
        )
        .init();

    let config = match config::load_shared() {
        Ok(config) => config,
        Err(e) => {
            error!(error = %e, "failed to load configuration");
            std::process::exit(1);
        }
    };
    let socket = config.read().unwrap().server.resolved_socket();
    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| "~/.local/share".into())
        .join("gitlab-trackrd");
    // One-time cleanup of the pre-fjall redb stores; their data was cache and
    // is repopulated by the regular sync.
    for file in [
        "cache.redb",
        "boards.redb",
        "history.redb",
        "queue.redb",
        "dead_letter.redb",
    ] {
        if let Err(e) = std::fs::remove_file(data_dir.join(file))
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(error = %e, file, "failed to remove legacy redb store");
        }
    }
    let db_dir = data_dir.join("db");

    // Credentials come only from the OS keychain (set via `tt login`). The
    // daemon never refuses to start: any failure here leaves it *dormant*
    // (serving, but returning `NotAuthenticated`). The dormancy reason is kept
    // in the session slot so the CLI can report a specific cause.
    let initial: ConnState = match secrets::load().await {
        Err(e) => {
            warn!(error = %e, "keychain read failed; starting dormant");
            ConnState::Dormant(DormancyReason::KeychainError(e.to_string()))
        }
        Ok(None) => {
            info!("no credentials available; daemon starting dormant (run `tt login`)");
            ConnState::Dormant(DormancyReason::NoCredentials)
        }
        Ok(Some(c)) => match GitlabClient::connect(&c.host, &c.token).await {
            Ok(client) => {
                let s = Session::from_client(client);
                info!(host = %s.host, user_id = s.user_id, "initial GitLab connection ready");
                ConnState::Connected(s)
            }
            Err(e) => {
                warn!(error = %e, host = %c.host, "initial GitLab connection failed; daemon starting dormant");
                ConnState::Dormant(DormancyReason::from_connect_error(&c.host, &e))
            }
        },
    };
    let session: SessionSlot = Arc::new(RwLock::new(initial));

    std::fs::create_dir_all(&db_dir)?;
    let db = fjall::Database::builder(&db_dir).open()?;
    let cache = Arc::new(IssueCache::open(&db)?);
    let boards = Arc::new(BoardCache::open(&db)?);
    let history = Arc::new(HistoryCache::open(&db)?);
    let queue = RetryQueue::new(Arc::clone(&session), &db, Arc::clone(&config))?;
    // Woken when a runtime GitLab failure demotes the session to
    // `Dormant(Unreachable)`, so the reconnect supervisor re-engages mid-run and
    // not only at boot. See `reconnect::spawn`.
    let reconnect_signal = Arc::new(Notify::new());
    let handlers = Arc::new(Handlers {
        session,
        cache,
        boards,
        history,
        queue,
        config: Arc::clone(&config),
        reconnect_signal,
    });

    reload::spawn(Arc::clone(&config));

    // If the daemon booted dormant because GitLab was unreachable, retry the
    // connection in the background with exponential backoff. A successful
    // reconnect flips the shared session to `Connected`, which drains the retry
    // queue and resumes the refresh loops. No-op when already connected or when
    // dormancy needs the user (bad token / logged out).
    reconnect::spawn(Arc::clone(&handlers));

    let listener = server::make_listener(&socket)?;

    let (quick_interval, slow_interval) = {
        let c = config.read().unwrap();
        (c.refresh.quick.interval_secs, c.refresh.slow.interval_secs)
    };
    if server::is_socket_activated() {
        info!(
            quick_interval,
            slow_interval, "starting gitlab-trackrd from socket"
        );
    } else {
        info!(
            socket = socket,
            quick_interval, slow_interval, "starting gitlab-trackrd"
        );
    }

    // One-shot startup warm-up: refresh issues/boards/active first so the
    // issue cache is populated, then backfill the full stale history window so
    // the older, never-refreshed tiers are filled. Order matters — history
    // enrichment reads project IDs from the issue cache.
    {
        let handlers_ref = Arc::clone(&handlers);
        tokio::spawn(async move {
            info!("startup cache warm-up triggered");
            handlers_ref.warm_up().await;
        });
    }

    // Quick tier: refresh issues, boards, and the recent history window every
    // `refresh.quick.interval_secs`. The interval is re-read each tick so a
    // config reload takes effect after the current sleep.
    {
        let handlers_ref = Arc::clone(&handlers);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            loop {
                let interval = config.read().unwrap().refresh.quick.interval();
                tokio::time::sleep(interval).await;
                info!("background cache refresh triggered");
                handlers_ref.refresh_cache().await;
            }
        });
    }

    // Slow tier: re-poll the bulk history window once a day and prune anything
    // past the retention horizon.
    {
        let handlers_ref = Arc::clone(&handlers);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            loop {
                let interval = config.read().unwrap().refresh.slow.interval();
                tokio::time::sleep(interval).await;
                info!("daily history refresh triggered");
                handlers_ref.refresh_history_daily().await;
            }
        });
    }

    let serve = server::serve(Arc::new(ServiceHandler::new(handlers)), listener);

    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        result = serve => result?,
        _ = tokio::signal::ctrl_c() => { info!("received SIGINT, shutting down"); }
        _ = sigterm.recv() => { info!("received SIGTERM, shutting down"); }
    }

    if !server::is_socket_activated() {
        let _ = std::fs::remove_file(&socket);
    }
    Ok(())
}
