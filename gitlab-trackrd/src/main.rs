//! `gitlab-trackrd` — GitLab time-tracking varlink daemon.

use std::sync::Arc;
use tokio::sync::RwLock;
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
mod reload;
mod secrets;
mod server;
mod service;

use boards::BoardCache;
use cache::IssueCache;
use error::Result;
use gitlab::GitlabClient;
use handlers::{Handlers, Session, SessionSlot};
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
    let db_path = dirs::data_local_dir()
        .unwrap_or_else(|| "~/.local/share".into())
        .join("gitlab-trackrd/cache.redb");

    let session: SessionSlot = Arc::new(RwLock::new(None));

    // Credentials come only from the OS keychain (set via `tt login`).
    let initial = match secrets::load().await {
        Ok(opt) => opt,
        Err(e) => {
            warn!(error = %e, "keychain read failed; starting dormant");
            None
        }
    };
    if let Some(c) = initial {
        match GitlabClient::connect(&c.host, &c.token).await {
            Ok(client) => {
                let s = Session::from_client(client);
                info!(host = %s.host, user_id = s.user_id, "initial GitLab connection ready");
                *session.write().await = Some(s);
            }
            Err(e) => {
                warn!(error = %e, host = %c.host, "initial GitLab connection failed; daemon starting dormant")
            }
        }
    } else {
        info!("no credentials available; daemon starting dormant (run `tt login`)");
    }

    let cache = Arc::new(IssueCache::open(&db_path)?);
    let boards_db_path = db_path.with_file_name("boards.redb");
    let boards = Arc::new(BoardCache::open(&boards_db_path)?);
    let history_db_path = db_path.with_file_name("history.redb");
    let history = Arc::new(HistoryCache::open(&history_db_path)?);
    let queue_db_path = db_path.with_file_name("queue.redb");
    let queue = RetryQueue::new(Arc::clone(&session), &queue_db_path, Arc::clone(&config))?;
    let handlers = Arc::new(Handlers {
        session,
        cache,
        boards,
        history,
        queue,
        config: Arc::clone(&config),
    });

    reload::spawn(Arc::clone(&config));

    let listener = server::make_listener(&socket)?;

    let (active_secs, semi_secs) = {
        let c = config.read().unwrap();
        (c.refresh.active_secs, c.refresh.semi_secs)
    };
    if server::is_socket_activated() {
        info!(
            refresh_interval = active_secs,
            semi_refresh_interval = semi_secs,
            "starting gitlab-trackrd from socket"
        );
    } else {
        info!(
            socket = socket,
            refresh_interval = active_secs,
            semi_refresh_interval = semi_secs,
            "starting gitlab-trackrd"
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
            handlers_ref.refresh_cache().await;
            handlers_ref.backfill_history().await;
        });
    }

    // Active tier: refresh issues, boards, and the last-24h history every
    // `refresh.active_secs`. The interval is re-read each tick so a config
    // reload takes effect after the current sleep.
    {
        let handlers_ref = Arc::clone(&handlers);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            loop {
                let secs = config.read().unwrap().refresh.active_secs;
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                info!("background cache refresh triggered");
                handlers_ref.refresh_cache().await;
            }
        });
    }

    // Semi-active tier: re-poll the 24h–30d history band once a day and prune
    // anything past the stale window.
    {
        let handlers_ref = Arc::clone(&handlers);
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            loop {
                let secs = config.read().unwrap().refresh.semi_secs;
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
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
