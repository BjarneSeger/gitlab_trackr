//! `gitlab-trackrd` — GitLab time-tracking varlink daemon.

use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod boards;
mod cache;
mod config;
mod db;
mod error;
mod gitlab;
mod handlers;
mod queue;
mod server;
mod service;

use boards::BoardCache;
use cache::IssueCache;
use config::Config;
use error::Result;
use gitlab::GitlabClient;
use handlers::Handlers;
use queue::RetryQueue;
use service::ServiceHandler;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("GITLAB_TRACKR")
                .unwrap_or_else(|_| EnvFilter::new("gitlab-trackrd=info")),
        )
        .init();

    let cfg = Config::from_env()?;
    let gitlab = Arc::new(GitlabClient::connect(&cfg.host, &cfg.token).await?);
    let cache = Arc::new(IssueCache::open(&cfg.db_path)?);
    let boards_db_path = cfg.db_path.with_file_name("boards.redb");
    let boards = Arc::new(BoardCache::open(&boards_db_path)?);
    let queue_db_path = cfg.db_path.with_file_name("queue.redb");
    let queue = RetryQueue::new(Arc::clone(&gitlab), &queue_db_path)?;
    let handlers = Arc::new(Handlers {
        gitlab,
        cache,
        boards,
        queue,
    });

    let listener = server::make_listener(&cfg.socket)?;

    if server::is_socket_activated() {
        info!(
            refresh_interval = cfg.refresh_interval,
            "starting gitlab-trackrd from socket"
        );
    } else {
        info!(
            socket = cfg.socket,
            refresh_interval = cfg.refresh_interval,
            "starting gitlab-trackrd"
        );
    }

    {
        let handlers_ref = Arc::clone(&handlers);
        let interval_secs = cfg.refresh_interval;
        tokio::spawn(async move {
            let duration = std::time::Duration::from_secs(interval_secs);
            loop {
                tokio::time::sleep(duration).await;
                info!("background cache refresh triggered");
                handlers_ref.refresh_cache().await;
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
        let _ = std::fs::remove_file(&cfg.socket);
    }
    Ok(())
}
