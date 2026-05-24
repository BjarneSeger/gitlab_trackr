//! `gitlab_trackrd` — GitLab time-tracking varlink daemon.
//!
//! Exposes a [varlink](https://varlink.org) IPC socket that clients use to:
//!
//! - retrieve the current user's open, assigned GitLab issues ([`iface::VarlinkInterface::get_assigned_issues`])
//! - post spent time to an issue ([`iface::VarlinkInterface::post_time`])
//!
//! The interface is defined in `src/org.thehoster.gitlab.trackrd.varlink`; the
//! Rust bindings are generated at compile time by [`build.rs`](../build.rs) via
//! `varlink_generator` and included into the `iface` module.
//!
//! # Configuration
//!
//! All configuration is via environment variables:
//!
//! | Variable | Required | Default | Description |
//! |---|---|---|---|
//! | `GITLAB_TOKEN` | yes | — | Personal access token with `read_api` + `write_api` scopes |
//! | `GITLAB_HOST` | no | `gitlab.com` | GitLab instance hostname |
//! | `GITLAB_TRACKRD_SOCKET` | no | `unix:$XDG_RUNTIME_DIR/gitlab_trackrd.socket` | Varlink socket address |
//! | `GITLAB_TRACKRD_CACHE_TTL` | no | `300` | Issue cache lifetime in seconds |

use std::sync::{Arc, Mutex};

use redb::{Database, TableDefinition};
use tracing::info;
use tracing_subscriber::EnvFilter;
use varlink::{ListenAsyncConfig, listen_async};

mod daemon;
mod gl;
mod service;
mod utils;

use daemon::Daemon;
use gitlab_trackr_api::Issue;
use service::ServiceHandler;

/// redb table that stores the serialised [`CachedData`](daemon::CachedData) blob under the key `"assigned"`.
const ISSUES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("issues_cache");

/// Default number of seconds before the cached issue list is considered stale.
const DEFAULT_CACHE_TTL: u64 = 300;

/// Top-level error type used throughout the daemon.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A GitLab API call failed or returned an unexpected response.
    #[error("GitLab error: {0}")]
    Gitlab(String),
    /// A redb read or write operation failed.
    #[error("Cache error: {0}")]
    Cache(String),
    /// JSON serialisation / deserialisation failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// An OS-level I/O operation failed (e.g. creating the cache directory).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A required environment variable was not set.
    #[error("Environment variable '{0}' is not set")]
    Env(&'static str),
    /// The varlink runtime reported an error.
    #[error("Varlink error: {0}")]
    Varlink(#[from] varlink::Error),
}

type Result<T> = std::result::Result<T, Error>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("GITLAB_TRACKR")
                .unwrap_or_else(|_| EnvFilter::new("gitlab_trackrd=info")),
        )
        .init();

    let token = std::env::var("GITLAB_TOKEN").map_err(|_| Error::Env("GITLAB_TOKEN"))?;
    let host = std::env::var("GITLAB_HOST").unwrap_or_else(|_| "gitlab.com".to_string());
    let socket = std::env::var("GITLAB_TRACKRD_SOCKET").unwrap_or_else(|_| {
        std::env::var("XDG_RUNTIME_DIR")
            .map(|d| format!("unix:{d}/gitlab_trackrd.socket"))
            .unwrap_or_else(|_| "unix:/tmp/gitlab_trackrd.socket".to_string())
    });
    let cache_ttl = std::env::var("GITLAB_TRACKRD_CACHE_TTL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CACHE_TTL);

    let client = gitlab::GitlabBuilder::new(host, token)
        .build_async()
        .await
        .map_err(|e| Error::Gitlab(e.to_string()))?;

    let db_path = dirs::data_local_dir()
        .unwrap_or_else(|| "~/.local/share".into())
        .join("gitlab_trackrd/cache.redb");
    std::fs::create_dir_all(db_path.parent().unwrap())?;
    let db = Database::create(&db_path).map_err(|e| Error::Cache(e.to_string()))?;
    {
        let txn = db.begin_write().map_err(|e| Error::Cache(e.to_string()))?;
        txn.open_table(ISSUES_TABLE)
            .map_err(|e| Error::Cache(e.to_string()))?;
        txn.commit().map_err(|e| Error::Cache(e.to_string()))?;
    }

    let daemon = Arc::new(Daemon {
        client,
        db: Arc::new(Mutex::new(db)),
        cache_ttl,
    });

    info!(socket, cache_ttl, "starting gitlab_trackrd");
    listen_async(
        Arc::new(ServiceHandler::new(daemon)),
        &socket,
        &ListenAsyncConfig::default(),
    )
    .await?;
    Ok(())
}
