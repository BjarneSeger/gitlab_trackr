//! Thin wrapper around the generated varlink client.
//!
//! Exists so each subcommand doesn't have to repeat the socket-resolution and
//! `AsyncConnection::with_address` dance.

use anyhow::{Context, Result};
use gitlab_trackr_api::VarlinkClient;
use varlink::AsyncConnection;

/// Resolve the daemon's varlink socket address.
///
/// Precedence: `GITLAB_TRACKRD_SOCKET` env var → `unix:$XDG_RUNTIME_DIR/gitlab_trackrd.socket`
/// → `unix:/tmp/gitlab_trackrd.socket`. **Must stay in sync with the daemon's
/// own resolution in `gitlab_trackrd/src/main.rs`** — if the daemon's default
/// changes, this must change too, or `tt` will silently miss the running daemon.
pub fn default_socket() -> String {
    std::env::var("GITLAB_TRACKRD_SOCKET").unwrap_or_else(|_| {
        std::env::var("XDG_RUNTIME_DIR")
            .map(|d| format!("unix:{d}/gitlab_trackrd.socket"))
            .unwrap_or_else(|_| "unix:/tmp/gitlab_trackrd.socket".to_string())
    })
}

/// Open an async varlink connection to the daemon.
///
/// The connection is single-use per command invocation; we don't pool it
/// because the CLI exits right after the call returns.
pub async fn connect(socket: &str) -> Result<VarlinkClient> {
    let conn = AsyncConnection::with_address(socket)
        .await
        .with_context(|| format!("connecting to varlink socket {socket}"))?;
    Ok(VarlinkClient::new(conn))
}
