//! Filesystem watcher that hot-reloads the daemon config.
//!
//! Watches the *directory* holding the user's `config.toml` (so atomic
//! write-temp+rename saves and first-time creation are caught) and, on a
//! debounced change, re-runs [`config::reload`]. A malformed save is logged and
//! ignored — the daemon keeps serving the last-good config. The `server.socket`
//! field can't change at runtime, so a change to it only warns.

use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tracing::{info, warn};

use crate::config::{self, SharedConfig};

/// How long to wait for the event burst from a single save to settle before
/// reloading.
const DEBOUNCE: Duration = Duration::from_millis(400);

/// Start watching the config file and reload `shared` whenever it changes.
///
/// Best-effort: if the config directory can't be watched the daemon still runs,
/// it just won't pick up edits live.
pub fn spawn(shared: SharedConfig) {
    let path = config::config_path();
    let Some(dir) = path.parent().map(|p| p.to_path_buf()) else {
        warn!(path = %path.display(), "config path has no parent; not watching for changes");
        return;
    };

    // notify's callback runs on its own OS thread; bridge events into the
    // current-thread runtime over an unbounded channel (sync, non-blocking send).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut watcher = match notify::recommended_watcher(move |res| {
        if let Ok(event) = res {
            let _ = tx.send(event);
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            warn!(error = %e, "failed to create config watcher; changes won't be picked up");
            return;
        }
    };

    if let Err(e) = watcher.watch(&dir, RecursiveMode::NonRecursive) {
        warn!(error = %e, dir = %dir.display(), "failed to watch config dir; changes won't be picked up");
        return;
    }

    info!(path = %path.display(), "watching config for changes");

    tokio::spawn(async move {
        // Keep the watcher alive for as long as we're draining its events.
        let _watcher = watcher;
        while let Some(event) = rx.recv().await {
            if !event.paths.contains(&path) {
                continue;
            }
            // Coalesce the rest of this save's event burst before reloading.
            while tokio::time::timeout(DEBOUNCE, rx.recv()).await.is_ok() {}

            let before = shared.read().unwrap().server.resolved_socket();
            match config::reload(&shared) {
                Ok(()) => {
                    let after = shared.read().unwrap().server.resolved_socket();
                    if before != after {
                        warn!(
                            old = %before,
                            new = %after,
                            "socket change requires a restart to take effect",
                        );
                    }
                    info!("config reloaded");
                }
                Err(e) => {
                    warn!(error = %e, "config reload failed; keeping previous config");
                }
            }
        }
    });
}
