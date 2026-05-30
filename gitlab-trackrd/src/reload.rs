//! Filesystem watcher that hot-reloads the daemon config.
//!
//! Watches the *directory* holding the user's `config.toml` (so atomic
//! write-temp+rename saves and first-time creation are caught) and, on a
//! debounced change, re-runs [`config::reload`]. A malformed save is logged and
//! ignored — the daemon keeps serving the last-good config. The `server.socket`
//! field can't change at runtime, so a change to it only warns.

use std::path::Path;
use std::time::Duration;

use notify::{Event, EventKind, RecursiveMode, Watcher};
use tracing::{info, warn};

use crate::config::{self, SharedConfig};

/// How long to wait after the first change before reloading, so an editor's
/// multi-event save burst collapses into a single reload.
const DEBOUNCE: Duration = Duration::from_millis(400);

/// Whether `event` is a content change to the config file itself.
///
/// Filters out neighbouring editor noise in the same directory (vim's
/// `.config.toml.swp` swap file, `config.toml~` backups, write-temp files) and
/// `Access` events — including the read [`config::reload`] itself performs,
/// which would otherwise re-trigger the watcher in a loop.
fn is_config_change(event: &Event, path: &Path) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) && event.paths.iter().any(|p| p == path)
}

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
            // Editor noise in the same directory (swap/backup/temp files, bare
            // reads) is filtered here so it can never start a reload.
            if !is_config_change(&event, &path) {
                continue;
            }

            // Fixed debounce from the first change: let the editor's write
            // burst settle, then discard whatever queued up (the rest of the
            // burst plus any neighbouring noise) so it all collapses into one
            // reload. `reload()` re-reads the file, so a save that lands inside
            // this window is still reflected.
            tokio::time::sleep(DEBOUNCE).await;
            while rx.try_recv().is_ok() {}

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
