//! Daemon configuration, loaded from a TOML file.
//!
//! Layered with [`confique`]: the user file at
//! `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` wins, then the
//! package-provided default at [`SYSTEM_CONFIG`], then the `#[config(default)]`
//! values baked into the structs below. Every field is optional in the file.
//!
//! The config is grouped into nested sections, one per concern, so the TOML
//! reads as `[server]` / `[refresh]` / `[history]` / `[queue]` tables instead
//! of a flat list of keys. Each [`Config`] field is a sub-struct owned by the
//! module that consumes it, alongside the helpers that turn raw values into the
//! runtime types those modules expect.
//!
//! Credentials are deliberately **not** here — the GitLab host/token live in the
//! OS keychain and are set through the varlink API (`tt login`), never via this
//! file or the environment.
//!
//! Run the `gen-config-template` binary to print an annotated TOML template with
//! the current defaults and doc comments inline (used to generate the shipped
//! default config).

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use confique::Config as ConfiqueConfig;

/// Shared, swappable config read by every consumer at the moment of use, so a
/// hot reload (see `reload`) takes effect without a restart.
///
/// Reads must stay momentary — extract the `Copy` value you need in a single
/// statement so the guard drops before any `.await`; never hold it across one.
pub type SharedConfig = Arc<RwLock<Config>>;

/// Default install path for the package-provided config, layered under the
/// user's own file.
const SYSTEM_CONFIG: &str = "/usr/share/gitlab-trackrd/config.toml";

/// `gitlab-trackrd` configuration.
///
/// Each section is a nested sub-struct; missing keys fall back to the
/// `#[config(default = ...)]` value on the corresponding field.
#[derive(Debug, ConfiqueConfig)]
pub struct Config {
    /// Varlink server / listening socket.
    #[config(nested)]
    pub server: ServerConfig,

    /// Background refresh cadence for the cache tiers.
    #[config(nested)]
    pub refresh: RefreshConfig,

    /// Timelog history retention tiers.
    #[config(nested)]
    pub history: HistoryConfig,

    /// Retry-queue backoff and lifetime tuning.
    #[config(nested)]
    pub queue: QueueConfig,
}

/// Varlink server settings (see `server.rs`).
#[derive(Debug, ConfiqueConfig)]
pub struct ServerConfig {
    /// Unix socket the daemon listens on. If unset, defaults to
    /// `$XDG_RUNTIME_DIR/gitlab-trackrd.socket` (then `/tmp/...` as a last
    /// resort). Ignored under systemd socket activation.
    pub socket: Option<String>,
}

impl ServerConfig {
    /// The configured socket, or the `$XDG_RUNTIME_DIR` -> `/tmp` fallback chain
    /// when unset.
    pub fn resolved_socket(&self) -> String {
        if let Some(socket) = &self.socket {
            return socket.clone();
        }
        dirs::runtime_dir()
            .map(|d| d.join("gitlab-trackrd.socket").to_string_lossy().into_owned())
            .unwrap_or_else(|| "/tmp/gitlab-trackrd.socket".to_string())
    }
}

/// How often the background loops re-poll each cache tier (driven from
/// `main.rs`).
#[derive(Debug, ConfiqueConfig)]
pub struct RefreshConfig {
    /// Seconds between active-tier refreshes (assigned issues, boards, and the
    /// last-24h timelog history).
    #[config(default = 300)]
    pub active_secs: u64,

    /// Seconds between semi-active history refreshes (the 24h–30d band). Once a
    /// day by default.
    #[config(default = 86400)]
    pub semi_secs: u64,
}

/// Timelog history retention tiers, consumed by `history.rs` via `Handlers`.
#[derive(Debug, ConfiqueConfig)]
pub struct HistoryConfig {
    /// Active history tier, in hours: the most volatile band, re-polled every
    /// `refresh.active_secs`.
    #[config(default = 24)]
    pub active_window_hours: u64,

    /// Semi-active history tier, in hours: re-polled every
    /// `refresh.semi_secs`. (30 days by default.)
    #[config(default = 720)]
    pub semi_window_hours: u64,

    /// Overall history retention, in hours: fetched once at startup; anything
    /// older is pruned. (90 days by default.)
    #[config(default = 2160)]
    pub stale_window_hours: u64,
}

impl HistoryConfig {
    pub fn active_window(&self) -> Duration {
        Duration::from_secs(self.active_window_hours * 3600)
    }

    pub fn semi_window(&self) -> Duration {
        Duration::from_secs(self.semi_window_hours * 3600)
    }

    pub fn stale_window(&self) -> Duration {
        Duration::from_secs(self.stale_window_hours * 3600)
    }
}

/// Retry-queue timing, consumed by `queue.rs`. The `*_secs` fields come from the
/// TOML; the accessors hand `queue.rs` the [`Duration`]s its worker uses.
#[derive(Debug, Clone, Copy, ConfiqueConfig)]
pub struct QueueConfig {
    /// Retry-queue exponential backoff: initial delay, in seconds.
    #[config(default = 1)]
    pub base_delay_secs: u64,

    /// Retry-queue exponential backoff: maximum delay, in seconds. (30 min.)
    #[config(default = 1800)]
    pub max_delay_secs: u64,

    /// How long, in seconds, a queued task keeps retrying before it is
    /// dead-lettered. (7 days by default.)
    #[config(default = 604800)]
    pub max_lifetime_secs: u64,

    /// How long, in seconds, the retry worker sleeps while the daemon is
    /// dormant (no GitLab session) before checking again.
    #[config(default = 30)]
    pub session_wait_secs: u64,
}

impl QueueConfig {
    /// Initial exponential-backoff delay.
    pub fn base_delay(&self) -> Duration {
        Duration::from_secs(self.base_delay_secs)
    }

    /// Exponential-backoff cap.
    pub fn max_delay(&self) -> Duration {
        Duration::from_secs(self.max_delay_secs)
    }

    /// How long a task keeps retrying before it is dead-lettered.
    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_secs)
    }

    /// How long the worker sleeps while dormant (no session) before retrying.
    pub fn session_wait(&self) -> Duration {
        Duration::from_secs(self.session_wait_secs)
    }
}

impl Default for QueueConfig {
    /// Mirrors the `#[config(default = ...)]` values above; used as the worker's
    /// `OnceLock` fallback and by the queue tests.
    fn default() -> Self {
        Self {
            base_delay_secs: 1,
            max_delay_secs: 1800,
            max_lifetime_secs: 604800,
            session_wait_secs: 30,
        }
    }
}

/// `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` (falls back to `./`).
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gitlab-trackrd/config.toml")
}

/// Load the layered config: user file → system default → built-in defaults.
///
/// Missing files are treated as empty layers; parse errors propagate.
pub fn load() -> Result<Config, confique::Error> {
    Config::builder()
        .file(config_path())
        .file(Path::new(SYSTEM_CONFIG))
        .load()
}

/// Load once and wrap for sharing across the daemon's tasks. Used at startup; a
/// parse error propagates so the caller can fail fast (no prior config exists).
pub fn load_shared() -> Result<SharedConfig, confique::Error> {
    Ok(Arc::new(RwLock::new(load()?)))
}

/// Re-run [`load`] and swap the contents in place.
///
/// On a parse error the existing config is left untouched and the error is
/// returned, so a malformed mid-edit save never disturbs the running daemon —
/// the caller logs and keeps serving the last-good values.
pub fn reload(shared: &SharedConfig) -> Result<(), confique::Error> {
    let fresh = load()?;
    *shared.write().unwrap() = fresh;
    Ok(())
}

/// A fully-defaulted config (no file layers), for tests that need a
/// [`SharedConfig`] without touching the real XDG path.
#[cfg(test)]
pub fn defaults() -> Config {
    Config::builder().load().expect("built-in defaults are valid")
}

/// Render an annotated TOML template (current defaults + doc comments inline).
///
/// Used by the `gen-config-template` binary via the library target; the daemon
/// binary itself never calls it.
#[allow(dead_code)]
pub fn template() -> String {
    confique::toml::template::<Config>(confique::toml::FormatOptions::default())
}
