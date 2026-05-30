//! Daemon configuration, loaded from a TOML file.
//!
//! Layered with [`confique`]: the user file at
//! `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` wins, then the
//! package-provided default at [`SYSTEM_CONFIG`], then the `#[config(default)]`
//! values baked into [`Config`] below. Every field is optional in the file.
//!
//! Credentials are deliberately **not** here — the GitLab host/token live in the
//! OS keychain and are set through the varlink API (`tt login`), never via this
//! file or the environment.
//!
//! Run the `gen-config-template` binary to print an annotated TOML template with
//! the current defaults and doc comments inline (used to generate the shipped
//! default config).

use std::path::{Path, PathBuf};
use std::time::Duration;

use confique::Config as ConfiqueConfig;

/// Default install path for the package-provided config, layered under the
/// user's own file.
const SYSTEM_CONFIG: &str = "/usr/share/gitlab-trackrd/config.toml";

/// `gitlab-trackrd` configuration.
///
/// Missing keys fall back to the `#[config(default = ...)]` value on each field.
#[derive(Debug, ConfiqueConfig)]
pub struct Config {
    /// Unix socket the daemon listens on. If unset, defaults to
    /// `$XDG_RUNTIME_DIR/gitlab-trackrd.socket` (then `/tmp/...` as a last
    /// resort). Ignored under systemd socket activation.
    pub socket: Option<String>,

    /// Seconds between active-tier refreshes (assigned issues, boards, and the
    /// last-24h timelog history).
    #[config(default = 300)]
    pub refresh_interval: u64,

    /// Seconds between semi-active history refreshes (the 24h–30d band). Once a
    /// day by default.
    #[config(default = 86400)]
    pub semi_refresh_interval: u64,

    /// Active history tier, in hours: the most volatile band, re-polled every
    /// `refresh_interval`.
    #[config(default = 24)]
    pub active_window_hours: u64,

    /// Semi-active history tier, in hours: re-polled every
    /// `semi_refresh_interval`. (30 days by default.)
    #[config(default = 720)]
    pub semi_window_hours: u64,

    /// Overall history retention, in hours: fetched once at startup; anything
    /// older is pruned. (90 days by default.)
    #[config(default = 2160)]
    pub stale_window_hours: u64,

    /// Retry-queue exponential backoff: initial delay, in seconds.
    #[config(default = 1)]
    pub queue_base_delay_secs: u64,

    /// Retry-queue exponential backoff: maximum delay, in seconds. (30 min.)
    #[config(default = 1800)]
    pub queue_max_delay_secs: u64,

    /// How long, in seconds, a queued task keeps retrying before it is
    /// dead-lettered. (7 days by default.)
    #[config(default = 604800)]
    pub queue_max_lifetime_secs: u64,

    /// How long, in seconds, the retry worker sleeps while the daemon is
    /// dormant (no GitLab session) before checking again.
    #[config(default = 30)]
    pub queue_session_wait_secs: u64,
}

impl Config {
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

/// Render an annotated TOML template (current defaults + doc comments inline).
///
/// Used by the `gen-config-template` binary via the library target; the daemon
/// binary itself never calls it.
#[allow(dead_code)]
pub fn template() -> String {
    confique::toml::template::<Config>(confique::toml::FormatOptions::default())
}
