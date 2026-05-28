//! Daemon configuration sourced from environment variables plus the OS
//! keychain (read at startup by `main`, not here).

use std::path::PathBuf;

use crate::error::Result;
use crate::secrets::Credentials;

/// Default interval in seconds between background cache refreshes (the active
/// history tier plus issues and boards).
const DEFAULT_REFRESH_INTERVAL: u64 = 300;
/// Default interval in seconds between semi-active history refreshes (once a day).
const DEFAULT_SEMI_REFRESH_INTERVAL: u64 = 86_400;

pub struct Config {
    /// Credentials sourced from environment variables (`GITLAB_TOKEN` +
    /// optional `GITLAB_HOST`). When set, these win over anything in the OS
    /// keychain — useful for CI and existing setups. `None` means the daemon
    /// should try the keychain.
    pub env_creds: Option<Credentials>,
    pub socket: String,
    pub db_path: PathBuf,
    /// How often the active tier (last 24h), issues, and boards are refreshed.
    pub refresh_interval: u64,
    /// How often the semi-active tier (24h–30d) is refreshed.
    pub semi_refresh_interval: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let env_creds = match std::env::var("GITLAB_TOKEN") {
            Ok(token) if !token.is_empty() => {
                let host =
                    std::env::var("GITLAB_HOST").unwrap_or_else(|_| "gitlab.com".to_string());
                Some(Credentials { host, token })
            }
            _ => None,
        };
        let socket = std::env::var("GITLAB_TRACKRD_SOCKET").unwrap_or_else(|_| {
            std::env::var("XDG_RUNTIME_DIR")
                .map(|d| format!("{d}/gitlab-trackrd.socket"))
                .unwrap_or_else(|_| "/tmp/gitlab-trackrd.socket".to_string())
        });
        let db_path = dirs::data_local_dir()
            .unwrap_or_else(|| "~/.local/share".into())
            .join("gitlab-trackrd/cache.redb");

        let refresh_interval = std::env::var("GITLAB_TRACKRD_REFRESH_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_REFRESH_INTERVAL);

        let semi_refresh_interval = std::env::var("GITLAB_TRACKRD_SEMI_REFRESH_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SEMI_REFRESH_INTERVAL);

        Ok(Self {
            env_creds,
            socket,
            db_path,
            refresh_interval,
            semi_refresh_interval,
        })
    }
}
