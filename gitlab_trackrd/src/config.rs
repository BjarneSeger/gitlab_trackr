//! Daemon configuration sourced entirely from environment variables.

use std::path::PathBuf;

use crate::error::{Error, Result};

/// Default number of seconds before the cached issue list is considered stale.
const DEFAULT_CACHE_TTL: u64 = 300;

pub struct Config {
    pub token: String,
    pub host: String,
    pub cache_ttl: u64,
    pub socket: String,
    pub db_path: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let token = std::env::var("GITLAB_TOKEN").map_err(|_| Error::Env("GITLAB_TOKEN"))?;
        let host = std::env::var("GITLAB_HOST").unwrap_or_else(|_| "gitlab.com".to_string());
        let cache_ttl = std::env::var("GITLAB_TRACKRD_CACHE_TTL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CACHE_TTL);
        let socket = std::env::var("GITLAB_TRACKRD_SOCKET").unwrap_or_else(|_| {
            std::env::var("XDG_RUNTIME_DIR")
                .map(|d| format!("{d}/gitlab-trackrd.socket"))
                .unwrap_or_else(|_| "/tmp/gitlab-trackrd.socket".to_string())
        });
        let db_path = dirs::data_local_dir()
            .unwrap_or_else(|| "~/.local/share".into())
            .join("gitlab-trackrd/cache.redb");

        Ok(Self {
            token,
            host,
            cache_ttl,
            socket,
            db_path,
        })
    }
}
