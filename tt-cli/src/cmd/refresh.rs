//! `tt refresh` — drop the daemon's caches and re-fetch.
//!
//! With no flags this clears everything (assigned issues, boards, and all three
//! history tiers) — use it after editing an issue in the GitLab UI when you
//! don't want to wait out the daemon's refresh interval. The per-tier flags
//! clear only the named caches; cleared history tiers are re-fetched right away.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, config};

pub async fn run(active: bool, semi: bool, stale: bool, issues: bool) -> Result<()> {
    // Collect the requested scopes. No flags ⇒ `None`, which the daemon reads
    // as "clear everything".
    let mut scope: Vec<String> = Vec::new();
    if active {
        scope.push("active".to_string());
    }
    if semi {
        scope.push("semi".to_string());
    }
    if stale {
        scope.push("stale".to_string());
    }
    if issues {
        scope.push("issues".to_string());
    }
    let scope = if scope.is_empty() { None } else { Some(scope) };

    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    client
        .clear_cache(scope.clone())
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("ClearCache failed: {e}"))?;
    match scope {
        None => println!("cache cleared"),
        Some(s) => println!("cleared: {}", s.join(", ")),
    }
    Ok(())
}
