//! `tt refresh` — bust the daemon's in-memory issue cache.
//!
//! Use after closing/opening an issue in the GitLab UI when you don't want to
//! wait the daemon's cache TTL (default 5 minutes) for the change to surface.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, config};

pub async fn run() -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    client
        .clear_cache()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("ClearCache failed: {e}"))?;
    println!("cache cleared");
    Ok(())
}
