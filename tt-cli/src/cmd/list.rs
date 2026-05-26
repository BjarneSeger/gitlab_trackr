//! `tt list` — dump the daemon's view of your assigned, open issues.
//!
//! Hits the daemon's cache, so it's effectively free if called within the
//! cache TTL window.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, config};

pub async fn run() -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let reply = client
        .get_assigned_issues(None)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("GetAssignedIssues failed: {e}"))?;
    for issue in reply.issues {
        println!(
            "#{:<5} {:<8} {}  {}",
            issue.iid, issue.state, issue.title, issue.web_url
        );
    }
    Ok(())
}
