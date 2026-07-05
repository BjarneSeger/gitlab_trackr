//! `tt list` — dump the daemon's view of your assigned, open issues.
//!
//! Hits the daemon's cache, so it's effectively free if called within the
//! cache TTL window.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::OutputFormat;
use crate::{client, config};

pub async fn run(groups: Vec<String>, output: OutputFormat) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let filter = (!groups.is_empty()).then_some(groups);
    let reply = client
        .get_assigned_issues(filter)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("GetAssignedIssues", e))?;

    match output {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&reply.issues)?);
        }
        OutputFormat::Text => {
            for issue in reply.issues {
                println!(
                    "#{:<5} {:<8} {}  {}",
                    issue.iid, issue.state, issue.title, issue.web_url
                );
            }
        }
    }
    Ok(())
}
