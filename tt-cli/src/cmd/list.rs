//! `tt list` — dump the daemon's view of your assigned, open issues, or (with
//! `--mrs`) merge requests.
//!
//! Pure cache read: the daemon's background sync owns freshness, so this just
//! serves whatever was last synced — no fetch, effectively free. The MR view
//! rides the search sync, so it refreshes at that (slower) cadence.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::OutputFormat;
use crate::{client, config};

pub async fn run(groups: Vec<String>, mrs: bool, output: OutputFormat) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let filter = (!groups.is_empty()).then_some(groups);

    if mrs {
        let reply = client
            .get_assigned_merge_requests(filter)
            .call()
            .await
            .map_err(|e| crate::friendly::friendly("GetAssignedMergeRequests", e))?;
        match output {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&reply.merge_requests)?);
            }
            OutputFormat::Text => {
                for mr in reply.merge_requests {
                    println!(
                        "!{:<5} {:<8} {}  {}",
                        mr.iid, mr.state, mr.title, mr.web_url
                    );
                }
            }
        }
        return Ok(());
    }

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
