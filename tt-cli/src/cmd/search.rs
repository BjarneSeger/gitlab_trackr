//! `tt search` — query the daemon's cached search corpus.
//!
//! Pure cache read: the daemon's background search sync owns freshness
//! (incremental `updated_after` pulls, a periodic full resync), so this just
//! serves whatever was last synced — no fetch, effectively free.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::{OutputFormat, SearchKind};
use crate::{client, config};

pub async fn run(
    query: String,
    kinds: Vec<SearchKind>,
    limit: Option<i64>,
    output: OutputFormat,
) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let filter = (!kinds.is_empty()).then(|| kinds.iter().map(|k| wire_kind(*k)).collect());
    let reply = client
        .search(query, filter, limit)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("Search", e))?;

    match output {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&reply)?);
        }
        OutputFormat::Text => {
            if reply.issues.is_empty()
                && reply.merge_requests.is_empty()
                && reply.projects.is_empty()
                && reply.groups.is_empty()
            {
                println!("no matches");
                return Ok(());
            }
            if !reply.issues.is_empty() {
                println!("Issues:");
                for i in &reply.issues {
                    println!("  #{:<5} {:<8} {}  {}", i.iid, i.state, i.title, i.web_url);
                }
            }
            if !reply.merge_requests.is_empty() {
                println!("Merge requests:");
                for m in &reply.merge_requests {
                    println!("  !{:<5} {:<8} {}  {}", m.iid, m.state, m.title, m.web_url);
                }
            }
            if !reply.projects.is_empty() {
                println!("Projects:");
                for p in &reply.projects {
                    println!("  {}  {}", p.path, p.web_url);
                }
            }
            if !reply.groups.is_empty() {
                println!("Groups:");
                for g in &reply.groups {
                    println!("  {}  {}", g.path, g.web_url);
                }
            }
        }
    }
    Ok(())
}

fn wire_kind(kind: SearchKind) -> String {
    match kind {
        SearchKind::Issues => "issues",
        SearchKind::Mrs => "merge_requests",
        SearchKind::Projects => "projects",
        SearchKind::Groups => "groups",
    }
    .to_string()
}
