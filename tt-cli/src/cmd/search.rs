//! `tt search` — query the daemon's search corpus, transparently refreshed.
//!
//! Text output streams (varlink `more`): the daemon replies instantly from
//! its local corpus, then — while connected — asks GitLab live and sends a
//! second, merged reply; anything new is appended under a "Fresh from
//! GitLab" marker. The daemon's contract is deterministic: an error is one
//! terminal reply, a success is exactly two. `--output json` sticks to the
//! plain single-reply call so scripts get one complete document (the daemon
//! folds the live results in before answering).

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use gitlab_trackr_api::{Search_Reply, VarlinkClientInterface};

use crate::cli::{OutputFormat, SearchKind};
use crate::{client, config};

/// Upper bound on waiting for the daemon's second (live-merged) reply. The
/// daemon bounds its live lookup with `search.live_deadline_ms` (3 s by
/// default), so this only trips against a hung or pre-streaming daemon —
/// the cached results are already on screen either way.
const LIVE_REPLY_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run(
    query: String,
    kinds: Vec<SearchKind>,
    limit: Option<i64>,
    output: OutputFormat,
) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let filter: Option<Vec<String>> =
        (!kinds.is_empty()).then(|| kinds.iter().map(|k| wire_kind(*k)).collect());

    match output {
        OutputFormat::Json => {
            let reply = client
                .search(query, filter, limit)
                .call()
                .await
                .map_err(|e| crate::friendly::friendly("Search", e))?;
            println!("{}", serde_json::to_string_pretty(&reply)?);
        }
        OutputFormat::Text => {
            let mut call = client.search(query, filter, limit);
            let call = call
                .more()
                .await
                .map_err(|e| crate::friendly::friendly("Search", e))?;
            let cached = call
                .recv()
                .await
                .map_err(|e| crate::friendly::friendly("Search", e))?;
            let printed_cached = print_sections(&cached);

            let printed_fresh = match tokio::time::timeout(LIVE_REPLY_TIMEOUT, call.recv()).await {
                Ok(Ok(merged)) => {
                    let fresh = minus(&merged, &cached);
                    if print_would_emit(&fresh) {
                        if printed_cached {
                            println!();
                        }
                        println!("Fresh from GitLab:");
                        print_sections(&fresh)
                    } else {
                        false
                    }
                }
                Ok(Err(e)) => return Err(crate::friendly::friendly("Search", e)),
                // The cached results are already printed; a missing second
                // reply (hung daemon, pre-streaming daemon) costs nothing
                // more than this silence.
                Err(_) => false,
            };

            if !printed_cached && !printed_fresh {
                println!("no matches");
            }
        }
    }
    Ok(())
}

/// Print the non-empty sections of `reply`; returns whether anything was
/// printed.
fn print_sections(reply: &Search_Reply) -> bool {
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
    print_would_emit(reply)
}

fn print_would_emit(reply: &Search_Reply) -> bool {
    !(reply.issues.is_empty()
        && reply.merge_requests.is_empty()
        && reply.projects.is_empty()
        && reply.groups.is_empty())
}

/// Entries of `merged` that were not in `cached`, by id — what the live
/// lookup added beyond the instantly-printed corpus results.
fn minus(merged: &Search_Reply, cached: &Search_Reply) -> Search_Reply {
    let issues: HashSet<i64> = cached.issues.iter().map(|i| i.id).collect();
    let mrs: HashSet<i64> = cached.merge_requests.iter().map(|m| m.id).collect();
    let projects: HashSet<i64> = cached.projects.iter().map(|p| p.id).collect();
    let groups: HashSet<i64> = cached.groups.iter().map(|g| g.id).collect();
    Search_Reply {
        issues: merged
            .issues
            .iter()
            .filter(|i| !issues.contains(&i.id))
            .cloned()
            .collect(),
        merge_requests: merged
            .merge_requests
            .iter()
            .filter(|m| !mrs.contains(&m.id))
            .cloned()
            .collect(),
        projects: merged
            .projects
            .iter()
            .filter(|p| !projects.contains(&p.id))
            .cloned()
            .collect(),
        groups: merged
            .groups
            .iter()
            .filter(|g| !groups.contains(&g.id))
            .cloned()
            .collect(),
    }
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
