//! `tt log` — non-interactive time logging.
//!
//! Designed for scripting and one-off use ("I forgot to start the prompt,
//! just log 45m on #42"). Tries hard to avoid making the user supply
//! `--project-id` — see [`resolve_project_id`] for the precedence rules.

use anyhow::{Context, Result, bail};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, config, state};

pub async fn run(
    iid: i64,
    duration: String,
    project_id: Option<i64>,
    summary: Option<String>,
) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);

    let project_id = match project_id {
        Some(p) => p,
        None => resolve_project_id(iid, &socket).await?,
    };

    let client = client::connect(&socket).await?;
    client
        .post_time(project_id, iid, duration.clone(), summary.clone())
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("PostTime failed: {e}"))?;

    let mut st = state::load().unwrap_or_default();
    st.last_issue = Some(state::LastIssue {
        project_id,
        issue_iid: iid,
    });
    state::save(&st).context("saving state")?;

    println!("logged {duration} on !{iid} (project {project_id})");
    Ok(())
}

/// Resolve a project ID for a given issue IID without bothering the user.
///
/// Precedence:
/// 1. The cached `last_issue` if its IID matches — covers re-logging against
///    the same issue, which is the common case.
/// 2. The assigned-issues list from the daemon, if it contains exactly one
///    match.
/// 3. Bail with an explanation. We refuse to guess across ambiguous matches
///    because picking the wrong project would silently log time on someone
///    else's issue.
async fn resolve_project_id(iid: i64, socket: &str) -> Result<i64> {
    if let Ok(st) = state::load()
        && let Some(last) = st.last_issue
        && last.issue_iid == iid
    {
        return Ok(last.project_id);
    }
    let client = client::connect(socket).await?;
    let reply = client
        .get_assigned_issues(None)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("GetAssignedIssues failed: {e}"))?;
    let matches: Vec<_> = reply.issues.iter().filter(|i| i.iid == iid).collect();
    match matches.as_slice() {
        [] => bail!("no assigned issue with iid {iid} — pass --project-id explicitly"),
        [only] => Ok(only.project_id),
        many => bail!(
            "iid {iid} is ambiguous across {} assigned projects — pass --project-id",
            many.len()
        ),
    }
}
