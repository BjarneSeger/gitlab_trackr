//! Resolve a project ID for a given issue IID without bothering the user.
//!
//! Used by every mutating command (`log`, `close`, `assign`, `unassign`) so
//! the user doesn't have to remember or look up the project ID for each call.
//!
//! Precedence:
//! 1. The cached `last_issue` in [`crate::state`] if its IID matches — covers
//!    re-acting on the same issue, which is the common case.
//! 2. The assigned-issues list from the daemon, if it contains exactly one
//!    match.
//! 3. Bail with an explanation. We refuse to guess across ambiguous matches
//!    because picking the wrong project would silently act on someone else's
//!    issue.

use anyhow::{Result, bail};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, state};

pub async fn resolve(iid: i64, socket: &str) -> Result<i64> {
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
