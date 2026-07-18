//! Resolve a project ID for a given issuable reference without bothering the
//! user.
//!
//! Used by every mutating command (`log`, `close`, `assign`, `unassign`) so
//! the user doesn't have to remember or look up the project ID for each call.
//!
//! Precedence:
//! 1. The cached `last_issue` in [`crate::state`] if its kind and IID match —
//!    covers re-acting on the same issuable, which is the common case.
//! 2. The assigned list from the daemon (`GetAssignedIssues` /
//!    `GetAssignedMergeRequests` per kind), if it contains exactly one match.
//! 3. For MRs only: the daemon's full search corpus, exact-filtered on the
//!    iid — so `tt close '!42'` works on MRs that aren't assigned to you.
//!    (Issues keep the assigned-only behavior they always had.)
//! 4. Bail with an explanation. We refuse to guess across ambiguous matches
//!    because picking the wrong project would silently act on someone else's
//!    issuable.

use anyhow::{Result, bail};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::refspec::RefKind;
use crate::{client, state};

/// Generous per-kind cap for the MR search fallback: the daemon matches
/// `#42`-style queries against titles too, so we over-fetch and exact-filter
/// on the iid client-side; the default 50 could truncate before the filter.
const SEARCH_FALLBACK_LIMIT: i64 = 500;

pub async fn resolve(iid: i64, kind: RefKind, socket: &str) -> Result<i64> {
    if let Ok(st) = state::load()
        && let Some(last) = st.last_issue
        && last.kind == kind
        && last.issue_iid == iid
    {
        return Ok(last.project_id);
    }
    let client = client::connect(socket).await?;

    let assigned: Vec<i64> = match kind {
        RefKind::Issue => {
            let reply = client
                .get_assigned_issues(None)
                .call()
                .await
                .map_err(|e| crate::friendly::friendly("GetAssignedIssues", e))?;
            reply
                .issues
                .iter()
                .filter(|i| i.iid == iid)
                .map(|i| i.project_id)
                .collect()
        }
        RefKind::Mr => {
            let reply = client
                .get_assigned_merge_requests(None)
                .call()
                .await
                .map_err(|e| crate::friendly::friendly("GetAssignedMergeRequests", e))?;
            reply
                .merge_requests
                .iter()
                .filter(|m| m.iid == iid)
                .map(|m| m.project_id)
                .collect()
        }
    };

    match assigned.as_slice() {
        [only] => return Ok(*only),
        [] if kind == RefKind::Issue => {
            bail!("no assigned issue with iid {iid} — pass --project-id explicitly")
        }
        [] => {} // MR: fall through to the search corpus below.
        many => bail!(
            "iid {iid} is ambiguous across {} assigned projects — pass --project-id",
            many.len()
        ),
    }

    // MR fallback: the whole search corpus, not just assigned MRs.
    let reply = client
        .search(
            format!("#{iid}"),
            Some(vec!["merge_requests".to_string()]),
            Some(SEARCH_FALLBACK_LIMIT),
        )
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("Search", e))?;
    let matches: Vec<i64> = reply
        .merge_requests
        .iter()
        .filter(|m| m.iid == iid)
        .map(|m| m.project_id)
        .collect();
    match matches.as_slice() {
        [] => bail!("no known merge request with iid {iid} — pass --project-id explicitly"),
        [only] => Ok(*only),
        many => bail!(
            "MR iid {iid} is ambiguous across {} projects — pass --project-id",
            many.len()
        ),
    }
}
