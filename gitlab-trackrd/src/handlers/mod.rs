//! Varlink method implementations — orchestration only.
//!
//! Each method is a short cascade: consult the cache, fall back to GitLab,
//! reply. GitLab errors become `GitlabError` varlink replies; cache failures
//! are logged and treated as a miss so the daemon stays available.
//!
//! Split across submodules to keep each file readable:
//! - [`refresh`] — the background cache/history warm-up and refresh cascade.
//! - [`varlink`] — the [`VarlinkInterface`](gitlab_trackr_api::VarlinkInterface)
//!   method impls plus the write-path deferral helpers they use.
//!
//! This module holds the shared connection types, the helpers every submodule
//! reaches for, and the small pure validators.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Notify, RwLock};

use gitlab_trackr_api::NotAuthReason;

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::config::SharedConfig;
use crate::error::{DormancyReason, Error};
use crate::gitlab::{GitlabApi, GitlabClient};
use crate::history::HistoryCache;
use crate::queue::RetryQueue;
use crate::search::SearchCache;

mod refresh;
mod varlink;

#[cfg(test)]
mod tests;

/// Live GitLab connection. Carries enough state for `WhoAmI` to answer without
/// a round-trip.
#[derive(Clone)]
pub struct Session {
    pub gitlab: Arc<dyn GitlabApi>,
    pub host: String,
    pub user_id: i64,
}

impl Session {
    pub fn from_client(client: GitlabClient) -> Self {
        let host = client.host().to_string();
        let user_id = client.current_user_id();
        Self {
            gitlab: Arc::new(client),
            host,
            user_id,
        }
    }
}

/// Connection state the daemon shares between the handlers, the retry queue,
/// and the background refresh task.
///
/// `Dormant` carries *why* there is no session (see [`DormancyReason`]) so the
/// CLI can report a specific cause instead of a bare "not authenticated".
pub enum ConnState {
    Connected(Session),
    Dormant(DormancyReason),
}

impl ConnState {
    /// The live GitLab client, if connected. Used by the retry-queue worker,
    /// which only needs the client and treats any dormant state as "defer".
    pub fn gitlab(&self) -> Option<Arc<dyn GitlabApi>> {
        match self {
            Self::Connected(s) => Some(s.gitlab.clone()),
            Self::Dormant(_) => None,
        }
    }
}

pub type SessionSlot = Arc<RwLock<ConnState>>;

pub struct Handlers {
    pub session: SessionSlot,
    pub cache: Arc<IssueCache>,
    pub boards: Arc<BoardCache>,
    pub history: Arc<HistoryCache>,
    pub search: Arc<SearchCache>,
    /// Serializes search-cache syncs: warm-up, the periodic loop, and
    /// `ClearCache` can all trigger one concurrently, and a second concurrent
    /// sync would only duplicate work — losers `try_lock` and skip.
    pub search_sync_gate: Mutex<()>,
    pub queue: RetryQueue,
    /// Live daemon config; history windows are read from `config.history` at use
    /// time so a hot reload takes effect without a restart.
    pub config: SharedConfig,
    /// Nudged the instant a runtime GitLab call fails transiently and the session
    /// is demoted to `Dormant(Unreachable)` (see [`crate::reconnect::commit_unreachable`]),
    /// waking the background reconnect supervisor so it re-engages at once instead
    /// of only at startup.
    pub reconnect_signal: Arc<Notify>,
}

impl Handlers {
    /// Resolve the live GitLab client, or `NotAuthenticated` carrying the
    /// dormancy reason.
    async fn gitlab(&self) -> std::result::Result<Arc<dyn GitlabApi>, DormancyReason> {
        match &*self.session.read().await {
            ConnState::Connected(s) => Ok(s.gitlab.clone()),
            ConnState::Dormant(r) => Err(r.clone()),
        }
    }

    /// Resolve the full session, or `NotAuthenticated` carrying the dormancy
    /// reason.
    async fn current_session(&self) -> std::result::Result<Session, DormancyReason> {
        match &*self.session.read().await {
            ConnState::Connected(s) => Ok(s.clone()),
            ConnState::Dormant(r) => Err(r.clone()),
        }
    }

    /// Route a GitLab error observed by a background refresh into the connection
    /// state: a transient (network) failure demotes the live session to
    /// `Dormant(Unreachable)` and wakes the reconnect supervisor, so a connection
    /// lost mid-run is noticed and retried the same way one down at boot is.
    /// `gitlab` is the client the failed call used; demotion is guarded on its
    /// identity so a stale in-flight fetch can't clobber a session a concurrent
    /// `tt login` just established (see [`crate::reconnect::commit_unreachable`]).
    /// Any non-transient error (e.g. a mid-run token revocation, not
    /// auto-retryable) is left to the caller's log. Idempotent: a second call
    /// while already dormant is a no-op.
    ///
    /// Only the background refresh paths call this. The write handlers queue on a
    /// transient failure (the retry queue drains them) rather than demoting, so a
    /// single blipped write can't tear the whole session down; the periodic
    /// refresh — which retries internally before failing — stays the demotion
    /// authority.
    async fn note_gitlab_error(&self, gitlab: &Arc<dyn GitlabApi>, e: &Error) {
        if let Error::Transient(detail) = e {
            crate::reconnect::commit_unreachable(
                &self.session,
                &self.reconnect_signal,
                gitlab,
                detail.clone(),
            )
            .await;
        }
    }
}

/// Extract the varlink `(reason, detail)` pair from a dormancy error so the six
/// `reply_not_authenticated` sites don't each repeat the match. The error is
/// always `NotAuthenticated` here (all `gitlab()`/`current_session()` yield);
/// the fallback is purely defensive.
fn dormant_args(reason: &DormancyReason) -> (Option<NotAuthReason>, Option<String>) {
    (Some(reason.reason()), reason.detail())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Reject obviously-malformed issue references up front (eager pre-check), so a
/// doomed request is never attempted or queued. Returns the error message when
/// invalid.
fn issue_ref_error(project_id: i64, issue_iid: i64) -> Option<String> {
    (project_id <= 0 || issue_iid <= 0)
        .then(|| format!("invalid issue reference (project {project_id}, iid {issue_iid})"))
}

/// Permissive sanity check for a GitLab time-tracking duration (`30m`,
/// `1h30m`, `1.5h`, `2d`). Rejects empties and obvious typos (`abc`, `1x`)
/// without trying to be a full GitLab-compatible parser — valid syntax is never
/// refused, so the only false negatives would need a unit GitLab doesn't use.
fn looks_like_duration(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let mut has_digit = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            has_digit = true;
        } else if c != '.'
            && !c.is_whitespace()
            && !matches!(c.to_ascii_lowercase(), 's' | 'm' | 'h' | 'd' | 'w' | 'o')
        {
            return false;
        }
    }
    has_digit
}
