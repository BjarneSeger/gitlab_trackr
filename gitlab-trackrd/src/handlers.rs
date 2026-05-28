//! Varlink method implementations — orchestration only.
//!
//! Each method is a short cascade: consult the cache, fall back to GitLab,
//! reply. GitLab errors become `GitlabError` varlink replies; cache failures
//! are logged and treated as a miss so the daemon stays available.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_AssignSelf, Call_ClearCache, Call_ClearFailures, Call_CloseIssue, Call_DismissFailure,
    Call_GetAssignedIssues, Call_GetFailures, Call_GetHistory, Call_Login, Call_Logout,
    Call_PostTime, Call_RetryFailure, Call_UnassignSelf, Call_WhoAmI, FailedTask, HistoryEvent,
    Issue, VarlinkInterface,
};

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::error::{Error, Result};
use crate::gitlab::{FetchedTimelog, GitlabApi, GitlabClient, IssueWithLabels};
use crate::history::{ACTIVE_WINDOW, HistoryCache, SEMI_WINDOW, STALE_WINDOW, StoredTimelog};
use crate::queue::RetryQueue;
use crate::secrets::{self, Credentials};

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

/// Slot the daemon shares between the handlers, the retry queue, and the
/// background refresh task. `None` ⇒ dormant (no GitLab connection yet).
pub type SessionSlot = Arc<RwLock<Option<Session>>>;

pub struct Handlers {
    pub session: SessionSlot,
    pub cache: Arc<IssueCache>,
    pub boards: Arc<BoardCache>,
    pub history: Arc<HistoryCache>,
    pub queue: RetryQueue,
}

impl Handlers {
    /// Resolve the live GitLab client or return `NotAuthenticated`.
    async fn gitlab(&self) -> Result<Arc<dyn GitlabApi>> {
        self.session
            .read()
            .await
            .as_ref()
            .map(|s| s.gitlab.clone())
            .ok_or(Error::NotAuthenticated)
    }

    /// Resolve the full session or return `NotAuthenticated`.
    async fn current_session(&self) -> Result<Session> {
        self.session
            .read()
            .await
            .clone()
            .ok_or(Error::NotAuthenticated)
    }
}

impl Handlers {
    /// Unconditionally fetch issues and boards from GitLab and update both caches.
    /// Called by the background refresh task; errors are logged and not propagated.
    pub async fn refresh_cache(&self) {
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping background cache refresh");
                return;
            }
        };

        match gitlab.fetch_assigned_issues(None).await {
            Ok(raw) => {
                let issues = enrich_graph_status(&*gitlab, &self.boards, raw).await;
                if let Err(e) = self.cache.put(&issues) {
                    warn!(error = %e, "background cache write failed");
                } else {
                    info!(count = issues.len(), "background cache refresh complete");
                }
            }
            Err(e) => {
                warn!(error = %e, "background cache refresh: GitLab fetch failed");
            }
        }

        self.refresh_history_window(&gitlab, ACTIVE_WINDOW).await;
    }

    /// Daily refresh of the semi-active tier (24h–30d) followed by a prune of
    /// anything past the stale window. Acquires the session itself; no-op when
    /// dormant. Called by the daily background loop.
    pub async fn refresh_history_daily(&self) {
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping daily history refresh");
                return;
            }
        };
        self.refresh_history_window(&gitlab, SEMI_WINDOW).await;
        self.prune_history();
    }

    /// One-time backfill of the full stale window (up to 90d) so the older,
    /// never-refreshed bands are populated. Acquires the session itself; no-op
    /// when dormant. Called once at startup and after a full cache clear.
    pub async fn backfill_history(&self) {
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping history backfill");
                return;
            }
        };
        self.refresh_history_window(&gitlab, STALE_WINDOW).await;
        self.prune_history();
    }

    /// Pull the user's GitLab timelogs spanning `window` back from now, enrich
    /// with cached issue data, and store. Best-effort — each step's failure is
    /// logged and swallowed. Pruning is a separate step (see [`Self::prune_history`])
    /// so the frequent active refresh doesn't scan the whole table every time.
    async fn refresh_history_window(&self, gitlab: &Arc<dyn GitlabApi>, window: Duration) {
        let now = now_secs();
        let cutoff = now.saturating_sub(window.as_secs());
        let since = chrono::DateTime::<chrono::Utc>::from_timestamp(cutoff as i64, 0)
            .unwrap_or_else(chrono::Utc::now);

        let fetched = match gitlab.fetch_my_timelogs(since).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "timelog refresh: GitLab fetch failed");
                return;
            }
        };

        let cached_issues = self.cache.get().ok().flatten().unwrap_or_default();
        let by_url: HashMap<&str, &Issue> = cached_issues
            .iter()
            .map(|i| (i.web_url.as_str(), i))
            .collect();
        let by_iid: HashMap<i64, &Issue> = cached_issues.iter().map(|i| (i.iid, i)).collect();

        let stored: Vec<StoredTimelog> = fetched
            .into_iter()
            .map(|t| enrich_timelog(t, &by_url, &by_iid))
            .collect();

        if let Err(e) = self.history.upsert(&stored) {
            warn!(error = %e, "history upsert failed");
        } else {
            info!(
                count = stored.len(),
                window_secs = window.as_secs(),
                "history refresh complete"
            );
        }
    }

    /// Drop history entries that have aged past the stale window.
    fn prune_history(&self) {
        let cutoff = now_secs().saturating_sub(STALE_WINDOW.as_secs());
        match self.history.prune(cutoff) {
            Ok(0) => {}
            Ok(n) => info!(removed = n, "pruned stale history entries"),
            Err(e) => warn!(error = %e, "history prune failed"),
        }
    }

    /// Drop an issue from the assigned-issues cache so a close/unassign is
    /// reflected in `tt list` immediately. Best-effort — a failure is logged
    /// and swallowed (the next refresh will reconcile the list anyway).
    fn forget_cached_issue(&self, project_id: i64, issue_iid: i64) {
        match self.cache.remove_issue(project_id, issue_iid) {
            Ok(true) => debug!(project_id, issue_iid, "removed issue from cache"),
            Ok(false) => {}
            Err(e) => warn!(error = %e, project_id, issue_iid, "cache issue removal failed"),
        }
    }
}

/// Fill `graph_status` on each issue using cached or freshly-fetched board
/// list labels. Best-effort: a board fetch failure for a project leaves
/// that project's issues with an empty `graph_status`.
async fn enrich_graph_status(
    gitlab: &dyn GitlabApi,
    boards: &BoardCache,
    raw: Vec<IssueWithLabels>,
) -> Vec<Issue> {
    let mut by_project: HashMap<i64, Option<Vec<String>>> = HashMap::new();
    let mut out = Vec::with_capacity(raw.len());

    for IssueWithLabels { mut issue, labels } in raw {
        let project_id = issue.project_id;

        let board_labels = match by_project.get(&project_id) {
            Some(entry) => entry.clone(),
            None => {
                let resolved = match boards.get(project_id) {
                    Ok(Some(cached)) => Some(cached),
                    Ok(None) => match gitlab.fetch_board_list_labels(project_id).await {
                        Ok(fetched) => {
                            if let Err(e) = boards.put(project_id, fetched.clone()) {
                                warn!(error = %e, project_id, "failed to persist board labels");
                            }
                            Some(fetched)
                        }
                        Err(e) => {
                            warn!(error = %e, project_id, "board fetch failed; graph_status will be empty");
                            None
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, project_id, "board cache read failed");
                        None
                    }
                };
                by_project.insert(project_id, resolved.clone());
                resolved
            }
        };

        issue.graph_status = match board_labels {
            Some(board) => labels
                .iter()
                .find(|l| board.iter().any(|b| b == *l))
                .cloned()
                .unwrap_or_else(|| issue.state.clone()),
            None => String::new(),
        };

        out.push(issue);
    }

    out
}

#[async_trait::async_trait]
impl VarlinkInterface for Handlers {
    #[instrument(skip(self, call))]
    async fn get_assigned_issues(
        &self,
        call: &mut dyn Call_GetAssignedIssues,
        groups: Option<Vec<String>>,
    ) -> varlink::Result<()> {
        match self.cache.get() {
            Ok(Some(issues)) => {
                debug!(count = issues.len(), "cache hit");
                return call.reply(issues);
            }
            Ok(None) => debug!("cache miss, fetching from GitLab"),
            Err(e) => warn!("cache read failed, treating as miss: {e}"),
        }

        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => return call.reply_not_authenticated(),
        };

        let fetched = if let Some(groups) = groups {
            gitlab.fetch_group_issues(groups).await
        } else {
            gitlab.fetch_assigned_issues(None).await
        };

        match fetched {
            Ok(raw) => {
                let issues = enrich_graph_status(&*gitlab, &self.boards, raw).await;
                if let Err(e) = self.cache.put(&issues) {
                    warn!(error = %e, "cache write failed");
                }
                call.reply(issues)
            }
            Err(e) => {
                warn!(error = %e, "GitLab fetch failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn clear_cache(
        &self,
        call: &mut dyn Call_ClearCache,
        scope: Option<Vec<String>>,
    ) -> varlink::Result<()> {
        // An empty / absent scope means "clear everything".
        let scopes = scope.unwrap_or_default();
        let all = scopes.is_empty();
        let want = |s: &str| all || scopes.iter().any(|x| x == s);

        if want("issues") {
            if let Err(e) = self.cache.clear() {
                warn!("issue cache clear failed: {e}");
            } else {
                info!("issue cache cleared");
            }
            if let Err(e) = self.boards.clear() {
                warn!("board cache clear failed: {e}");
            } else {
                info!("board cache cleared");
            }
        }

        // History tiers. `all` wipes the whole store in one shot; individual
        // flags clear just their `spent_at` band (active is open-ended on top,
        // the bands abut at the active/semi window boundaries).
        let now = now_secs();
        let active_start = now.saturating_sub(ACTIVE_WINDOW.as_secs());
        let semi_start = now.saturating_sub(SEMI_WINDOW.as_secs());

        if all {
            if let Err(e) = self.history.clear() {
                warn!("history clear failed: {e}");
            } else {
                info!("history cleared");
            }
        } else {
            if want("active") {
                clear_band(&self.history, active_start, u64::MAX, "active");
            }
            if want("semi") {
                clear_band(&self.history, semi_start, active_start, "semi");
            }
            if want("stale") {
                clear_band(&self.history, 0, semi_start, "stale");
            }
        }

        // Re-fetch what we cleared so it repopulates immediately rather than
        // waiting for the next scheduled refresh (which, for the stale tier,
        // only happens at startup). Skipped when dormant.
        if let Ok(gitlab) = self.gitlab().await {
            if all {
                // Full wipe: warm issues/boards first (history enrichment reads
                // project IDs from the issue cache), then backfill every tier —
                // exactly the startup sequence.
                self.refresh_cache().await;
                self.backfill_history().await;
            } else if want("stale") {
                // `stale`'s window subsumes the narrower tiers.
                self.refresh_history_window(&gitlab, STALE_WINDOW).await;
                self.prune_history();
            } else if want("semi") {
                self.refresh_history_window(&gitlab, SEMI_WINDOW).await;
            } else if want("active") {
                self.refresh_history_window(&gitlab, ACTIVE_WINDOW).await;
            }
        }

        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn post_time(
        &self,
        call: &mut dyn Call_PostTime,
        project_id: i64,
        issue_iid: i64,
        duration: String,
        summary: Option<String>,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, issue_iid) {
            return call.reply_gitlab_error(msg);
        }
        if !looks_like_duration(&duration) {
            return call.reply_gitlab_error(format!("invalid duration: {duration:?}"));
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => return call.reply_not_authenticated(),
        };
        match gitlab
            .add_spent_time(project_id, issue_iid, &duration, summary.as_deref())
            .await
        {
            Ok(()) => {
                info!(project_id, issue_iid, duration, "posted time");
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "PostTime network error, queuing for retry");
                let issue_id = self.cache.get().ok().flatten().and_then(|issues| {
                    issues
                        .into_iter()
                        .find(|i| i.project_id == project_id && i.iid == issue_iid)
                        .map(|i| i.id)
                });
                self.queue
                    .post_time(project_id, issue_iid, duration, summary, issue_id)
                    .await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "PostTime rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn get_history(
        &self,
        call: &mut dyn Call_GetHistory,
        days: Option<i64>,
    ) -> varlink::Result<()> {
        let now = now_secs();
        let days = days.unwrap_or(7).max(0) as u64;
        let cutoff = now.saturating_sub(days.saturating_mul(86_400));

        let cached_issues = self.cache.get().ok().flatten().unwrap_or_default();
        let by_key: HashMap<(i64, i64), &Issue> = cached_issues
            .iter()
            .map(|i| ((i.project_id, i.iid), i))
            .collect();

        let mut events: Vec<HistoryEvent> = Vec::new();

        match self.queue.pending_post_time() {
            Ok(pending) => {
                for p in pending {
                    let issue = by_key.get(&(p.project_id, p.issue_iid));
                    events.push(HistoryEvent {
                        timestamp: p.queued_at_secs as i64,
                        source: "queued".to_string(),
                        project_id: p.project_id,
                        issue_iid: p.issue_iid,
                        issue_title: issue.map(|i| i.title.clone()).unwrap_or_default(),
                        web_url: issue.map(|i| i.web_url.clone()).unwrap_or_default(),
                        duration: p.duration,
                        summary: p.summary.unwrap_or_default(),
                    });
                }
            }
            Err(e) => warn!(error = %e, "queue scan failed; queued events omitted"),
        }

        match self.history.all_since(cutoff) {
            Ok(entries) => {
                for e in entries {
                    events.push(HistoryEvent {
                        timestamp: e.spent_at_secs as i64,
                        source: "gitlab".to_string(),
                        project_id: e.project_id,
                        issue_iid: e.issue_iid,
                        issue_title: e.issue_title,
                        web_url: e.web_url,
                        duration: e.duration,
                        summary: e.summary,
                    });
                }
            }
            Err(e) => warn!(error = %e, "history read failed; returning queued only"),
        }

        call.reply(events)
    }

    #[instrument(skip(self, call))]
    async fn get_failures(&self, call: &mut dyn Call_GetFailures) -> varlink::Result<()> {
        let failures = match self.queue.failures() {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "dead-letter read failed; returning empty");
                Vec::new()
            }
        };
        let out = failures
            .into_iter()
            .map(|f| FailedTask {
                id: f.id as i64,
                op: f.op_kind.to_string(),
                project_id: f.project_id,
                issue_iid: f.issue_iid,
                detail: f.detail,
                error: f.error,
                queued_at: f.queued_at_secs as i64,
                failed_at: f.failed_at_secs as i64,
            })
            .collect();
        call.reply(out)
    }

    #[instrument(skip(self, call))]
    async fn retry_failure(
        &self,
        call: &mut dyn Call_RetryFailure,
        id: i64,
    ) -> varlink::Result<()> {
        match self.queue.retry_failure(id as u64).await {
            Ok(true) => {
                info!(id, "re-enqueued dead-letter task");
                call.reply()
            }
            Ok(false) => call.reply_gitlab_error(format!("no failed task with id {id}")),
            Err(e) => {
                warn!(error = %e, id, "retry_failure failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn dismiss_failure(
        &self,
        call: &mut dyn Call_DismissFailure,
        id: i64,
    ) -> varlink::Result<()> {
        match self.queue.dismiss_failure(id as u64) {
            Ok(true) => {
                info!(id, "dismissed dead-letter task");
                call.reply()
            }
            Ok(false) => call.reply_gitlab_error(format!("no failed task with id {id}")),
            Err(e) => {
                warn!(error = %e, id, "dismiss_failure failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn clear_failures(&self, call: &mut dyn Call_ClearFailures) -> varlink::Result<()> {
        if let Err(e) = self.queue.clear_failures() {
            warn!(error = %e, "clear_failures failed");
            return call.reply_gitlab_error(e.to_string());
        }
        info!("cleared dead-letter queue");
        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn close_issue(
        &self,
        call: &mut dyn Call_CloseIssue,
        project_id: i64,
        issue_iid: i64,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, issue_iid) {
            return call.reply_gitlab_error(msg);
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => return call.reply_not_authenticated(),
        };
        match gitlab.close_issue(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "closed issue");
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "CloseIssue network error, queuing for retry");
                self.queue.close_issue(project_id, issue_iid).await;
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "CloseIssue rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn assign_self(
        &self,
        call: &mut dyn Call_AssignSelf,
        project_id: i64,
        issue_iid: i64,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, issue_iid) {
            return call.reply_gitlab_error(msg);
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => return call.reply_not_authenticated(),
        };
        match gitlab.assign_self(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "assigned self");
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "AssignSelf network error, queuing for retry");
                self.queue.assign_self(project_id, issue_iid).await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "AssignSelf rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn unassign_self(
        &self,
        call: &mut dyn Call_UnassignSelf,
        project_id: i64,
        issue_iid: i64,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, issue_iid) {
            return call.reply_gitlab_error(msg);
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => return call.reply_not_authenticated(),
        };
        match gitlab.unassign_self(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "unassigned self");
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "UnassignSelf network error, queuing for retry");
                self.queue.unassign_self(project_id, issue_iid).await;
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "UnassignSelf rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call, token))]
    async fn login(
        &self,
        call: &mut dyn Call_Login,
        host: String,
        token: String,
    ) -> varlink::Result<()> {
        let client = match GitlabClient::connect(&host, &token).await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, host, "Login: GitLab rejected the token");
                return call.reply_gitlab_error(e.to_string());
            }
        };
        let creds = Credentials {
            host: host.clone(),
            token,
        };
        if let Err(e) = secrets::store(&creds).await {
            warn!(error = %e, "Login: keychain write failed");
            return call.reply_gitlab_error(format!("keychain write failed: {e}"));
        }
        let session = Session::from_client(client);
        info!(host, user_id = session.user_id, "logged in");
        *self.session.write().await = Some(session);
        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn logout(&self, call: &mut dyn Call_Logout) -> varlink::Result<()> {
        *self.session.write().await = None;
        if let Err(e) = secrets::delete().await {
            warn!(error = %e, "Logout: keychain delete failed");
            return call.reply_gitlab_error(format!("keychain delete failed: {e}"));
        }
        info!("logged out");
        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn who_am_i(&self, call: &mut dyn Call_WhoAmI) -> varlink::Result<()> {
        match self.current_session().await {
            Ok(s) => call.reply(s.host, s.user_id),
            Err(_) => call.reply_not_authenticated(),
        }
    }
}

/// Fill `project_id` on a fetched timelog from the issue cache.
///
/// GitLab's GraphQL `Timelog.issue` doesn't expose `project_id` directly, so
/// we match by `web_url` first (exact, robust) and fall back to `iid` (which
/// can collide across projects but is better than nothing). If neither hits,
/// `project_id` stays at `0` — the client can still display the entry.
fn enrich_timelog(
    t: FetchedTimelog,
    by_url: &HashMap<&str, &Issue>,
    by_iid: &HashMap<i64, &Issue>,
) -> StoredTimelog {
    let issue = by_url
        .get(t.web_url.as_str())
        .or_else(|| by_iid.get(&t.issue_iid));
    StoredTimelog {
        timelog_id: t.timelog_id,
        spent_at_secs: t.spent_at_secs,
        project_id: issue.map(|i| i.project_id).unwrap_or(0),
        issue_iid: t.issue_iid,
        issue_title: if t.issue_title.is_empty() {
            issue.map(|i| i.title.clone()).unwrap_or_default()
        } else {
            t.issue_title
        },
        web_url: if t.web_url.is_empty() {
            issue.map(|i| i.web_url.clone()).unwrap_or_default()
        } else {
            t.web_url
        },
        duration: t.duration,
        summary: t.summary,
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Clear one history tier's `spent_at` band, logging the outcome.
fn clear_band(history: &HistoryCache, min_secs: u64, max_secs: u64, tier: &str) {
    match history.clear_between(min_secs, max_secs) {
        Ok(n) => info!(removed = n, tier, "history tier cleared"),
        Err(e) => warn!(error = %e, tier, "history tier clear failed"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as TrackrResult;
    use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn issue(project_id: i64, iid: i64, title: &str, web_url: &str) -> Issue {
        Issue {
            id: 0,
            iid,
            project_id,
            title: title.to_string(),
            web_url: web_url.to_string(),
            state: "opened".to_string(),
            parent: String::new(),
            total_time: String::new(),
            graph_status: String::new(),
        }
    }

    // ── enrich_timelog ──────────────────────────────────────────────────────

    #[test]
    fn enrich_timelog_matches_by_web_url() {
        let i = issue(7, 42, "Title", "https://gl/-/issues/42");
        let by_url = HashMap::from([(i.web_url.as_str(), &i)]);
        let by_iid = HashMap::<i64, &Issue>::new();

        let t = FetchedTimelog {
            timelog_id: 1,
            spent_at_secs: 100,
            issue_iid: 42,
            issue_title: "fresh".to_string(),
            web_url: "https://gl/-/issues/42".to_string(),
            duration: "1h".to_string(),
            summary: "s".to_string(),
        };
        let r = enrich_timelog(t, &by_url, &by_iid);
        assert_eq!(r.project_id, 7);
        assert_eq!(r.issue_title, "fresh", "fresh title preserved");
        assert_eq!(r.web_url, "https://gl/-/issues/42");
    }

    #[test]
    fn enrich_timelog_falls_back_to_iid_when_url_misses() {
        let i = issue(7, 42, "Cached", "https://gl/-/issues/42");
        let by_url = HashMap::<&str, &Issue>::new();
        let by_iid = HashMap::from([(42_i64, &i)]);

        let t = FetchedTimelog {
            timelog_id: 1,
            spent_at_secs: 100,
            issue_iid: 42,
            issue_title: "fresh".to_string(),
            web_url: String::new(),
            duration: "1h".to_string(),
            summary: String::new(),
        };
        let r = enrich_timelog(t, &by_url, &by_iid);
        assert_eq!(r.project_id, 7);
        assert_eq!(
            r.web_url, "https://gl/-/issues/42",
            "empty url filled from cache"
        );
    }

    #[test]
    fn enrich_timelog_no_match_leaves_project_id_zero() {
        let by_url = HashMap::<&str, &Issue>::new();
        let by_iid = HashMap::<i64, &Issue>::new();

        let t = FetchedTimelog {
            timelog_id: 1,
            spent_at_secs: 100,
            issue_iid: 99,
            issue_title: "fresh".to_string(),
            web_url: "https://gl/-/issues/99".to_string(),
            duration: "30m".to_string(),
            summary: String::new(),
        };
        let r = enrich_timelog(t, &by_url, &by_iid);
        assert_eq!(r.project_id, 0);
        assert_eq!(r.issue_title, "fresh");
        assert_eq!(r.web_url, "https://gl/-/issues/99");
    }

    #[test]
    fn enrich_timelog_empty_title_filled_from_cache() {
        let i = issue(7, 42, "From cache", "https://gl/-/issues/42");
        let by_url = HashMap::from([(i.web_url.as_str(), &i)]);
        let by_iid = HashMap::<i64, &Issue>::new();

        let t = FetchedTimelog {
            timelog_id: 1,
            spent_at_secs: 100,
            issue_iid: 42,
            issue_title: String::new(),
            web_url: "https://gl/-/issues/42".to_string(),
            duration: "1h".to_string(),
            summary: String::new(),
        };
        let r = enrich_timelog(t, &by_url, &by_iid);
        assert_eq!(r.issue_title, "From cache");
    }

    // ── enrich_graph_status with FakeGitlab ────────────────────────────────

    /// Minimal `GitlabApi` impl that returns pre-canned `fetch_board_list_labels`
    /// responses and counts how many times each method was called.
    #[derive(Default)]
    struct FakeGitlab {
        board_labels: Mutex<HashMap<i64, TrackrResult<Vec<String>>>>,
        board_calls: AtomicUsize,
    }

    impl FakeGitlab {
        fn with_board_labels(project_id: i64, labels: Vec<String>) -> Self {
            let me = Self::default();
            me.board_labels
                .lock()
                .unwrap()
                .insert(project_id, Ok(labels));
            me
        }

        fn with_board_error(project_id: i64) -> Self {
            let me = Self::default();
            me.board_labels.lock().unwrap().insert(
                project_id,
                Err(crate::error::Error::Transient("offline".to_string())),
            );
            me
        }

        fn board_calls(&self) -> usize {
            self.board_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl GitlabApi for FakeGitlab {
        async fn fetch_assigned_issues(
            &self,
            _group: Option<String>,
        ) -> TrackrResult<Vec<IssueWithLabels>> {
            unimplemented!("not used in enrich_graph_status tests")
        }
        async fn fetch_group_issues(
            &self,
            _groups: Vec<String>,
        ) -> TrackrResult<Vec<IssueWithLabels>> {
            unimplemented!()
        }
        async fn add_spent_time(
            &self,
            _project_id: i64,
            _issue_iid: i64,
            _duration: &str,
            _summary: Option<&str>,
        ) -> TrackrResult<()> {
            unimplemented!()
        }
        async fn create_timelog(
            &self,
            _issue_id: i64,
            _duration: &str,
            _summary: &str,
            _spent_at: chrono::DateTime<chrono::Utc>,
        ) -> TrackrResult<()> {
            unimplemented!()
        }
        async fn fetch_my_timelogs(
            &self,
            _since: chrono::DateTime<chrono::Utc>,
        ) -> TrackrResult<Vec<FetchedTimelog>> {
            unimplemented!()
        }
        async fn close_issue(&self, _project_id: i64, _issue_iid: i64) -> TrackrResult<()> {
            unimplemented!()
        }
        async fn assign_self(&self, _project_id: i64, _issue_iid: i64) -> TrackrResult<()> {
            unimplemented!()
        }
        async fn unassign_self(&self, _project_id: i64, _issue_iid: i64) -> TrackrResult<()> {
            unimplemented!()
        }
        async fn fetch_board_list_labels(&self, project_id: i64) -> TrackrResult<Vec<String>> {
            self.board_calls.fetch_add(1, Ordering::SeqCst);
            match self.board_labels.lock().unwrap().remove(&project_id) {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(e),
                None => Ok(vec![]),
            }
        }
    }

    fn boards_cache() -> (BoardCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boards.redb");
        (BoardCache::open(&path).unwrap(), dir)
    }

    fn iwl(project_id: i64, state: &str, labels: &[&str]) -> IssueWithLabels {
        IssueWithLabels {
            issue: Issue {
                id: 1,
                iid: 1,
                project_id,
                title: "t".into(),
                web_url: "u".into(),
                state: state.into(),
                parent: String::new(),
                total_time: String::new(),
                graph_status: String::new(),
            },
            labels: labels.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn enrich_graph_status_uses_cached_board_labels() {
        let (boards, _td) = boards_cache();
        boards.put(7, vec!["Doing".into(), "Done".into()]).unwrap();
        let gitlab = FakeGitlab::default();

        let out =
            enrich_graph_status(&gitlab, &boards, vec![iwl(7, "opened", &["bug", "Doing"])]).await;

        assert_eq!(out[0].graph_status, "Doing");
        assert_eq!(gitlab.board_calls(), 0, "cache hit must skip the API call");
    }

    #[tokio::test]
    async fn enrich_graph_status_fetches_and_persists_on_cache_miss() {
        let (boards, _td) = boards_cache();
        let gitlab = FakeGitlab::with_board_labels(7, vec!["Review".into()]);

        let out = enrich_graph_status(&gitlab, &boards, vec![iwl(7, "opened", &["Review"])]).await;

        assert_eq!(out[0].graph_status, "Review");
        assert_eq!(gitlab.board_calls(), 1);
        assert_eq!(
            boards.get(7).unwrap(),
            Some(vec!["Review".into()]),
            "freshly fetched labels are persisted"
        );
    }

    #[tokio::test]
    async fn enrich_graph_status_empty_when_fetch_fails() {
        let (boards, _td) = boards_cache();
        let gitlab = FakeGitlab::with_board_error(7);

        let out = enrich_graph_status(&gitlab, &boards, vec![iwl(7, "opened", &["bug"])]).await;

        assert!(out[0].graph_status.is_empty());
        assert_eq!(boards.get(7).unwrap(), None, "errored fetch is not cached");
    }

    #[tokio::test]
    async fn enrich_graph_status_falls_back_to_state_when_no_label_matches() {
        let (boards, _td) = boards_cache();
        boards.put(7, vec!["Doing".into()]).unwrap();
        let gitlab = FakeGitlab::default();

        let out =
            enrich_graph_status(&gitlab, &boards, vec![iwl(7, "opened", &["bug", "high"])]).await;

        assert_eq!(out[0].graph_status, "opened");
    }

    #[tokio::test]
    async fn enrich_graph_status_picks_first_matching_label() {
        let (boards, _td) = boards_cache();
        boards
            .put(7, vec!["Backlog".into(), "Doing".into(), "Review".into()])
            .unwrap();
        let gitlab = FakeGitlab::default();

        // Issue labels: ["random", "Review", "Doing"] — first to also appear in
        // the board list is "Review".
        let out = enrich_graph_status(
            &gitlab,
            &boards,
            vec![iwl(7, "opened", &["random", "Review", "Doing"])],
        )
        .await;

        assert_eq!(out[0].graph_status, "Review");
    }

    #[tokio::test]
    async fn enrich_graph_status_caches_per_project_within_one_call() {
        let (boards, _td) = boards_cache();
        let gitlab = FakeGitlab::with_board_labels(7, vec!["Doing".into()]);

        let out = enrich_graph_status(
            &gitlab,
            &boards,
            vec![
                iwl(7, "opened", &["Doing"]),
                iwl(7, "opened", &["Doing"]),
                iwl(7, "opened", &["nope"]),
            ],
        )
        .await;

        assert_eq!(out.len(), 3);
        assert_eq!(gitlab.board_calls(), 1, "single fetch per project");
    }

    // ── clear_cache scoping ────────────────────────────────────────────────

    /// Build `Handlers` with a dormant session (no GitLab), so `clear_cache`
    /// clears without the follow-up re-fetch — letting us assert the bands.
    fn dormant_handlers() -> (Handlers, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let p = |name: &str| dir.path().join(name);
        let session: SessionSlot = Arc::new(RwLock::new(None));
        let cache = Arc::new(IssueCache::open(&p("cache.redb")).unwrap());
        let boards = Arc::new(BoardCache::open(&p("boards.redb")).unwrap());
        let history = Arc::new(HistoryCache::open(&p("history.redb")).unwrap());
        let queue = RetryQueue::new(Arc::clone(&session), &p("queue.redb")).unwrap();
        (
            Handlers {
                session,
                cache,
                boards,
                history,
                queue,
            },
            dir,
        )
    }

    fn stored(timelog_id: u64, spent_at_secs: u64) -> StoredTimelog {
        StoredTimelog {
            timelog_id,
            spent_at_secs,
            project_id: 1,
            issue_iid: 1,
            issue_title: "t".into(),
            web_url: "u".into(),
            duration: "1h".into(),
            summary: String::new(),
        }
    }

    #[tokio::test]
    async fn clear_cache_active_scope_only_clears_last_24h() {
        use gitlab_trackr_api::AsyncCall;
        let (h, _dir) = dormant_handlers();
        let now = now_secs();
        h.history
            .upsert(&[
                stored(1, now - 3_600),       // within 24h → active
                stored(2, now - 3 * 86_400),  // 3 days → semi
                stored(3, now - 45 * 86_400), // 45 days → stale
            ])
            .unwrap();

        let mut call = AsyncCall::default();
        h.clear_cache(
            &mut call as &mut dyn Call_ClearCache,
            Some(vec!["active".to_string()]),
        )
        .await
        .unwrap();

        let remaining: Vec<u64> = h
            .history
            .all_since(0)
            .unwrap()
            .iter()
            .map(|e| e.timelog_id)
            .collect();
        assert_eq!(remaining, vec![2, 3], "only the active-band entry is removed");
    }

    #[tokio::test]
    async fn clear_cache_no_scope_clears_all_history() {
        use gitlab_trackr_api::AsyncCall;
        let (h, _dir) = dormant_handlers();
        let now = now_secs();
        h.history
            .upsert(&[stored(1, now - 3_600), stored(2, now - 45 * 86_400)])
            .unwrap();

        let mut call = AsyncCall::default();
        h.clear_cache(&mut call as &mut dyn Call_ClearCache, None)
            .await
            .unwrap();

        assert!(h.history.all_since(0).unwrap().is_empty());
    }

    // ── eager pre-checks ───────────────────────────────────────────────────

    #[test]
    fn looks_like_duration_accepts_valid_and_rejects_typos() {
        for ok in ["1h", "30m", "1h30m", "1.5h", "2d", "1w", " 45m "] {
            assert!(looks_like_duration(ok), "should accept {ok:?}");
        }
        for bad in ["", "   ", "abc", "1x", "30 min"] {
            assert!(!looks_like_duration(bad), "should reject {bad:?}");
        }
    }

    #[tokio::test]
    async fn post_time_rejects_bad_duration_without_queueing() {
        use gitlab_trackr_api::AsyncCall;
        let (h, _dir) = dormant_handlers();
        let mut call = AsyncCall::default();
        h.post_time(
            &mut call as &mut dyn Call_PostTime,
            7,
            42,
            "abc".to_string(),
            None,
        )
        .await
        .unwrap();

        let reply = call.take_reply().expect("a reply");
        assert!(reply.error.is_some(), "bad duration → error reply");
        assert!(
            h.queue.pending_post_time().unwrap().is_empty(),
            "nothing enqueued"
        );
    }

    #[tokio::test]
    async fn close_issue_rejects_bad_issue_ref() {
        use gitlab_trackr_api::AsyncCall;
        let (h, _dir) = dormant_handlers();
        let mut call = AsyncCall::default();
        h.close_issue(&mut call as &mut dyn Call_CloseIssue, 0, 42)
            .await
            .unwrap();

        let reply = call.take_reply().expect("a reply");
        assert!(reply.error.is_some(), "project_id 0 → error reply");
    }
}
