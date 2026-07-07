//! The [`VarlinkInterface`] method implementations plus the write-path helpers
//! they lean on. Each method is a short cascade: consult the cache, fall back
//! to GitLab, reply — see the crate module docs for the error conventions.

use std::collections::HashMap;

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_AssignSelf, Call_ClearCache, Call_ClearFailures, Call_CloseIssue, Call_DismissFailure,
    Call_GetAssignedIssues, Call_GetFailures, Call_GetHistory, Call_Login, Call_Logout,
    Call_PostTime, Call_RetryFailure, Call_UnassignSelf, Call_WhoAmI, FailedTask, HistoryEvent,
    Issue, VarlinkInterface,
};

use crate::error::{DormancyReason, Error};
use crate::gitlab::GitlabClient;
use crate::history::HistoryCache;
use crate::secrets::{self, Credentials};

use super::{
    ConnState, Handlers, Session, dormant_args, issue_ref_error, looks_like_duration, now_secs,
};

impl Handlers {
    /// The global numeric issue ID (the one GraphQL embeds in
    /// `gid://gitlab/Issue/<id>`) for a cached `(project, iid)`, so a queued
    /// retry can use the GraphQL path. `None` when the issue isn't cached — the
    /// queue then falls back to REST without a `spent_at`.
    fn resolve_issue_id(&self, project_id: i64, issue_iid: i64) -> Option<i64> {
        self.cache.issue_id(project_id, issue_iid).ok().flatten()
    }

    /// Queue a `PostTime` write for retry when GitLab can't be reached right now
    /// (a known outage or a transient failure mid-call); the retry queue drains
    /// it on reconnect. Shared by the `Unreachable`-dormancy and transient-error
    /// arms of [`Self::post_time`].
    async fn defer_post_time(
        &self,
        project_id: i64,
        issue_iid: i64,
        duration: String,
        summary: Option<String>,
    ) {
        let issue_id = self.resolve_issue_id(project_id, issue_iid);
        self.queue
            .post_time(project_id, issue_iid, duration, summary, issue_id)
            .await;
    }

    /// Queue a `CloseIssue` write for retry and drop the issue from the cache so
    /// `tt list` reflects it at once. Shared by both deferral arms of
    /// [`Self::close_issue`].
    async fn defer_close_issue(&self, project_id: i64, issue_iid: i64) {
        self.queue.close_issue(project_id, issue_iid).await;
        self.forget_cached_issue(project_id, issue_iid);
    }

    /// Queue an `AssignSelf` write for retry. Shared by both deferral arms of
    /// [`Self::assign_self`].
    async fn defer_assign_self(&self, project_id: i64, issue_iid: i64) {
        self.queue.assign_self(project_id, issue_iid).await;
    }

    /// Queue an `UnassignSelf` write for retry and drop the issue from the cache.
    /// Shared by both deferral arms of [`Self::unassign_self`].
    async fn defer_unassign_self(&self, project_id: i64, issue_iid: i64) {
        self.queue.unassign_self(project_id, issue_iid).await;
        self.forget_cached_issue(project_id, issue_iid);
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

#[async_trait::async_trait]
impl VarlinkInterface for Handlers {
    #[instrument(skip(self, call))]
    async fn get_assigned_issues(
        &self,
        call: &mut dyn Call_GetAssignedIssues,
        groups: Option<Vec<String>>,
    ) -> varlink::Result<()> {
        let all = match self.cache.get() {
            Ok(Some(all)) => all,
            Ok(None) => {
                return match self.gitlab().await {
                    Ok(_) => call.reply(Vec::new()),
                    Err(e) => {
                        let (reason, detail) = dormant_args(&e);
                        call.reply_not_authenticated(reason, detail)
                    }
                };
            }
            Err(e) => {
                warn!("cache read failed, treating as empty: {e}");
                return call.reply(Vec::new());
            }
        };

        let issues = match groups {
            Some(groups) if !groups.is_empty() => {
                let mut seen = std::collections::HashSet::new();
                groups
                    .iter()
                    .flat_map(|g| self.cache.get_group(g).unwrap_or_default())
                    .filter(|i| seen.insert((i.project_id, i.iid)))
                    .collect()
            }
            _ => all,
        };

        debug!(count = issues.len(), "serving issues from cache");
        call.reply(issues)
    }

    #[instrument(skip(self, call))]
    async fn clear_cache(
        &self,
        call: &mut dyn Call_ClearCache,
        scope: Option<Vec<String>>,
    ) -> varlink::Result<()> {
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

        let now = now_secs();
        let (quick_secs, slow_secs) = {
            let c = self.config.read().unwrap();
            (
                c.refresh.quick.window().as_secs(),
                c.refresh.slow.window().as_secs(),
            )
        };
        let quick_start = now.saturating_sub(quick_secs);
        let slow_start = now.saturating_sub(slow_secs);

        if all {
            if let Err(e) = self.history.clear() {
                warn!("history clear failed: {e}");
            } else {
                info!("history cleared");
            }
        } else {
            if want("quick") {
                clear_band(&self.history, quick_start, u64::MAX, "quick");
            }
            if want("slow") {
                clear_band(&self.history, slow_start, quick_start, "slow");
            }
            if want("stale") {
                clear_band(&self.history, 0, slow_start, "stale");
            }
        }

        if let Ok(gitlab) = self.gitlab().await {
            if all {
                self.warm_up().await;
            } else if want("stale") {
                let retention = self.config.read().unwrap().history.retention();
                self.refresh_history_window(&gitlab, retention).await;
                self.prune_history();
            } else if want("slow") {
                let slow_window = self.config.read().unwrap().refresh.slow.window();
                self.refresh_history_window(&gitlab, slow_window).await;
            } else if want("quick") {
                let quick_window = self.config.read().unwrap().refresh.quick.window();
                self.refresh_history_window(&gitlab, quick_window).await;
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
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    issue_iid, "PostTime while unreachable, queuing for retry"
                );
                self.defer_post_time(project_id, issue_iid, duration, summary)
                    .await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab
            .add_spent_time(project_id, issue_iid, &duration, summary.as_deref())
            .await
        {
            Ok(()) => {
                info!(project_id, issue_iid, duration, "posted time");
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, issue_iid, "PostTime network error, queuing for retry");
                self.defer_post_time(project_id, issue_iid, duration, summary)
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
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    issue_iid, "CloseIssue while unreachable, queuing for retry"
                );
                self.defer_close_issue(project_id, issue_iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.close_issue(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "closed issue");
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, issue_iid, "CloseIssue network error, queuing for retry");
                self.defer_close_issue(project_id, issue_iid).await;
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
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    issue_iid, "AssignSelf while unreachable, queuing for retry"
                );
                self.defer_assign_self(project_id, issue_iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.assign_self(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "assigned self");
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, issue_iid, "AssignSelf network error, queuing for retry");
                self.defer_assign_self(project_id, issue_iid).await;
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
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    issue_iid, "UnassignSelf while unreachable, queuing for retry"
                );
                self.defer_unassign_self(project_id, issue_iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.unassign_self(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "unassigned self");
                self.forget_cached_issue(project_id, issue_iid);
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, issue_iid, "UnassignSelf network error, queuing for retry");
                self.defer_unassign_self(project_id, issue_iid).await;
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
        let client = match GitlabClient::connect_with_retry(&host, &token).await {
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
        *self.session.write().await = ConnState::Connected(session);
        self.queue.drain_waker().notify_one();
        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn logout(&self, call: &mut dyn Call_Logout) -> varlink::Result<()> {
        *self.session.write().await = ConnState::Dormant(DormancyReason::LoggedOut);
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
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                call.reply_not_authenticated(reason, detail)
            }
        }
    }
}

/// Clear one history tier's `spent_at` band, logging the outcome.
fn clear_band(history: &HistoryCache, min_secs: u64, max_secs: u64, tier: &str) {
    match history.clear_between(min_secs, max_secs) {
        Ok(n) => info!(removed = n, tier, "history tier cleared"),
        Err(e) => warn!(error = %e, tier, "history tier clear failed"),
    }
}
