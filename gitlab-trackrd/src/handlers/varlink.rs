//! The [`VarlinkInterface`] method implementations plus the write-path helpers
//! they lean on. Each method is a short cascade: consult the cache, fall back
//! to GitLab, reply — see the crate module docs for the error conventions.

use std::collections::HashMap;

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_AssignSelf, Call_ClearCache, Call_ClearFailures, Call_CloseIssue, Call_DismissFailure,
    Call_GetAssignedIssues, Call_GetFailures, Call_GetHistory, Call_Login, Call_Logout,
    Call_PostTime, Call_RetryFailure, Call_Search, Call_UnassignSelf, Call_WhoAmI, FailedTask,
    Group, HistoryEvent, Issue, MergeRequest, Project, VarlinkInterface,
};

use crate::error::{DormancyReason, Error};
use crate::gitlab::{GitlabClient, Issuable};
use crate::history::HistoryCache;
use crate::search::{SearchIssue, parse_iid_query, text_matches};
use crate::secrets::{self, Credentials};

use super::refresh::graph_status_from;
use super::{
    ConnState, Handlers, Session, dormant_args, issue_ref_error, looks_like_duration, now_secs,
};

/// The kind strings `Search` accepts, matching the `ClearCache` scope style.
const SEARCH_KINDS: [&str; 4] = ["issues", "merge_requests", "projects", "groups"];

/// Per-kind result cap when the caller doesn't pass a `limit`.
const DEFAULT_SEARCH_LIMIT: usize = 50;

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
            .post_time(
                Issuable::Issue,
                project_id,
                issue_iid,
                duration,
                summary,
                issue_id,
            )
            .await;
    }

    /// Queue a `CloseIssue` write for retry and drop the issue from the cache so
    /// `tt list` reflects it at once. Shared by both deferral arms of
    /// [`Self::close_issue`].
    async fn defer_close_issue(&self, project_id: i64, issue_iid: i64) {
        self.queue
            .close(Issuable::Issue, project_id, issue_iid)
            .await;
        self.forget_cached_issue(project_id, issue_iid);
    }

    /// Queue an `AssignSelf` write for retry. Shared by both deferral arms of
    /// [`Self::assign_self`].
    async fn defer_assign_self(&self, project_id: i64, issue_iid: i64) {
        self.queue
            .assign_self(Issuable::Issue, project_id, issue_iid)
            .await;
    }

    /// Queue an `UnassignSelf` write for retry and drop the issue from the cache.
    /// Shared by both deferral arms of [`Self::unassign_self`].
    async fn defer_unassign_self(&self, project_id: i64, issue_iid: i64) {
        self.queue
            .unassign_self(Issuable::Issue, project_id, issue_iid)
            .await;
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

    /// Map a cached search issue onto the wire `Issue`. `graph_status` is
    /// best-effort from already-cached board labels only — `Search` is a pure
    /// cache reader, so projects the assigned-issues refresh never touched
    /// simply get an empty status.
    fn wire_search_issue(&self, i: SearchIssue) -> Issue {
        let board = self.boards.get(i.project_id).ok().flatten();
        let graph_status = graph_status_from(board.as_deref(), &i.labels, &i.state);
        Issue {
            id: i.id,
            iid: i.iid,
            project_id: i.project_id,
            title: i.title,
            web_url: i.web_url,
            state: i.state,
            parent: i.parent,
            total_time: i.total_time,
            graph_status,
        }
    }
}

/// A cache read for one `Search` kind, degraded to empty on failure so the
/// daemon stays available (the standing cache-error convention).
fn read_or_empty<T>(result: crate::error::Result<Vec<T>>, kind: &str) -> Vec<T> {
    result.unwrap_or_else(|e| {
        warn!(error = %e, kind, "search cache read failed, treating as empty");
        Vec::new()
    })
}

/// Whether an issue/MR matches the search: case-insensitive substring on the
/// title or any label, or an exact `#iid` reference query.
fn search_item_matches(
    needle: &str,
    iid_query: Option<i64>,
    title: &str,
    labels: &[String],
    iid: i64,
) -> bool {
    text_matches(needle, title)
        || labels.iter().any(|l| text_matches(needle, l))
        || iid_query == Some(iid)
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
    async fn search(
        &self,
        call: &mut dyn Call_Search,
        query: String,
        kinds: Option<Vec<String>>,
        limit: Option<i64>,
    ) -> varlink::Result<()> {
        let needle = query.trim().to_lowercase();
        if needle.is_empty() {
            return call.reply_gitlab_error("empty search query".to_string());
        }
        let limit = match limit {
            None => DEFAULT_SEARCH_LIMIT,
            Some(n) if n > 0 => n as usize,
            Some(n) => return call.reply_gitlab_error(format!("invalid limit: {n}")),
        };
        let kinds = kinds.unwrap_or_default();
        if let Some(bad) = kinds.iter().find(|k| !SEARCH_KINDS.contains(&k.as_str())) {
            return call.reply_gitlab_error(format!(
                "unknown kind {bad:?} (expected one of: {})",
                SEARCH_KINDS.join(", ")
            ));
        }
        let want = |k: &str| kinds.is_empty() || kinds.iter().any(|x| x == k);

        // Cold cache (never synced): mirror `get_assigned_issues` — an honest
        // NotAuthenticated while dormant, an empty reply while the first sync
        // is still pending.
        let never_synced = match self.search.stamps() {
            Ok(s) => s.last_partial_sync_secs == 0,
            Err(e) => {
                warn!("search stamp read failed, treating as never synced: {e}");
                true
            }
        };
        if never_synced {
            return match self.gitlab().await {
                Ok(_) => call.reply(Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                Err(e) => {
                    let (reason, detail) = dormant_args(&e);
                    call.reply_not_authenticated(reason, detail)
                }
            };
        }

        let iid_query = parse_iid_query(&query);

        let mut issues: Vec<Issue> = Vec::new();
        if want("issues") {
            let mut hits: Vec<SearchIssue> = read_or_empty(self.search.all_issues(), "issues")
                .into_iter()
                .filter(|i| search_item_matches(&needle, iid_query, &i.title, &i.labels, i.iid))
                .collect();
            hits.sort_by_key(|i| std::cmp::Reverse(i.updated_at_secs));
            hits.truncate(limit);
            issues = hits
                .into_iter()
                .map(|i| self.wire_search_issue(i))
                .collect();
        }

        let mut merge_requests: Vec<MergeRequest> = Vec::new();
        if want("merge_requests") {
            let mut hits = read_or_empty(self.search.all_mrs(), "merge requests");
            hits.retain(|m| search_item_matches(&needle, iid_query, &m.title, &m.labels, m.iid));
            hits.sort_by_key(|m| std::cmp::Reverse(m.updated_at_secs));
            hits.truncate(limit);
            merge_requests = hits
                .into_iter()
                .map(|m| MergeRequest {
                    id: m.id,
                    iid: m.iid,
                    project_id: m.project_id,
                    title: m.title,
                    web_url: m.web_url,
                    state: m.state,
                })
                .collect();
        }

        let mut projects: Vec<Project> = Vec::new();
        if want("projects") {
            let mut hits = read_or_empty(self.search.all_projects(), "projects");
            hits.retain(|p| text_matches(&needle, &p.name) || text_matches(&needle, &p.path));
            hits.sort_by(|a, b| a.path.cmp(&b.path));
            hits.truncate(limit);
            projects = hits
                .into_iter()
                .map(|p| Project {
                    id: p.id,
                    name: p.name,
                    path: p.path,
                    web_url: p.web_url,
                })
                .collect();
        }

        let mut groups: Vec<Group> = Vec::new();
        if want("groups") {
            let mut hits = read_or_empty(self.search.all_groups(), "groups");
            hits.retain(|g| text_matches(&needle, &g.name) || text_matches(&needle, &g.path));
            hits.sort_by(|a, b| a.path.cmp(&b.path));
            hits.truncate(limit);
            groups = hits
                .into_iter()
                .map(|g| Group {
                    id: g.id,
                    name: g.name,
                    path: g.path,
                    web_url: g.web_url,
                })
                .collect();
        }

        debug!(
            issues = issues.len(),
            merge_requests = merge_requests.len(),
            projects = projects.len(),
            groups = groups.len(),
            "serving search results from cache"
        );
        call.reply(issues, merge_requests, projects, groups)
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
            // Zeroed so the next quick tick actually refetches — a fresh stamp
            // would serve the just-cleared cache as stale-empty until the
            // interval elapses.
            if let Err(e) = self.refresh_meta.update(|s| s.last_quick_sync_secs = 0) {
                warn!("refresh stamp reset failed: {e}");
            }
        }

        if want("search") {
            // begin_sync waits out an in-flight sync, so its final
            // set_stamps can't stamp "synced" over the half-wiped corpus.
            // The guard must drop before the refill sync below, whose
            // try_begin_sync would otherwise lose and silently skip.
            let guard = self.search.begin_sync().await;
            if let Err(e) = guard.clear() {
                warn!("search cache clear failed: {e}");
            } else {
                info!("search cache cleared; next sync will be full");
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
            if let Err(e) = self.refresh_meta.clear() {
                warn!("refresh stamp clear failed: {e}");
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
            if want("quick") || want("slow") || want("stale") {
                // The refill below repopulates immediately when connected; the
                // zeroed slow stamp covers the dormant case, so the cleared
                // band is refetched at the next opportunity instead of being
                // stamped over as fresh.
                if let Err(e) = self.refresh_meta.update(|s| {
                    s.last_slow_sync_secs = 0;
                    if want("stale") {
                        s.backfilled_retention_hours = 0;
                    }
                }) {
                    warn!("refresh stamp reset failed: {e}");
                }
            }
        }

        if let Ok(gitlab) = self.gitlab().await {
            if all {
                self.warm_up().await;
            } else {
                if want("search") {
                    // The clear above zeroed the stamps, so this runs full.
                    self.sync_search_cache().await;
                }
                if want("stale") {
                    let retention = self.config.read().unwrap().history.retention();
                    let _ = self.refresh_history_window(&gitlab, retention).await;
                    self.prune_history();
                } else if want("slow") {
                    let slow_window = self.config.read().unwrap().refresh.slow.window();
                    let _ = self.refresh_history_window(&gitlab, slow_window).await;
                } else if want("quick") {
                    let quick_window = self.config.read().unwrap().refresh.quick.window();
                    let _ = self.refresh_history_window(&gitlab, quick_window).await;
                }
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
            .add_spent_time(
                Issuable::Issue,
                project_id,
                issue_iid,
                &duration,
                summary.as_deref(),
            )
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
                    let issue = by_key.get(&(p.project_id, p.iid));
                    events.push(HistoryEvent {
                        timestamp: p.queued_at_secs as i64,
                        source: "queued".to_string(),
                        project_id: p.project_id,
                        issue_iid: p.iid,
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
                issue_iid: f.iid,
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
        match gitlab.close(Issuable::Issue, project_id, issue_iid).await {
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
        match gitlab
            .assign_self(Issuable::Issue, project_id, issue_iid)
            .await
        {
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
        match gitlab
            .unassign_self(Issuable::Issue, project_id, issue_iid)
            .await
        {
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
