//! The [`VarlinkInterface`] method implementations plus the write-path helpers
//! they lean on. Each method is a short cascade: consult the cache, fall back
//! to GitLab, reply — see the crate module docs for the error conventions.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_AssignSelf, Call_ClearCache, Call_ClearFailures, Call_Close, Call_DismissFailure,
    Call_GetAssignedIssues, Call_GetAssignedMergeRequests, Call_GetFailures, Call_GetHistory,
    Call_Login, Call_Logout, Call_PostTime, Call_RetryFailure, Call_Search, Call_UnassignSelf,
    Call_WhoAmI, FailedTask, Group, HistoryEvent, IssuableKind, Issue, MergeRequest, Project,
    VarlinkInterface,
};

use crate::cache::{in_group, namespace_of};
use crate::config::SearchPopulation;
use crate::error::{DormancyReason, Error};
use crate::gitlab::{GitlabApi, GitlabClient, Issuable};
use crate::history::HistoryCache;
use crate::search::{
    SEARCH_SCHEMA_VERSION, SearchGroup, SearchIssue, SearchMr, SearchProject, parse_iid_query,
    text_matches,
};
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
    /// The global numeric issuable ID (the one GraphQL embeds in
    /// `gid://gitlab/<Kind>/<id>`) for a cached `(project, iid)`, so a queued
    /// retry can use the GraphQL path. Issues resolve via the assigned-issue
    /// cache, MRs via the search corpus. `None` when not cached — the queue
    /// then falls back to REST without a `spent_at`.
    fn resolve_issuable_id(&self, kind: Issuable, project_id: i64, iid: i64) -> Option<i64> {
        match kind {
            Issuable::Issue => self.cache.issue_id(project_id, iid).ok().flatten(),
            Issuable::MergeRequest => self.search.mr_id(project_id, iid).ok().flatten(),
        }
    }

    /// Queue a `PostTime` write for retry when GitLab can't be reached right now
    /// (a known outage or a transient failure mid-call); the retry queue drains
    /// it on reconnect. Shared by the `Unreachable`-dormancy and transient-error
    /// arms of [`Self::post_time`].
    async fn defer_post_time(
        &self,
        kind: Issuable,
        project_id: i64,
        iid: i64,
        duration: String,
        summary: Option<String>,
    ) {
        let issuable_id = self.resolve_issuable_id(kind, project_id, iid);
        self.queue
            .post_time(kind, project_id, iid, duration, summary, issuable_id)
            .await;
    }

    /// Queue a `Close` write for retry and reflect it in the caches so the
    /// assigned views do at once. Shared by both deferral arms of
    /// [`Self::close`].
    async fn defer_close(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.queue.close(kind, project_id, iid).await;
        self.reflect_close(kind, project_id, iid);
    }

    /// Queue an `AssignSelf` write for retry. Shared by both deferral arms of
    /// [`Self::assign_self`].
    async fn defer_assign_self(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.queue.assign_self(kind, project_id, iid).await;
    }

    /// Queue an `UnassignSelf` write for retry and reflect it in the caches.
    /// Shared by both deferral arms of [`Self::unassign_self`].
    async fn defer_unassign_self(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.queue.unassign_self(kind, project_id, iid).await;
        self.reflect_unassign(kind, project_id, iid);
    }

    /// Reflect a close in the caches immediately: drop the issue from the
    /// assigned cache, or flip the cached MR's state so it leaves the
    /// assigned-MR view. Best-effort — the next refresh/sync reconciles.
    fn reflect_close(&self, kind: Issuable, project_id: i64, iid: i64) {
        match kind {
            Issuable::Issue => self.forget_cached_issue(project_id, iid),
            Issuable::MergeRequest => self.update_cached_mr(project_id, iid, "close", |m| {
                m.state = "closed".to_string();
            }),
        }
    }

    /// Reflect an unassign in the caches immediately: drop the issue, or
    /// remove the synced user from the cached MR's assignees.
    fn reflect_unassign(&self, kind: Issuable, project_id: i64, iid: i64) {
        match kind {
            Issuable::Issue => self.forget_cached_issue(project_id, iid),
            Issuable::MergeRequest => {
                let user = self
                    .search
                    .stamps()
                    .map(|s| s.synced_user_id)
                    .unwrap_or_default();
                if user == 0 {
                    return;
                }
                self.update_cached_mr(project_id, iid, "unassign", move |m| {
                    m.assignees.retain(|a| a.id != user);
                });
            }
        }
    }

    /// Apply a mutation to one cached search MR under the sync gate. Uses
    /// `try_begin_sync` — a write handler must never wait out an in-flight
    /// full resync; when the gate is contended the update is skipped, since
    /// the running sync is fetching fresh data anyway.
    fn update_cached_mr(
        &self,
        project_id: i64,
        iid: i64,
        what: &str,
        f: impl FnOnce(&mut SearchMr),
    ) {
        let Some(guard) = self.search.try_begin_sync() else {
            debug!(
                project_id,
                iid, what, "search sync in flight; skipping MR cache update"
            );
            return;
        };
        match guard.update_mr(project_id, iid, f) {
            Ok(true) => debug!(project_id, iid, what, "cached MR updated"),
            Ok(false) => {}
            Err(e) => warn!(error = %e, project_id, iid, what, "MR cache update failed"),
        }
    }

    /// Drop an issue from the assigned-issues cache so a close/unassign is
    /// reflected in `tt list` immediately. Best-effort — a failure is logged
    /// and swallowed (the next refresh will reconcile the list anyway).
    fn forget_cached_issue(&self, project_id: i64, iid: i64) {
        match self.cache.remove_issue(project_id, iid) {
            Ok(true) => debug!(project_id, iid, "removed issue from cache"),
            Ok(false) => {}
            Err(e) => warn!(error = %e, project_id, iid, "cache issue removal failed"),
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

    /// The pure cache-read half of `Search` — phase 1 of a streamed
    /// (`more: true`) call. Same validation and matching as the full path,
    /// no live phase, so it replies instantly.
    pub(crate) async fn search_cached(
        &self,
        call: &mut dyn Call_Search,
        query: String,
        kinds: Option<Vec<String>>,
        limit: Option<i64>,
    ) -> varlink::Result<()> {
        self.search_impl(call, query, kinds, limit, false).await
    }

    /// Shared implementation of [`VarlinkInterface::search`] (live phase
    /// allowed) and [`Handlers::search_cached`] (cache only): validate,
    /// optionally run the live micro-sync, then read, merge, and wire the
    /// corpus.
    #[instrument(skip(self, call))]
    async fn search_impl(
        &self,
        call: &mut dyn Call_Search,
        query: String,
        kinds: Option<Vec<String>>,
        limit: Option<i64>,
        allow_live: bool,
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

        // The transparent live phase: under the tracked population with a
        // connected session, ask GitLab directly — bounded by
        // `search.live_limit` and `search.live_deadline_ms` — and fold the
        // results into the corpus before reading it.
        let live = if allow_live {
            self.live_search(&query, &needle, &kinds, &want).await
        } else {
            None
        };
        let live_attempted = live.is_some();
        let live = live.unwrap_or_default();

        // Cold cache (never synced) without a live phase: mirror
        // `get_assigned_issues` — an honest NotAuthenticated while dormant,
        // an empty reply while the first sync is still pending. With a live
        // phase the reply below already reflects GitLab, stamps or not.
        if !live_attempted {
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
        }

        let iid_query = parse_iid_query(&query);

        let mut issues: Vec<Issue> = Vec::new();
        if want("issues") {
            let mut hits: Vec<SearchIssue> = read_or_empty(self.search.all_issues(), "issues")
                .into_iter()
                .filter(|i| search_item_matches(&needle, iid_query, &i.title, &i.labels, i.iid))
                .collect();
            merge_live(
                &mut hits,
                &live.issues,
                |i| i.id,
                |i| {
                    iid_query.is_none()
                        || search_item_matches(&needle, iid_query, &i.title, &i.labels, i.iid)
                },
            );
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
            merge_live(
                &mut hits,
                &live.mrs,
                |m| m.id,
                |m| {
                    iid_query.is_none()
                        || search_item_matches(&needle, iid_query, &m.title, &m.labels, m.iid)
                },
            );
            hits.sort_by_key(|m| std::cmp::Reverse(m.updated_at_secs));
            hits.truncate(limit);
            merge_requests = hits.into_iter().map(wire_mr).collect();
        }

        let mut projects: Vec<Project> = Vec::new();
        if want("projects") {
            let mut hits = read_or_empty(self.search.all_projects(), "projects");
            hits.retain(|p| text_matches(&needle, &p.name) || text_matches(&needle, &p.path));
            merge_live(&mut hits, &live.projects, |p| p.id, |_| true);
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
            merge_live(&mut hits, &live.groups, |g| g.id, |_| true);
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
            live = live_attempted,
            "serving search results"
        );
        call.reply(issues, merge_requests, projects, groups)
    }

    /// Run the bounded live micro-sync for one `Search` call, if eligible.
    /// `None` means the live phase was not in play (eager population or
    /// dormant session) and the cold-cache guard applies as it always did;
    /// `Some` means the transparent path handled the call — possibly with
    /// empty hits, since debounced repeats, per-kind failures, and deadline
    /// expiry all degrade to "just the cache". A live problem is never a
    /// session problem: nothing here demotes or touches the sync stamps.
    async fn live_search(
        &self,
        query: &str,
        needle: &str,
        kinds: &[String],
        want: &impl Fn(&str) -> bool,
    ) -> Option<LiveHits> {
        let (tracked_mode, deadline, live_limit, debounce) = {
            let c = self.config.read().unwrap();
            (
                matches!(
                    c.search.population,
                    SearchPopulation::Auto | SearchPopulation::Tracked
                ),
                c.search.live_deadline(),
                c.search.live_limit as usize,
                c.search.live_debounce(),
            )
        };
        if !tracked_mode {
            return None;
        }
        let Ok(gitlab) = self.gitlab().await else {
            return None;
        };

        let mut sorted_kinds: Vec<&str> = kinds.iter().map(String::as_str).collect();
        sorted_kinds.sort_unstable();
        let key = format!("{needle}\x1f{}", sorted_kinds.join(","));
        if self.live_debounced(&key, debounce) {
            debug!("live search debounced; serving cache only");
            return Some(LiveHits::default());
        }

        match tokio::time::timeout(
            deadline,
            self.fetch_live_hits(&gitlab, query, want, live_limit),
        )
        .await
        {
            Ok(hits) => {
                // Only a fully-answered lookup arms the debounce — a failed
                // kind (e.g. a rate limit) should be retried by the very
                // next search, which is a manual, human-paced action.
                if hits.complete {
                    self.record_live_search(key, debounce);
                }
                self.absorb_live_hits(&hits);
                Some(hits)
            }
            Err(_) => {
                debug!("live search deadline expired; serving cache only");
                Some(LiveHits::default())
            }
        }
    }

    /// The per-kind live fetchers, run concurrently under the caller's
    /// deadline. Each kind fails independently — a rejected `/search` must
    /// not blank the kinds that answered — and a failure is a debug log.
    async fn fetch_live_hits(
        &self,
        gitlab: &Arc<dyn GitlabApi>,
        query: &str,
        want: &impl Fn(&str) -> bool,
        limit: usize,
    ) -> LiveHits {
        /// Run one kind's fetcher when wanted; `(hits, ok)`. The future is
        /// built eagerly but never polled when unwanted.
        async fn arm<T>(
            wanted: bool,
            kind: &'static str,
            fut: impl Future<Output = crate::error::Result<Vec<T>>>,
        ) -> (Vec<T>, bool) {
            if !wanted {
                return (Vec::new(), true);
            }
            match fut.await {
                Ok(v) => (v, true),
                Err(e) => {
                    debug!(error = %e, kind, "live search fetch failed; serving cache only");
                    (Vec::new(), false)
                }
            }
        }

        let (issues, mrs, projects, groups) = tokio::join!(
            arm(
                want("issues"),
                "issues",
                gitlab.search_issues_live(query, limit)
            ),
            arm(
                want("merge_requests"),
                "merge requests",
                gitlab.search_mrs_live(query, limit)
            ),
            arm(
                want("projects"),
                "projects",
                gitlab.search_projects_live(query, limit)
            ),
            arm(
                want("groups"),
                "groups",
                gitlab.search_groups_live(query, limit)
            ),
        );
        LiveHits {
            complete: issues.1 && mrs.1 && projects.1 && groups.1,
            issues: issues.0,
            mrs: mrs.0,
            projects: projects.0,
            groups: groups.0,
        }
    }

    /// Whether an identical live lookup completed within the debounce window.
    fn live_debounced(&self, key: &str, window: Duration) -> bool {
        if window.is_zero() {
            return false;
        }
        self.live_search_recent
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|done| done.elapsed() < window)
    }

    /// Arm the debounce for `key`, opportunistically dropping expired
    /// entries so the map doesn't grow with query diversity.
    fn record_live_search(&self, key: String, window: Duration) {
        if window.is_zero() {
            return;
        }
        let mut recent = self.live_search_recent.lock().unwrap();
        recent.retain(|_, done| done.elapsed() < window);
        recent.insert(key, Instant::now());
    }

    /// Fold live hits into the corpus and mark their projects as tracked, so
    /// the background partial sync keeps them fresh from here on. Uses the
    /// non-blocking sync gate: when a background sync holds it, caching is
    /// skipped — the reply still carries the hits (pass-through), and the
    /// next search simply re-fetches. Synchronous on purpose: never holds
    /// the guard across an await.
    fn absorb_live_hits(&self, hits: &LiveHits) {
        if hits.issues.is_empty()
            && hits.mrs.is_empty()
            && hits.projects.is_empty()
            && hits.groups.is_empty()
        {
            return;
        }
        let Some(guard) = self.search.try_begin_sync() else {
            debug!("search sync in flight; serving live hits without caching them");
            return;
        };
        // `/search` issue JSON can omit the epic and time stats; don't let a
        // live hit blank fields a background sync already filled.
        let issues: Vec<SearchIssue> = hits
            .issues
            .iter()
            .map(|hit| {
                let mut hit = hit.clone();
                if (hit.parent.is_empty() || hit.total_time.is_empty())
                    && let Ok(Some(prev)) = self.search.issue_by_id(hit.id)
                {
                    if hit.parent.is_empty() {
                        hit.parent = prev.parent;
                    }
                    if hit.total_time.is_empty() {
                        hit.total_time = prev.total_time;
                    }
                }
                hit
            })
            .collect();
        let stored = guard
            .upsert_issues(&issues)
            .and(guard.upsert_mrs(&hits.mrs))
            .and(guard.upsert_projects(&hits.projects))
            .and(guard.upsert_groups(&hits.groups))
            .and(
                guard.note_tracked(
                    hits.issues
                        .iter()
                        .map(|i| i.project_id)
                        .chain(hits.mrs.iter().map(|m| m.project_id)),
                    now_secs(),
                ),
            );
        if let Err(e) = stored {
            warn!(error = %e, "caching live search hits failed; results still served");
        }
    }
}

/// What one `Search` call's live micro-sync brought back.
#[derive(Default)]
struct LiveHits {
    issues: Vec<SearchIssue>,
    mrs: Vec<SearchMr>,
    projects: Vec<SearchProject>,
    groups: Vec<SearchGroup>,
    /// Every wanted kind answered (no fetch error). Gates the debounce.
    complete: bool,
}

/// A cache read for one `Search` kind, degraded to empty on failure so the
/// daemon stays available (the standing cache-error convention).
fn read_or_empty<T>(result: crate::error::Result<Vec<T>>, kind: &str) -> Vec<T> {
    result.unwrap_or_else(|e| {
        warn!(error = %e, kind, "search cache read failed, treating as empty");
        Vec::new()
    })
}

/// Wire → internal issuable kind. The only place the generated enum's
/// lowercase variants are touched.
fn internal_kind(kind: &IssuableKind) -> Issuable {
    match kind {
        IssuableKind::issue => Issuable::Issue,
        IssuableKind::merge_request => Issuable::MergeRequest,
    }
}

/// Internal → wire issuable kind.
fn wire_kind(kind: Issuable) -> IssuableKind {
    match kind {
        Issuable::Issue => IssuableKind::issue,
        Issuable::MergeRequest => IssuableKind::merge_request,
    }
}

/// Map a cached search MR onto the wire `MergeRequest`; assignee usernames
/// come from the pairs captured at sync time.
fn wire_mr(m: SearchMr) -> MergeRequest {
    MergeRequest {
        id: m.id,
        iid: m.iid,
        project_id: m.project_id,
        title: m.title,
        web_url: m.web_url,
        state: m.state,
        assignees: m.assignees.into_iter().map(|a| a.username).collect(),
    }
}

/// Fold live hits into the locally-matched list: dedupe by global id (a hit
/// the absorb step already cached shows up in the local read too), and pass
/// through hits the local matcher would reject — the server side also
/// matches descriptions, which the corpus doesn't store. `keep` narrows the
/// pass-through (reference queries stay exact).
fn merge_live<T: Clone>(
    local: &mut Vec<T>,
    live: &[T],
    id_of: impl Fn(&T) -> i64,
    keep: impl Fn(&T) -> bool,
) {
    let seen: HashSet<i64> = local.iter().map(&id_of).collect();
    local.extend(
        live.iter()
            .filter(|t| !seen.contains(&id_of(t)) && keep(t))
            .cloned(),
    );
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
    async fn get_assigned_merge_requests(
        &self,
        call: &mut dyn Call_GetAssignedMergeRequests,
        groups: Option<Vec<String>>,
    ) -> varlink::Result<()> {
        // Cold cache — never synced, or synced under a pre-assignee schema
        // (synced_user_id 0 covers both): mirror `get_assigned_issues` — an
        // honest NotAuthenticated while dormant, an empty reply while the
        // first (re)sync is pending.
        let stamps = self.search.stamps().unwrap_or_else(|e| {
            warn!("search stamp read failed, treating as never synced: {e}");
            Default::default()
        });
        if stamps.last_partial_sync_secs == 0
            || stamps.schema_version < SEARCH_SCHEMA_VERSION
            || stamps.synced_user_id == 0
        {
            return match self.gitlab().await {
                Ok(_) => call.reply(Vec::new()),
                Err(e) => {
                    let (reason, detail) = dormant_args(&e);
                    call.reply_not_authenticated(reason, detail)
                }
            };
        }

        let mut mine: Vec<SearchMr> = read_or_empty(self.search.all_mrs(), "merge requests")
            .into_iter()
            .filter(|m| {
                m.state == "opened" && m.assignees.iter().any(|a| a.id == stamps.synced_user_id)
            })
            .collect();
        if let Some(groups) = groups
            && !groups.is_empty()
        {
            mine.retain(|m| {
                let ns = namespace_of(&m.web_url);
                groups.iter().any(|g| in_group(&ns, g))
            });
        }
        // The wire type carries no timestamp, so order for the picker here.
        mine.sort_by_key(|m| std::cmp::Reverse(m.updated_at_secs));

        debug!(count = mine.len(), "serving assigned MRs from search cache");
        call.reply(mine.into_iter().map(wire_mr).collect())
    }

    #[instrument(skip(self, call))]
    async fn search(
        &self,
        call: &mut dyn Call_Search,
        query: String,
        kinds: Option<Vec<String>>,
        limit: Option<i64>,
    ) -> varlink::Result<()> {
        self.search_impl(call, query, kinds, limit, true).await
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
        iid: i64,
        kind: IssuableKind,
        duration: String,
        summary: Option<String>,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, iid) {
            return call.reply_gitlab_error(msg);
        }
        if !looks_like_duration(&duration) {
            return call.reply_gitlab_error(format!("invalid duration: {duration:?}"));
        }
        let kind = internal_kind(&kind);
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    iid,
                    kind = ?kind,
                    "PostTime while unreachable, queuing for retry"
                );
                self.defer_post_time(kind, project_id, iid, duration, summary)
                    .await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab
            .add_spent_time(kind, project_id, iid, &duration, summary.as_deref())
            .await
        {
            Ok(()) => {
                info!(project_id, iid, kind = ?kind, duration, "posted time");
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, iid, "PostTime network error, queuing for retry");
                self.defer_post_time(kind, project_id, iid, duration, summary)
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
                // Title/url joins for queued MR entries come from the search
                // corpus; only scanned when an MR is actually pending.
                let mrs = if pending.iter().any(|p| p.kind == Issuable::MergeRequest) {
                    read_or_empty(self.search.all_mrs(), "merge requests")
                } else {
                    Vec::new()
                };
                let mr_by_key: HashMap<(i64, i64), &SearchMr> =
                    mrs.iter().map(|m| ((m.project_id, m.iid), m)).collect();

                for p in pending {
                    let (title, web_url) = match p.kind {
                        Issuable::Issue => {
                            let issue = by_key.get(&(p.project_id, p.iid));
                            (
                                issue.map(|i| i.title.clone()).unwrap_or_default(),
                                issue.map(|i| i.web_url.clone()).unwrap_or_default(),
                            )
                        }
                        Issuable::MergeRequest => {
                            let mr = mr_by_key.get(&(p.project_id, p.iid));
                            (
                                mr.map(|m| m.title.clone()).unwrap_or_default(),
                                mr.map(|m| m.web_url.clone()).unwrap_or_default(),
                            )
                        }
                    };
                    events.push(HistoryEvent {
                        timestamp: p.queued_at_secs as i64,
                        source: "queued".to_string(),
                        kind: wire_kind(p.kind),
                        project_id: p.project_id,
                        iid: p.iid,
                        title,
                        web_url,
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
                        kind: wire_kind(e.kind),
                        project_id: e.project_id,
                        iid: e.iid,
                        title: e.title,
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
                kind: wire_kind(f.kind),
                project_id: f.project_id,
                iid: f.iid,
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
    async fn close(
        &self,
        call: &mut dyn Call_Close,
        project_id: i64,
        iid: i64,
        kind: IssuableKind,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, iid) {
            return call.reply_gitlab_error(msg);
        }
        let kind = internal_kind(&kind);
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    iid,
                    kind = ?kind,
                    "Close while unreachable, queuing for retry"
                );
                self.defer_close(kind, project_id, iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.close(kind, project_id, iid).await {
            Ok(()) => {
                info!(project_id, iid, kind = ?kind, "closed issuable");
                self.reflect_close(kind, project_id, iid);
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, iid, "Close network error, queuing for retry");
                self.defer_close(kind, project_id, iid).await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "Close rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn assign_self(
        &self,
        call: &mut dyn Call_AssignSelf,
        project_id: i64,
        iid: i64,
        kind: IssuableKind,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, iid) {
            return call.reply_gitlab_error(msg);
        }
        let kind = internal_kind(&kind);
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    iid,
                    kind = ?kind,
                    "AssignSelf while unreachable, queuing for retry"
                );
                self.defer_assign_self(kind, project_id, iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.assign_self(kind, project_id, iid).await {
            Ok(()) => {
                info!(project_id, iid, kind = ?kind, "assigned self");
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, iid, "AssignSelf network error, queuing for retry");
                self.defer_assign_self(kind, project_id, iid).await;
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
        iid: i64,
        kind: IssuableKind,
    ) -> varlink::Result<()> {
        if let Some(msg) = issue_ref_error(project_id, iid) {
            return call.reply_gitlab_error(msg);
        }
        let kind = internal_kind(&kind);
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(DormancyReason::Unreachable { .. }) => {
                info!(
                    project_id,
                    iid,
                    kind = ?kind,
                    "UnassignSelf while unreachable, queuing for retry"
                );
                self.defer_unassign_self(kind, project_id, iid).await;
                return call.reply();
            }
            Err(e) => {
                let (reason, detail) = dormant_args(&e);
                return call.reply_not_authenticated(reason, detail);
            }
        };
        match gitlab.unassign_self(kind, project_id, iid).await {
            Ok(()) => {
                info!(project_id, iid, kind = ?kind, "unassigned self");
                self.reflect_unassign(kind, project_id, iid);
                call.reply()
            }
            Err(err @ Error::Transient(_)) => {
                warn!(error = %err, project_id, iid, "UnassignSelf network error, queuing for retry");
                self.defer_unassign_self(kind, project_id, iid).await;
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
