use super::refresh::{enrich_graph_status, enrich_timelog};
use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, RwLock};

use gitlab_trackr_api::{
    Call_ClearCache, Call_Close, Call_GetAssignedIssues, Call_GetAssignedMergeRequests,
    Call_GetHistory, Call_PostTime, Call_UnassignSelf, IssuableKind, Issue, VarlinkInterface,
};

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::config::SharedConfig;
use crate::error::{DormancyReason, Result as TrackrResult};
use crate::gitlab::{FetchedTimelog, GitlabApi, Issuable, IssueWithLabels};
use crate::history::{HistoryCache, StoredTimelog};
use crate::queue::RetryQueue;
use crate::search::{SearchGroup, SearchIssue, SearchMr, SearchProject};

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
        kind: Issuable::Issue,
        project_id: 0,
        iid: 42,
        title: "fresh".to_string(),
        web_url: "https://gl/-/issues/42".to_string(),
        duration: "1h".to_string(),
        summary: "s".to_string(),
    };
    let r = enrich_timelog(t, &by_url, &by_iid, &HashMap::new());
    assert_eq!(r.project_id, 7);
    assert_eq!(r.title, "fresh", "fresh title preserved");
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
        kind: Issuable::Issue,
        project_id: 0,
        iid: 42,
        title: "fresh".to_string(),
        web_url: String::new(),
        duration: "1h".to_string(),
        summary: String::new(),
    };
    let r = enrich_timelog(t, &by_url, &by_iid, &HashMap::new());
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
        kind: Issuable::Issue,
        project_id: 0,
        iid: 99,
        title: "fresh".to_string(),
        web_url: "https://gl/-/issues/99".to_string(),
        duration: "30m".to_string(),
        summary: String::new(),
    };
    let r = enrich_timelog(t, &by_url, &by_iid, &HashMap::new());
    assert_eq!(r.project_id, 0);
    assert_eq!(r.title, "fresh");
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
        kind: Issuable::Issue,
        project_id: 0,
        iid: 42,
        title: String::new(),
        web_url: "https://gl/-/issues/42".to_string(),
        duration: "1h".to_string(),
        summary: String::new(),
    };
    let r = enrich_timelog(t, &by_url, &by_iid, &HashMap::new());
    assert_eq!(r.title, "From cache");
}

#[test]
fn enrich_timelog_mr_falls_back_to_search_corpus() {
    let m = search_mr(3, "MR title"); // project_id 1, web_url …/merge_requests/30
    let mr_by_url = HashMap::from([(m.web_url.as_str(), &m)]);

    let t = FetchedTimelog {
        timelog_id: 1,
        spent_at_secs: 100,
        kind: Issuable::MergeRequest,
        project_id: 0, // GraphQL gave no project → fall back to the corpus
        iid: 30,
        title: String::new(),
        web_url: m.web_url.clone(),
        duration: "1h".to_string(),
        summary: String::new(),
    };
    let r = enrich_timelog(t, &HashMap::new(), &HashMap::new(), &mr_by_url);
    assert_eq!(r.kind, Issuable::MergeRequest);
    assert_eq!(r.project_id, 1, "project filled from the search corpus");
    assert_eq!(r.title, "MR title", "empty title filled from the corpus");

    // GraphQL-provided project id always wins.
    let t = FetchedTimelog {
        timelog_id: 2,
        spent_at_secs: 100,
        kind: Issuable::MergeRequest,
        project_id: 9,
        iid: 30,
        title: "fresh".to_string(),
        web_url: "https://gl/unknown".to_string(),
        duration: "1h".to_string(),
        summary: String::new(),
    };
    let r = enrich_timelog(t, &HashMap::new(), &HashMap::new(), &mr_by_url);
    assert_eq!(r.project_id, 9);
    assert_eq!(r.title, "fresh");
}

// ── enrich_graph_status with FakeGitlab ────────────────────────────────

/// How a `FakeGitlab` fails its issue / timelog fetches, so the background
/// refresh tests can drive the runtime-demotion logic.
#[derive(Debug, Clone, Copy)]
enum FetchErr {
    /// Network failure → `Error::Transient` (should demote to `Unreachable`).
    Transient,
    /// GitLab-side failure → `Error::Gitlab` (must NOT demote).
    Permanent,
}

/// Minimal `GitlabApi` impl that returns pre-canned `fetch_board_list_labels`
/// responses and counts how many times each method was called.
#[derive(Default)]
struct FakeGitlab {
    board_labels: Mutex<HashMap<i64, TrackrResult<Vec<String>>>>,
    board_calls: AtomicUsize,
    /// Canned results for `fetch_assigned_issues` / `fetch_my_timelogs`, so
    /// the stamp-gating tests can drive a successful refresh. `None` keeps
    /// the method `unimplemented!()` for tests that must never hit it.
    assigned: Mutex<Option<Vec<IssueWithLabels>>>,
    timelogs: Mutex<Option<Vec<FetchedTimelog>>>,
    assigned_calls: AtomicUsize,
    timelog_calls: AtomicUsize,
    /// When set, `fetch_assigned_issues` / `fetch_my_timelogs` and the search
    /// fetches fail this way.
    fetch_err: Option<FetchErr>,
    /// When set, `add_spent_time` fails this way (drives the write-handler
    /// deferral path).
    write_err: Option<FetchErr>,
    /// When set, only the *global* (`project = None`) search issue/MR fetches
    /// fail this way — per-project fetches still serve the canned data.
    /// Drives the explicit-`all` rejection path.
    global_search_err: Option<FetchErr>,
    /// Canned results for the search-sync fetches.
    search_issues: Mutex<Vec<SearchIssue>>,
    search_mrs: Mutex<Vec<SearchMr>>,
    search_projects: Mutex<Vec<SearchProject>>,
    search_groups: Mutex<Vec<SearchGroup>>,
    /// Per-call argument log of `fetch_issues_for_search` /
    /// `fetch_merge_requests_for_search`, so sync tests can assert the
    /// population mode (project) and the incremental cursor (updated_after).
    search_issue_calls: Mutex<Vec<(Option<i64>, Option<chrono::DateTime<chrono::Utc>>)>>,
    search_mr_calls: Mutex<Vec<(Option<i64>, Option<chrono::DateTime<chrono::Utc>>)>>,
    project_list_calls: AtomicUsize,
    group_list_calls: AtomicUsize,
    /// Canned result for `fetch_assigned_merge_requests` (default: none
    /// assigned, so sync tests that don't care get an empty fetch).
    assigned_mrs: Mutex<Vec<SearchMr>>,
    assigned_mr_calls: AtomicUsize,
    /// When set, only `fetch_assigned_merge_requests` fails this way — the
    /// other search fetches still serve their canned data.
    assigned_mr_err: Option<FetchErr>,
    /// When set, per-project search issue/MR fetches for this project fail
    /// this way; other projects still serve the canned data. Drives the
    /// tracked sync's skip-inaccessible-project path.
    project_search_err: Option<(i64, FetchErr)>,
    /// Canned results for the live search fetchers.
    live_issues: Mutex<Vec<SearchIssue>>,
    live_mrs: Mutex<Vec<SearchMr>>,
    live_projects: Mutex<Vec<SearchProject>>,
    live_groups: Mutex<Vec<SearchGroup>>,
    live_issue_calls: AtomicUsize,
    live_mr_calls: AtomicUsize,
    live_project_calls: AtomicUsize,
    live_group_calls: AtomicUsize,
    /// When set, every live search fetcher fails this way.
    live_err: Option<FetchErr>,
    /// When set, every live search fetcher sleeps this long before returning
    /// — how the deadline-fallback test fakes a slow instance within the
    /// no-paused-clock timing rules.
    live_delay: Option<Duration>,
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

    /// A fake whose issue and timelog fetches fail in the given way.
    fn failing(err: FetchErr) -> Self {
        Self {
            fetch_err: Some(err),
            ..Self::default()
        }
    }

    /// A fake whose `add_spent_time` write fails in the given way.
    fn failing_write(err: FetchErr) -> Self {
        Self {
            write_err: Some(err),
            ..Self::default()
        }
    }

    fn board_calls(&self) -> usize {
        self.board_calls.load(Ordering::SeqCst)
    }

    /// Total live search fetcher invocations across all four kinds.
    fn live_calls(&self) -> usize {
        self.live_issue_calls.load(Ordering::SeqCst)
            + self.live_mr_calls.load(Ordering::SeqCst)
            + self.live_project_calls.load(Ordering::SeqCst)
            + self.live_group_calls.load(Ordering::SeqCst)
    }

    /// Shared prologue of the live fetchers: count, fake slowness, fail.
    async fn live_gate<T>(&self, counter: &AtomicUsize) -> Option<TrackrResult<Vec<T>>> {
        counter.fetch_add(1, Ordering::SeqCst);
        if let Some(d) = self.live_delay {
            tokio::time::sleep(d).await;
        }
        self.live_err.map(err_result)
    }

    fn fetch_result<T>(&self) -> TrackrResult<Vec<T>> {
        match self.fetch_err {
            Some(err) => err_result(err),
            None => unimplemented!("fetch not configured for this fake"),
        }
    }

    fn write_result(&self) -> TrackrResult<()> {
        match self.write_err {
            Some(FetchErr::Transient) => Err(crate::error::Error::Transient("offline".into())),
            Some(FetchErr::Permanent) => Err(crate::error::Error::Gitlab("400 Bad Request".into())),
            None => Ok(()),
        }
    }
}

fn err_result<T>(err: FetchErr) -> TrackrResult<Vec<T>> {
    match err {
        FetchErr::Transient => Err(crate::error::Error::Transient("offline".into())),
        FetchErr::Permanent => Err(crate::error::Error::Gitlab("500 Server Error".into())),
    }
}

#[async_trait::async_trait]
impl GitlabApi for FakeGitlab {
    async fn fetch_assigned_issues(
        &self,
        _group: Option<String>,
    ) -> TrackrResult<Vec<IssueWithLabels>> {
        self.assigned_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        match &*self.assigned.lock().unwrap() {
            Some(v) => Ok(v.clone()),
            None => self.fetch_result(),
        }
    }
    async fn add_spent_time(
        &self,
        _kind: Issuable,
        _project_id: i64,
        _iid: i64,
        _duration: &str,
        _summary: Option<&str>,
    ) -> TrackrResult<()> {
        self.write_result()
    }
    async fn create_timelog(
        &self,
        _kind: Issuable,
        _issuable_id: i64,
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
        self.timelog_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        match &*self.timelogs.lock().unwrap() {
            Some(v) => Ok(v.clone()),
            None => self.fetch_result(),
        }
    }
    async fn close(&self, _kind: Issuable, _project_id: i64, _iid: i64) -> TrackrResult<()> {
        unimplemented!()
    }
    async fn assign_self(&self, _kind: Issuable, _project_id: i64, _iid: i64) -> TrackrResult<()> {
        unimplemented!()
    }
    async fn unassign_self(
        &self,
        _kind: Issuable,
        _project_id: i64,
        _iid: i64,
    ) -> TrackrResult<()> {
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
    async fn fetch_issues_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> TrackrResult<Vec<SearchIssue>> {
        self.search_issue_calls
            .lock()
            .unwrap()
            .push((project, updated_after));
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        if project.is_none()
            && let Some(err) = self.global_search_err
        {
            return err_result(err);
        }
        if let Some((pid, err)) = self.project_search_err
            && project == Some(pid)
        {
            return err_result(err);
        }
        Ok(self.search_issues.lock().unwrap().clone())
    }
    async fn fetch_merge_requests_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> TrackrResult<Vec<SearchMr>> {
        self.search_mr_calls
            .lock()
            .unwrap()
            .push((project, updated_after));
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        if project.is_none()
            && let Some(err) = self.global_search_err
        {
            return err_result(err);
        }
        if let Some((pid, err)) = self.project_search_err
            && project == Some(pid)
        {
            return err_result(err);
        }
        Ok(self.search_mrs.lock().unwrap().clone())
    }
    async fn fetch_assigned_merge_requests(&self) -> TrackrResult<Vec<SearchMr>> {
        self.assigned_mr_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        if let Some(err) = self.assigned_mr_err {
            return err_result(err);
        }
        Ok(self.assigned_mrs.lock().unwrap().clone())
    }
    async fn fetch_member_projects(&self) -> TrackrResult<Vec<SearchProject>> {
        self.project_list_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        Ok(self.search_projects.lock().unwrap().clone())
    }
    async fn fetch_member_groups(&self) -> TrackrResult<Vec<SearchGroup>> {
        self.group_list_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        Ok(self.search_groups.lock().unwrap().clone())
    }
    async fn search_issues_live(
        &self,
        _query: &str,
        _limit: usize,
    ) -> TrackrResult<Vec<SearchIssue>> {
        if let Some(err) = self.live_gate(&self.live_issue_calls).await {
            return err;
        }
        Ok(self.live_issues.lock().unwrap().clone())
    }
    async fn search_mrs_live(&self, _query: &str, _limit: usize) -> TrackrResult<Vec<SearchMr>> {
        if let Some(err) = self.live_gate(&self.live_mr_calls).await {
            return err;
        }
        Ok(self.live_mrs.lock().unwrap().clone())
    }
    async fn search_projects_live(
        &self,
        _query: &str,
        _limit: usize,
    ) -> TrackrResult<Vec<SearchProject>> {
        if let Some(err) = self.live_gate(&self.live_project_calls).await {
            return err;
        }
        Ok(self.live_projects.lock().unwrap().clone())
    }
    async fn search_groups_live(
        &self,
        _query: &str,
        _limit: usize,
    ) -> TrackrResult<Vec<SearchGroup>> {
        if let Some(err) = self.live_gate(&self.live_group_calls).await {
            return err;
        }
        Ok(self.live_groups.lock().unwrap().clone())
    }
}

fn boards_cache() -> (BoardCache, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::Database::builder(dir.path().join("db"))
        .open()
        .unwrap();
    (BoardCache::open(&db).unwrap(), dir)
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

    let out = enrich_graph_status(&gitlab, &boards, vec![iwl(7, "opened", &["bug", "high"])]).await;

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

/// Build `Handlers` around a given connection state and a fresh temp database.
fn handlers_with(state: ConnState) -> (Handlers, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::Database::builder(dir.path().join("db"))
        .open()
        .unwrap();
    let session: SessionSlot = Arc::new(RwLock::new(state));
    let cache = Arc::new(IssueCache::open(&db).unwrap());
    let boards = Arc::new(BoardCache::open(&db).unwrap());
    let history = Arc::new(HistoryCache::open(&db).unwrap());
    let search = Arc::new(crate::search::SearchCache::open(&db).unwrap());
    let refresh_meta = Arc::new(crate::refresh_meta::RefreshMeta::open(&db).unwrap());
    let config: SharedConfig = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
    let queue = RetryQueue::new(Arc::clone(&session), &db, Arc::clone(&config)).unwrap();
    (
        Handlers {
            session,
            cache,
            boards,
            history,
            search,
            refresh_meta,
            queue,
            config,
            reconnect_signal: Arc::new(Notify::new()),
            live_search_recent: std::sync::Mutex::new(std::collections::HashMap::new()),
        },
        dir,
    )
}

/// Build `Handlers` with a dormant (no-GitLab) session, so `clear_cache`
/// clears without the follow-up re-fetch — letting us assert the bands.
/// `pub(crate)` so the `service.rs` dispatch smoke test can borrow it.
pub(crate) fn dormant_handlers() -> (Handlers, tempfile::TempDir) {
    handlers_with(ConnState::Dormant(DormancyReason::NoCredentials))
}

/// Build `Handlers` connected to a `FakeGitlab`, so the background refresh
/// path runs against a controllable client.
fn connected_handlers(fake: FakeGitlab) -> (Handlers, tempfile::TempDir) {
    handlers_with(ConnState::Connected(Session {
        gitlab: Arc::new(fake),
        host: "gitlab.example.com".into(),
        user_id: 1,
    }))
}

fn stored(timelog_id: u64, spent_at_secs: u64) -> StoredTimelog {
    StoredTimelog {
        timelog_id,
        spent_at_secs,
        kind: Issuable::Issue,
        project_id: 1,
        iid: 1,
        title: "t".into(),
        web_url: "u".into(),
        duration: "1h".into(),
        summary: String::new(),
    }
}

#[tokio::test]
async fn clear_cache_quick_scope_only_clears_last_24h() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    let now = now_secs();
    h.history
        .upsert(&[
            stored(1, now - 3_600),       // within 24h → quick
            stored(2, now - 3 * 86_400),  // 3 days → slow
            stored(3, now - 45 * 86_400), // 45 days → stale
        ])
        .unwrap();

    let mut call = AsyncCall::default();
    h.clear_cache(
        &mut call as &mut dyn Call_ClearCache,
        Some(vec!["quick".to_string()]),
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
    assert_eq!(
        remaining,
        vec![2, 3],
        "only the quick-band entry is removed"
    );
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
async fn close_rejects_bad_issuable_ref() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    let mut call = AsyncCall::default();
    h.close(&mut call as &mut dyn Call_Close, 0, 42, IssuableKind::issue)
        .await
        .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert!(reply.error.is_some(), "project_id 0 → error reply");
}

// ── runtime disconnect detection & recovery ────────────────────────────

#[tokio::test]
async fn refresh_cache_demotes_to_unreachable_on_transient_error() {
    let (h, _dir) = connected_handlers(FakeGitlab::failing(FetchErr::Transient));

    h.refresh_cache().await;

    assert!(
        matches!(
            &*h.session.read().await,
            ConnState::Dormant(DormancyReason::Unreachable { .. })
        ),
        "a transient background failure demotes the live session"
    );
    // A permit was left for the reconnect supervisor.
    tokio::time::timeout(Duration::from_millis(200), h.reconnect_signal.notified())
        .await
        .expect("the reconnect supervisor was signalled");
}

#[tokio::test]
async fn refresh_cache_stays_connected_on_permanent_error() {
    let (h, _dir) = connected_handlers(FakeGitlab::failing(FetchErr::Permanent));

    h.refresh_cache().await;

    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a non-transient error must not demote (it is not auto-retryable)"
    );
}

#[tokio::test]
async fn post_time_queues_through_an_unreachable_outage() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    let mut call = AsyncCall::default();

    h.post_time(
        &mut call as &mut dyn Call_PostTime,
        7,
        42,
        IssuableKind::issue,
        "30m".to_string(),
        None,
    )
    .await
    .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_none(),
        "unreachable → queued and reported success, not rejected"
    );
    assert_eq!(
        h.queue.pending_post_time().unwrap().len(),
        1,
        "the write is queued to drain on reconnect"
    );
}

#[tokio::test]
async fn post_time_rejects_when_dormant_but_not_unreachable() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers(); // NoCredentials
    let mut call = AsyncCall::default();

    h.post_time(
        &mut call as &mut dyn Call_PostTime,
        7,
        42,
        IssuableKind::issue,
        "30m".to_string(),
        None,
    )
    .await
    .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_some(),
        "no credentials → reject; queuing wouldn't help"
    );
    assert!(
        h.queue.pending_post_time().unwrap().is_empty(),
        "nothing queued for a non-unreachable dormancy"
    );
}

#[tokio::test]
async fn post_time_transient_queues_without_demoting() {
    use gitlab_trackr_api::AsyncCall;
    // Connected session; the write hits a single transient error. The write
    // must be queued (data-safe, the queue drains it) but the session must
    // NOT be demoted — one blip is not a disconnect. The periodic background
    // refresh (which retries internally) stays the demotion authority.
    let (h, _dir) = connected_handlers(FakeGitlab::failing_write(FetchErr::Transient));
    let mut call = AsyncCall::default();

    h.post_time(
        &mut call as &mut dyn Call_PostTime,
        7,
        42,
        IssuableKind::issue,
        "30m".to_string(),
        None,
    )
    .await
    .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_none(),
        "transient write → queued and reported success"
    );
    assert_eq!(
        h.queue.pending_post_time().unwrap().len(),
        1,
        "the write is queued to drain on reconnect"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a single transient write blip must not demote the live session"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), h.reconnect_signal.notified())
            .await
            .is_err(),
        "no reconnect signal from a write blip"
    );
}

// ── get_assigned_issues: pure cache reader ─────────────────────────────

/// Issues from a successful `GetAssignedIssues` reply.
fn reply_issues(call: &mut gitlab_trackr_api::AsyncCall) -> Vec<Issue> {
    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_none(),
        "expected success, got {:?}",
        reply.error
    );
    let params: gitlab_trackr_api::GetAssignedIssues_Reply =
        serde_json::from_value(reply.parameters.expect("parameters")).expect("parse reply");
    params.issues
}

/// Warm the cache with three issues: two under `team` (one in a subgroup)
/// and one under `other`.
fn seed_grouped_cache(h: &Handlers) {
    h.cache
        .put(&[
            issue(1, 1, "api", "https://gl/team/api/-/issues/1"),
            issue(1, 2, "web", "https://gl/team/sub/web/-/issues/2"),
            issue(2, 3, "other", "https://gl/other/x/-/issues/3"),
        ])
        .unwrap();
}

#[tokio::test]
async fn get_assigned_issues_serves_all_from_cache_while_dormant() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_grouped_cache(&h);

    let mut call = AsyncCall::default();
    h.get_assigned_issues(&mut call as &mut dyn Call_GetAssignedIssues, None)
        .await
        .unwrap();

    // Served under a dormant session: proves it never fetched — a fetch would
    // have replied NotAuthenticated instead of the cached list.
    let mut iids: Vec<i64> = reply_issues(&mut call).iter().map(|i| i.iid).collect();
    iids.sort_unstable();
    assert_eq!(iids, vec![1, 2, 3]);
}

#[tokio::test]
async fn get_assigned_issues_filters_by_group_from_cache() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_grouped_cache(&h);

    let mut call = AsyncCall::default();
    h.get_assigned_issues(
        &mut call as &mut dyn Call_GetAssignedIssues,
        Some(vec!["team".to_string()]),
    )
    .await
    .unwrap();

    let mut iids: Vec<i64> = reply_issues(&mut call).iter().map(|i| i.iid).collect();
    iids.sort_unstable();
    assert_eq!(
        iids,
        vec![1, 2],
        "team includes its subgroup, excludes other"
    );
}

#[tokio::test]
async fn get_assigned_issues_dedups_overlapping_groups() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_grouped_cache(&h);

    let mut call = AsyncCall::default();
    h.get_assigned_issues(
        &mut call as &mut dyn Call_GetAssignedIssues,
        Some(vec!["team".to_string(), "team/sub".to_string()]),
    )
    .await
    .unwrap();

    let mut iids: Vec<i64> = reply_issues(&mut call).iter().map(|i| i.iid).collect();
    iids.sort_unstable();
    assert_eq!(iids, vec![1, 2], "issue 2 (in both) not double-counted");
}

#[tokio::test]
async fn get_assigned_issues_empty_cache_dormant_is_not_authenticated() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();

    let mut call = AsyncCall::default();
    h.get_assigned_issues(&mut call as &mut dyn Call_GetAssignedIssues, None)
        .await
        .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert_eq!(
        reply.error.as_deref(),
        Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
        "cold cache + no session → honest auth error"
    );
}

// ── search sync engine ─────────────────────────────────────────────────

use crate::config::SearchPopulation;
use crate::search::{SEARCH_SCHEMA_VERSION, SyncStamps};

fn search_issue(id: i64, title: &str) -> SearchIssue {
    SearchIssue {
        id,
        iid: id * 10,
        project_id: 1,
        title: title.to_string(),
        web_url: format!("https://gl/team/api/-/issues/{}", id * 10),
        state: "opened".to_string(),
        labels: vec![],
        parent: String::new(),
        total_time: String::new(),
        updated_at_secs: 100,
    }
}

fn search_mr(id: i64, title: &str) -> SearchMr {
    SearchMr {
        id,
        iid: id * 10,
        project_id: 1,
        title: title.to_string(),
        web_url: format!("https://gl/team/api/-/merge_requests/{}", id * 10),
        state: "opened".to_string(),
        labels: vec![],
        assignees: vec![],
        updated_at_secs: 100,
    }
}

fn search_project(id: i64, path: &str) -> SearchProject {
    SearchProject {
        id,
        name: path.rsplit('/').next().unwrap().to_string(),
        path: path.to_string(),
        web_url: format!("https://gl/{path}"),
    }
}

fn search_group(id: i64, path: &str) -> SearchGroup {
    SearchGroup {
        id,
        name: path.to_string(),
        path: path.to_string(),
        web_url: format!("https://gl/{path}"),
    }
}

/// A `FakeGitlab` with one canned entry per search kind.
fn canned_search_fake() -> FakeGitlab {
    let fake = FakeGitlab::default();
    *fake.search_issues.lock().unwrap() = vec![search_issue(1, "canned issue")];
    *fake.search_mrs.lock().unwrap() = vec![search_mr(1, "canned mr")];
    *fake.search_projects.lock().unwrap() = vec![search_project(1, "team/api")];
    *fake.search_groups.lock().unwrap() = vec![search_group(1, "team")];
    fake
}

/// Like `connected_handlers`, but keeps the fake reachable for post-sync
/// assertions on its call logs.
fn connected_handlers_shared(fake: Arc<FakeGitlab>) -> (Handlers, tempfile::TempDir) {
    connected_handlers_with_host(fake, "gitlab.example.com")
}

/// [`connected_handlers_shared`] with an explicit session host, for the
/// auto-population host rule.
fn connected_handlers_with_host(
    fake: Arc<FakeGitlab>,
    host: &str,
) -> (Handlers, tempfile::TempDir) {
    handlers_with(ConnState::Connected(Session {
        gitlab: fake,
        host: host.into(),
        user_id: 1,
    }))
}

#[tokio::test]
async fn search_sync_throttled_while_stamps_fresh() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let now = now_secs();
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: now,
            last_full_sync_secs: now,
            schema_version: SEARCH_SCHEMA_VERSION,
            ..Default::default()
        })
        .unwrap();

    h.sync_search_cache().await;

    assert_eq!(
        fake.project_list_calls.load(Ordering::SeqCst),
        0,
        "fresh stamps → no GitLab traffic (the restart-storm guard)"
    );
    assert!(fake.search_issue_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn search_sync_cold_cache_runs_full_and_stamps_both() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;

    h.sync_search_cache().await;

    assert_eq!(h.search.all_issues().unwrap().len(), 1);
    assert_eq!(h.search.all_mrs().unwrap().len(), 1);
    assert_eq!(h.search.all_projects().unwrap().len(), 1);
    assert_eq!(h.search.all_groups().unwrap().len(), 1);

    let stamps = h.search.stamps().unwrap();
    assert!(stamps.last_partial_sync_secs > 0, "partial stamp set");
    assert_eq!(
        stamps.last_partial_sync_secs, stamps.last_full_sync_secs,
        "a full sync advances both stamps together"
    );

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    assert_eq!(
        issue_calls.as_slice(),
        &[(None, None)],
        "population=all + full sync → one global fetch, no cursor"
    );
}

#[tokio::test]
async fn search_sync_full_drops_entries_gitlab_no_longer_returns() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;
    {
        let g = h.search.try_begin_sync().unwrap();
        g.upsert_issues(&[search_issue(99, "deleted upstream")])
            .unwrap();
        g.upsert_mrs(&[search_mr(99, "deleted upstream")]).unwrap();
    }

    h.sync_search_cache().await; // zero stamps → full sync

    let ids: Vec<i64> = h
        .search
        .all_issues()
        .unwrap()
        .iter()
        .map(|i| i.id)
        .collect();
    assert_eq!(ids, vec![1], "issue 99 reconciled away by the full sync");
    let ids: Vec<i64> = h.search.all_mrs().unwrap().iter().map(|m| m.id).collect();
    assert_eq!(ids, vec![1], "mr 99 reconciled away by the full sync");
}

#[tokio::test]
async fn search_sync_incremental_uses_overlap_cursor_and_keeps_full_stamp() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;
    let now = now_secs();
    // Partial overdue (default interval 1800s), full still fresh.
    let stamps = SyncStamps {
        last_partial_sync_secs: now - 10_000,
        last_full_sync_secs: now - 100,
        schema_version: SEARCH_SCHEMA_VERSION,
        ..Default::default()
    };
    {
        let g = h.search.try_begin_sync().unwrap();
        g.set_stamps(&stamps).unwrap();
        // An entry GitLab no longer returns: an incremental sync must keep it.
        g.upsert_issues(&[search_issue(99, "stale but kept")])
            .unwrap();
    }

    h.sync_search_cache().await;

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    assert_eq!(issue_calls.len(), 1);
    let (project, cursor) = issue_calls[0];
    assert_eq!(project, None);
    assert_eq!(
        cursor
            .expect("incremental sync passes a cursor")
            .timestamp() as u64,
        stamps.last_partial_sync_secs - 300,
        "cursor is the last partial sync minus the overlap margin"
    );

    let fresh = h.search.stamps().unwrap();
    assert!(
        fresh.last_partial_sync_secs >= now,
        "partial stamp advanced"
    );
    assert_eq!(
        fresh.last_full_sync_secs, stamps.last_full_sync_secs,
        "incremental sync leaves the full stamp alone"
    );

    let mut ids: Vec<i64> = h
        .search
        .all_issues()
        .unwrap()
        .iter()
        .map(|i| i.id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 99], "incremental sync upserts without pruning");
}

#[tokio::test]
async fn search_sync_transient_failure_leaves_stamps_and_demotes() {
    let fake = Arc::new(FakeGitlab::failing(FetchErr::Transient));
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    let stamps = h.search.stamps().unwrap();
    assert_eq!(
        (stamps.last_partial_sync_secs, stamps.last_full_sync_secs),
        (0, 0),
        "failed sync must not advance the stamps — next tick retries"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Dormant(_)),
        "transient fetch failure demotes the session"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), h.reconnect_signal.notified())
            .await
            .is_ok(),
        "demotion wakes the reconnect supervisor"
    );
}

#[tokio::test]
async fn search_sync_permanent_failure_does_not_demote() {
    let fake = Arc::new(FakeGitlab::failing(FetchErr::Permanent));
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    assert_eq!(h.search.stamps().unwrap().last_partial_sync_secs, 0);
    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a permanent GitLab error is logged, not a connectivity problem"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), h.reconnect_signal.notified())
            .await
            .is_err(),
        "no reconnect signal for a permanent error"
    );
}

#[tokio::test]
async fn search_sync_assigned_mrs_feed_corpus_and_assigned_view() {
    use gitlab_trackr_api::AsyncCall;
    let fake = Arc::new(canned_search_fake());
    // The population fetch sees nothing; only the direct assigned fetch
    // carries the MR (session user is id 1).
    *fake.search_mrs.lock().unwrap() = vec![];
    let mut mine = search_mr(50, "assigned but unpopulated");
    mine.assignees = vec![crate::search::MrAssignee {
        id: 1,
        username: "me".into(),
    }];
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    assert_eq!(fake.assigned_mr_calls.load(Ordering::SeqCst), 1);
    let ids: Vec<i64> = h.search.all_mrs().unwrap().iter().map(|m| m.id).collect();
    assert_eq!(ids, vec![50], "assigned MR reached the corpus directly");

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(&mut call as &mut dyn Call_GetAssignedMergeRequests, None)
        .await
        .unwrap();
    let mrs = reply_mrs(&mut call);
    assert_eq!(
        mrs.len(),
        1,
        "assigned view must not depend on population coverage"
    );
    assert_eq!(mrs[0].iid, 500);
}

#[tokio::test]
async fn search_sync_full_retain_keeps_assigned_mrs() {
    let fake = Arc::new(canned_search_fake()); // population serves MR id 1
    *fake.assigned_mrs.lock().unwrap() = vec![search_mr(50, "assigned only")];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await; // zero stamps → full sync with retain

    let mut ids: Vec<i64> = h.search.all_mrs().unwrap().iter().map(|m| m.id).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 50],
        "full-sync retain must not prune an assigned MR the population fetch missed"
    );
}

#[tokio::test]
async fn search_sync_assigned_mr_transient_failure_leaves_stamps_and_demotes() {
    let fake = Arc::new(FakeGitlab {
        assigned_mr_err: Some(FetchErr::Transient),
        ..canned_search_fake()
    });
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    let stamps = h.search.stamps().unwrap();
    assert_eq!(
        (stamps.last_partial_sync_secs, stamps.last_full_sync_secs),
        (0, 0),
        "a failed assigned-MR fetch must not advance the stamps"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Dormant(_)),
        "transient assigned-MR fetch failure demotes the session"
    );
}

// ── Search: pure cache reader ──────────────────────────────────────────

use gitlab_trackr_api::{Call_Search, Search_Reply};

/// Parsed parameters from a successful `Search` reply.
fn reply_search(call: &mut gitlab_trackr_api::AsyncCall) -> Search_Reply {
    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_none(),
        "expected success, got {:?}",
        reply.error
    );
    serde_json::from_value(reply.parameters.expect("parameters")).expect("parse reply")
}

/// Drive `Search` against `h` and parse the successful reply.
async fn run_search(
    h: &Handlers,
    query: &str,
    kinds: Option<Vec<String>>,
    limit: Option<i64>,
) -> Search_Reply {
    use gitlab_trackr_api::AsyncCall;
    let mut call = AsyncCall::default();
    h.search(
        &mut call as &mut dyn Call_Search,
        query.to_string(),
        kinds,
        limit,
    )
    .await
    .unwrap();
    reply_search(&mut call)
}

/// Seed all four search kinds and stamp the cache as synced. The sync guard
/// drops on return, so callers are free to drive syncs or `clear_cache`.
fn seed_search_cache(h: &Handlers) {
    let g = h.search.try_begin_sync().unwrap();
    let mut labeled = search_issue(2, "unrelated title");
    labeled.labels = vec!["Backend".to_string()];
    g.upsert_issues(&[search_issue(1, "OAuth token refresh"), labeled])
        .unwrap();
    g.upsert_mrs(&[search_mr(3, "Fix oauth flow")]).unwrap();
    g.upsert_projects(&[search_project(4, "team/auth-service")])
        .unwrap();
    g.upsert_groups(&[search_group(5, "team")]).unwrap();
    g.set_stamps(&SyncStamps {
        last_partial_sync_secs: 1,
        last_full_sync_secs: 1,
        ..Default::default()
    })
    .unwrap();
}

#[tokio::test]
async fn search_matches_title_labels_and_paths_case_insensitively() {
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    let r = run_search(&h, "OAUTH", None, None).await;
    assert_eq!(r.issues.len(), 1, "title substring match");
    assert_eq!(r.issues[0].id, 1);
    assert_eq!(r.merge_requests.len(), 1, "MR title match");
    assert!(r.projects.is_empty());
    assert!(r.groups.is_empty());

    let r = run_search(&h, "backend", None, None).await;
    assert_eq!(r.issues.len(), 1, "label match");
    assert_eq!(r.issues[0].id, 2);

    let r = run_search(&h, "team", None, None).await;
    assert_eq!(r.projects.len(), 1, "project path match");
    assert_eq!(r.groups.len(), 1, "group path match");
}

#[tokio::test]
async fn search_iid_reference_matches_issues_and_mrs() {
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    // search_issue(1, ..) has iid 10; search_mr(3, ..) has iid 30.
    let r = run_search(&h, "#10", None, None).await;
    assert_eq!(r.issues.len(), 1, "issue found by #iid");
    assert!(r.merge_requests.is_empty(), "no MR has iid 10");

    let r = run_search(&h, "#30", None, None).await;
    assert!(r.issues.is_empty());
    assert_eq!(r.merge_requests.len(), 1, "MR found by #iid");
}

#[tokio::test]
async fn search_kinds_filter_restricts_the_reply() {
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    let r = run_search(&h, "team", Some(vec!["projects".to_string()]), None).await;
    assert_eq!(r.projects.len(), 1);
    assert!(
        r.groups.is_empty(),
        "matching group suppressed by the kinds filter"
    );
}

#[tokio::test]
async fn search_orders_by_recency_and_applies_per_kind_limit() {
    let (h, _dir) = dormant_handlers();
    let mut old = search_issue(1, "match old");
    old.updated_at_secs = 100;
    let mut new = search_issue(2, "match new");
    new.updated_at_secs = 200;
    {
        let g = h.search.try_begin_sync().unwrap();
        g.upsert_issues(&[old, new]).unwrap();
        g.set_stamps(&SyncStamps {
            last_partial_sync_secs: 1,
            last_full_sync_secs: 1,
            ..Default::default()
        })
        .unwrap();
    }

    let r = run_search(&h, "match", None, Some(1)).await;
    assert_eq!(r.issues.len(), 1, "per-kind limit applied");
    assert_eq!(r.issues[0].id, 2, "most recently updated wins");
}

#[tokio::test]
async fn search_cold_cache_connected_is_empty() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    let r = run_search(&h, "canned", None, None).await;
    assert!(
        r.issues.is_empty() && r.merge_requests.is_empty(),
        "first sync still pending → empty reply, no fetch from the read path"
    );
    assert_eq!(
        fake.project_list_calls.load(Ordering::SeqCst),
        0,
        "the read handler must never touch GitLab"
    );
}

#[tokio::test]
async fn search_graph_status_comes_from_cached_boards_only() {
    let (h, _dir) = dormant_handlers();
    h.boards.put(1, vec!["Doing".to_string()]).unwrap();

    let mut on_board = search_issue(1, "match on-board");
    on_board.labels = vec!["random".to_string(), "Doing".to_string()];
    let mut off_board = search_issue(2, "match off-board");
    off_board.labels = vec!["random".to_string()];
    let mut foreign = search_issue(3, "match foreign project");
    foreign.project_id = 99;
    {
        let g = h.search.try_begin_sync().unwrap();
        g.upsert_issues(&[on_board, off_board, foreign]).unwrap();
        g.set_stamps(&SyncStamps {
            last_partial_sync_secs: 1,
            last_full_sync_secs: 1,
            ..Default::default()
        })
        .unwrap();
    }

    let r = run_search(&h, "match", None, None).await;
    let by_id: HashMap<i64, &Issue> = r.issues.iter().map(|i| (i.id, i)).collect();
    assert_eq!(by_id[&1].graph_status, "Doing", "board label matched");
    assert_eq!(
        by_id[&2].graph_status, "opened",
        "no matching label → state fallback"
    );
    assert_eq!(
        by_id[&3].graph_status, "",
        "project without cached board → empty, no fetch"
    );
}

#[tokio::test]
async fn clear_cache_search_scope_empties_and_resets_stamps() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    let mut call = AsyncCall::default();
    h.clear_cache(
        &mut call as &mut dyn Call_ClearCache,
        Some(vec!["search".to_string()]),
    )
    .await
    .unwrap();

    assert!(h.search.all_issues().unwrap().is_empty());
    assert!(h.search.all_projects().unwrap().is_empty());
    assert_eq!(
        h.search.stamps().unwrap().last_partial_sync_secs,
        0,
        "stamps reset so the next sync runs full"
    );
    // Dormant → no re-fetch; history untouched by the search scope.
}

#[tokio::test]
async fn clear_cache_search_scope_waits_out_an_in_flight_sync() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    // Simulate a sync in flight: while its guard is held, the clear must
    // block instead of wiping the corpus under the sync's feet.
    let guard = h.search.try_begin_sync().unwrap();
    let mut call = AsyncCall::default();
    let clear = h.clear_cache(
        &mut call as &mut dyn Call_ClearCache,
        Some(vec!["search".to_string()]),
    );
    let mut clear = std::pin::pin!(clear);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), clear.as_mut())
            .await
            .is_err(),
        "clear waits while a sync holds the gate"
    );

    drop(guard);
    clear.await.unwrap();
    assert!(h.search.all_issues().unwrap().is_empty());
    assert_eq!(h.search.stamps().unwrap().last_partial_sync_secs, 0);
}

#[tokio::test]
async fn clear_cache_search_scope_triggers_full_resync_when_connected() {
    use gitlab_trackr_api::AsyncCall;
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;
    seed_search_cache(&h);

    let mut call = AsyncCall::default();
    h.clear_cache(
        &mut call as &mut dyn Call_ClearCache,
        Some(vec!["search".to_string()]),
    )
    .await
    .unwrap();

    assert!(
        fake.project_list_calls.load(Ordering::SeqCst) > 0,
        "connected clear re-syncs immediately"
    );
    assert_eq!(
        h.search.all_issues().unwrap().len(),
        1,
        "cache repopulated from the canned full sync"
    );
    assert!(h.search.stamps().unwrap().last_full_sync_secs > 0);
}

#[tokio::test]
async fn search_sync_auto_resolves_to_tracked_on_every_host() {
    for host in ["gitlab.com", "gitlab.example.com"] {
        let fake = Arc::new(canned_search_fake());
        let (h, _dir) = connected_handlers_with_host(Arc::clone(&fake), host);

        h.sync_search_cache().await;

        assert!(
            fake.search_issue_calls.lock().unwrap().is_empty(),
            "auto → tracked with no evidence fetches no issues on {host}"
        );
        assert!(
            fake.search_mr_calls.lock().unwrap().is_empty(),
            "auto → tracked with no evidence fetches no MRs on {host}"
        );
        assert_eq!(
            fake.assigned_mr_calls.load(Ordering::SeqCst),
            1,
            "the direct assigned-MR fetch still runs on {host}"
        );
        let stamps = h.search.stamps().unwrap();
        assert!(
            stamps.last_full_sync_secs > 0,
            "an evidence-less tracked sync still completes and stamps"
        );
        assert!(!stamps.degraded_to_member, "vestigial flag stays false");
    }
}

#[tokio::test]
async fn search_sync_tracked_derives_from_all_evidence_sources() {
    let fake = Arc::new(canned_search_fake());
    // Evidence: assigned MR in project 9; the direct fetch delivers it.
    let mut mine = search_mr(90, "assigned");
    mine.project_id = 9;
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    // Evidence: assigned issue in project 7 (issue cache) and a recent
    // history entry in project 5.
    h.cache
        .put(&[issue(7, 70, "assigned", "https://gl/team/api/-/issues/70")])
        .unwrap();
    let mut event = stored(1, now_secs());
    event.project_id = 5;
    h.history.upsert(&[event]).unwrap();

    h.sync_search_cache().await;

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    let fetched: Vec<Option<i64>> = issue_calls.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        fetched,
        vec![Some(5), Some(7), Some(9)],
        "exactly the evidenced projects are refreshed, in order"
    );
    assert!(
        issue_calls.iter().all(|(_, cursor)| cursor.is_none()),
        "cold sync is a full sync — no cursor"
    );
    drop(issue_calls);

    let mut tracked: Vec<i64> = h
        .search
        .tracked_projects()
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    tracked.sort_unstable();
    assert_eq!(tracked, vec![5, 7, 9]);
}

#[tokio::test]
async fn search_sync_tracked_partial_uses_cursor_per_project() {
    let fake = Arc::new(canned_search_fake());
    let mut mine = search_mr(90, "assigned");
    mine.project_id = 9;
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let now = now_secs();
    let stamps = SyncStamps {
        last_partial_sync_secs: now - 10_000,
        last_full_sync_secs: now - 100,
        schema_version: SEARCH_SCHEMA_VERSION,
        synced_user_id: 1,
        ..Default::default()
    };
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&stamps)
        .unwrap();

    h.sync_search_cache().await;

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    assert_eq!(issue_calls.len(), 1);
    let (project, cursor) = issue_calls[0];
    assert_eq!(project, Some(9), "partial refreshes the tracked project");
    assert_eq!(
        cursor.expect("partial sync passes a cursor").timestamp() as u64,
        stamps.last_partial_sync_secs - 300,
        "cursor is the last partial sync minus the overlap margin"
    );
}

#[tokio::test]
async fn search_sync_tracked_full_retains_per_project() {
    let fake = Arc::new(canned_search_fake());
    // Tracked evidence for project 9; the per-project fetch serves the
    // canned issue 1 (project 1), i.e. nothing that vouches for issue 99.
    let mut mine = search_mr(90, "assigned");
    mine.project_id = 9;
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    {
        let g = h.search.try_begin_sync().unwrap();
        let mut stale = search_issue(99, "deleted upstream");
        stale.project_id = 9;
        g.upsert_issues(&[stale]).unwrap();
    }

    h.sync_search_cache().await; // zero stamps → full sync

    let ids: Vec<i64> = h
        .search
        .all_issues()
        .unwrap()
        .iter()
        .map(|i| i.id)
        .collect();
    assert_eq!(
        ids,
        vec![1],
        "issue 99 reconciled away by the per-project retain; the fetched issue survives"
    );
}

#[tokio::test]
async fn search_sync_tracked_evicts_stale_projects_and_prunes_their_corpus() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let now = now_secs();
    // Project 3 was tracked long ago (evidence far outside the 90-day
    // retention) and left corpus entries behind; no current evidence.
    {
        let g = h.search.try_begin_sync().unwrap();
        g.note_tracked([3], now - 400 * 24 * 3600).unwrap();
        let mut old = search_issue(30, "from the evicted project");
        old.project_id = 3;
        g.upsert_issues(&[old]).unwrap();
    }

    h.sync_search_cache().await; // zero stamps → full sync

    assert!(
        h.search.tracked_projects().unwrap().is_empty(),
        "stale evidence evicts the tracked project"
    );
    assert!(
        h.search.all_issues().unwrap().is_empty(),
        "the evicted project's corpus entries are pruned"
    );
    assert!(
        fake.search_issue_calls.lock().unwrap().is_empty(),
        "evicted projects are not refreshed"
    );
}

#[tokio::test]
async fn search_sync_tracked_migration_prunes_eager_leftovers_only_on_full() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let now = now_secs();
    // A corpus inherited from an eager population: entries in a project with
    // no evidence, under valid current-schema stamps.
    {
        let g = h.search.try_begin_sync().unwrap();
        let mut leftover = search_issue(42, "from the all-population era");
        leftover.project_id = 3;
        g.upsert_issues(&[leftover]).unwrap();
        g.set_stamps(&SyncStamps {
            last_partial_sync_secs: now - 10_000,
            last_full_sync_secs: now - 100,
            schema_version: SEARCH_SCHEMA_VERSION,
            synced_user_id: 1,
            ..Default::default()
        })
        .unwrap();
    }

    h.sync_search_cache().await; // partial: upsert-only, no sweep

    assert_eq!(
        h.search.all_issues().unwrap().len(),
        1,
        "a partial sync keeps unevidenced leftovers"
    );

    // Force the full cadence: the sweep drops what nothing vouches for.
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: now - 10_000,
            last_full_sync_secs: 1,
            schema_version: SEARCH_SCHEMA_VERSION,
            synced_user_id: 1,
            ..Default::default()
        })
        .unwrap();

    h.sync_search_cache().await;

    assert!(
        h.search.all_issues().unwrap().is_empty(),
        "the full-tier sweep prunes corpus entries of never-tracked projects"
    );
}

// ── Search: transparent live phase ──────────────────────────────────────

/// Scaffolding for the `service.rs` streaming tests: a connected handler
/// whose live issue lookup returns one canned hit (id 70, "oauth live") over
/// a seeded corpus (issue 1 matches "oauth"). `delay` fakes a slow instance.
pub(crate) fn connected_with_live_hit(delay: Option<Duration>) -> (Handlers, tempfile::TempDir) {
    let fake = Arc::new(FakeGitlab {
        live_delay: delay,
        ..canned_search_fake()
    });
    *fake.live_issues.lock().unwrap() = vec![search_issue(70, "oauth live")];
    let (h, dir) = connected_handlers_shared(fake);
    seed_search_cache(&h);
    (h, dir)
}

/// Dormant handlers over a seeded corpus — the cache-only streaming case.
pub(crate) fn dormant_with_seeded_corpus() -> (Handlers, tempfile::TempDir) {
    let (h, dir) = dormant_handlers();
    seed_search_cache(&h);
    (h, dir)
}

#[tokio::test]
async fn search_live_hits_merge_persist_and_track_member_projects_only() {
    let fake = Arc::new(canned_search_fake());
    // Two live hits whose titles do NOT contain the needle — the server
    // matched them in descriptions, which the corpus doesn't store. One is
    // in the member project the seeded corpus knows (4), one in a foreign
    // project (7) — e.g. a public hit on a shared instance.
    let mut member_hit = search_issue(60, "unrelated words");
    member_hit.project_id = 4;
    let mut foreign_hit = search_issue(70, "other unrelated words");
    foreign_hit.project_id = 7;
    *fake.live_issues.lock().unwrap() = vec![member_hit, foreign_hit];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    seed_search_cache(&h); // corpus: issue 1 "OAuth token refresh", project 4

    let reply = run_search(&h, "oauth", None, None).await;

    let ids: Vec<i64> = reply.issues.iter().map(|i| i.id).collect();
    assert!(ids.contains(&1), "the local title match is served");
    assert!(
        ids.contains(&60) && ids.contains(&70),
        "description-only live hits pass through"
    );
    let cached: Vec<i64> = h
        .search
        .all_issues()
        .unwrap()
        .iter()
        .map(|i| i.id)
        .collect();
    assert!(
        cached.contains(&60) && cached.contains(&70),
        "both live hits land in the corpus"
    );
    let tracked: Vec<i64> = h
        .search
        .tracked_projects()
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        tracked.contains(&4),
        "the member project's live hit earns a tracked slot"
    );
    assert!(
        !tracked.contains(&7),
        "a foreign project's live hit must not enroll it in the background refresh"
    );
}

#[tokio::test]
async fn search_sync_tracked_skips_permanently_rejected_projects() {
    let fake = Arc::new(FakeGitlab {
        // Project 5 (history evidence) 403s its per-project fetches; the
        // sync must skip it and still refresh project 9 and stamp.
        project_search_err: Some((5, FetchErr::Permanent)),
        ..canned_search_fake()
    });
    let mut mine = search_mr(90, "assigned");
    mine.project_id = 9;
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let mut event = stored(1, now_secs());
    event.project_id = 5;
    h.history.upsert(&[event]).unwrap();

    h.sync_search_cache().await;

    let stamps = h.search.stamps().unwrap();
    assert!(
        stamps.last_full_sync_secs > 0,
        "one inaccessible project must not wedge the sync"
    );
    let fetched: Vec<Option<i64>> = fake
        .search_issue_calls
        .lock()
        .unwrap()
        .iter()
        .map(|(p, _)| *p)
        .collect();
    assert_eq!(
        fetched,
        vec![Some(5), Some(9)],
        "the rejected project is attempted, the healthy one still refreshed"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a per-project rejection is not a connectivity problem"
    );
}

#[tokio::test]
async fn search_sync_tracked_transient_project_failure_still_aborts() {
    let fake = Arc::new(FakeGitlab {
        project_search_err: Some((9, FetchErr::Transient)),
        ..canned_search_fake()
    });
    let mut mine = search_mr(90, "assigned");
    mine.project_id = 9;
    *fake.assigned_mrs.lock().unwrap() = vec![mine];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    assert_eq!(
        h.search.stamps().unwrap().last_partial_sync_secs,
        0,
        "a network failure aborts the run so the next tick retries"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Dormant(_)),
        "transient per-project failure demotes as usual"
    );
}

#[tokio::test]
async fn search_live_hit_updates_and_dedupes_with_cached_row() {
    let fake = Arc::new(canned_search_fake());
    let mut fresher = search_issue(1, "OAuth token refresh (edited)");
    fresher.updated_at_secs = 999;
    *fake.live_issues.lock().unwrap() = vec![fresher];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    seed_search_cache(&h);

    let reply = run_search(&h, "oauth", Some(vec!["issues".into()]), None).await;

    let matching: Vec<_> = reply.issues.iter().filter(|i| i.id == 1).collect();
    assert_eq!(matching.len(), 1, "one reply row per global id");
    assert_eq!(
        matching[0].title, "OAuth token refresh (edited)",
        "the fresher live copy wins"
    );
}

#[tokio::test]
async fn search_live_deadline_falls_back_to_cache() {
    let fake = Arc::new(FakeGitlab {
        live_delay: Some(Duration::from_millis(300)),
        ..canned_search_fake()
    });
    *fake.live_issues.lock().unwrap() = vec![search_issue(70, "oauth live")];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.live_deadline_ms = 50;
    seed_search_cache(&h);

    let started = std::time::Instant::now();
    let reply = run_search(&h, "oauth", Some(vec!["issues".into()]), None).await;
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "the deadline bounds the wait"
    );

    let ids: Vec<i64> = reply.issues.iter().map(|i| i.id).collect();
    assert_eq!(ids, vec![1], "cache-only reply after the deadline");
    assert!(
        !h.search.all_issues().unwrap().iter().any(|i| i.id == 70),
        "a timed-out lookup caches nothing"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a slow instance is not a dead session"
    );
}

#[tokio::test]
async fn search_first_ever_query_works_lazily() {
    let fake = Arc::new(canned_search_fake());
    *fake.live_issues.lock().unwrap() = vec![search_issue(70, "oauth live")];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    // No seed, zero stamps: the pre-tracked daemon replied empty here.

    let reply = run_search(&h, "oauth", Some(vec!["issues".into()]), None).await;

    let ids: Vec<i64> = reply.issues.iter().map(|i| i.id).collect();
    assert_eq!(ids, vec![70], "cold cache + live hit → results");
    assert!(
        h.search.all_issues().unwrap().iter().any(|i| i.id == 70),
        "the first search seeds the corpus"
    );
}

#[tokio::test]
async fn search_cold_cache_dormant_is_not_authenticated() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();

    let mut call = AsyncCall::default();
    h.search(
        &mut call as &mut dyn Call_Search,
        "anything".to_string(),
        None,
        None,
    )
    .await
    .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert_eq!(
        reply.error.as_deref(),
        Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
        "never-synced while dormant → honest auth error"
    );
}

#[tokio::test]
async fn search_dormant_serves_cache_without_live_lookup() {
    let (h, _dir) = dormant_handlers();
    seed_search_cache(&h);

    let reply = run_search(&h, "oauth", None, None).await;
    assert_eq!(
        reply.issues.len(),
        1,
        "dormant search stays a pure cache read"
    );
}

#[tokio::test]
async fn search_live_failure_never_demotes() {
    for err in [FetchErr::Transient, FetchErr::Permanent] {
        let fake = Arc::new(FakeGitlab {
            live_err: Some(err),
            ..canned_search_fake()
        });
        let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
        seed_search_cache(&h);

        let reply = run_search(&h, "oauth", None, None).await;
        assert_eq!(reply.issues.len(), 1, "the cache is still served");
        assert!(
            matches!(&*h.session.read().await, ConnState::Connected(_)),
            "the read path has no demotion authority ({err:?})"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), h.reconnect_signal.notified())
                .await
                .is_err(),
            "no reconnect signal from the live path"
        );
    }
}

#[tokio::test]
async fn search_live_debounce_skips_identical_queries() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    seed_search_cache(&h);

    run_search(&h, "oauth", None, None).await;
    run_search(&h, "oauth", None, None).await;
    assert_eq!(
        fake.live_issue_calls.load(Ordering::SeqCst),
        1,
        "an identical repeat within the window is debounced"
    );

    run_search(&h, "other", None, None).await;
    assert_eq!(
        fake.live_issue_calls.load(Ordering::SeqCst),
        2,
        "a different query is not debounced"
    );
}

#[tokio::test]
async fn search_live_debounce_disabled_by_zero() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.live_debounce_secs = 0;
    seed_search_cache(&h);

    run_search(&h, "oauth", None, None).await;
    run_search(&h, "oauth", None, None).await;
    assert_eq!(fake.live_issue_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn search_eager_population_stays_a_pure_cache_read() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::Member;
    seed_search_cache(&h);

    let reply = run_search(&h, "oauth", None, None).await;
    assert_eq!(reply.issues.len(), 1);
    assert_eq!(
        fake.live_calls(),
        0,
        "eager populations keep Search fully offline"
    );
}

#[tokio::test]
async fn search_iid_query_filters_live_noise() {
    let fake = Arc::new(canned_search_fake());
    // Both live hits matched "#10" only in their descriptions server-side:
    // one is the real reference (iid 10), one is unrelated noise (iid 999).
    let mut noise = search_mr(80, "unrelated words");
    noise.iid = 999;
    let mut real = search_mr(81, "also unrelated");
    real.iid = 10;
    *fake.live_mrs.lock().unwrap() = vec![noise, real];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    seed_search_cache(&h);

    let reply = run_search(&h, "#10", Some(vec!["merge_requests".into()]), None).await;

    let iids: Vec<i64> = reply.merge_requests.iter().map(|m| m.iid).collect();
    assert!(iids.contains(&10), "the exact reference passes");
    assert!(
        !iids.contains(&999),
        "reference queries stay exact — description noise is filtered"
    );
}

#[tokio::test]
async fn search_serves_live_hits_without_caching_while_sync_holds_the_gate() {
    let fake = Arc::new(canned_search_fake());
    *fake.live_issues.lock().unwrap() = vec![search_issue(70, "oauth live")];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    seed_search_cache(&h);
    let guard = h.search.begin_sync().await; // a background sync in flight

    let reply = run_search(&h, "oauth", Some(vec!["issues".into()]), None).await;

    let ids: Vec<i64> = reply.issues.iter().map(|i| i.id).collect();
    assert!(ids.contains(&70), "live hits are served via pass-through");
    drop(guard);
    assert!(
        !h.search.all_issues().unwrap().iter().any(|i| i.id == 70),
        "nothing was cached while the gate was held"
    );
}

// ── GetAssignedMergeRequests ────────────────────────────────────────────

/// Seed the search corpus with MRs and stamps as one completed sync (user 1,
/// current schema) would have left them. The assigned-MR view is cold until
/// both rows AND stamps exist — zero stamps read as never-synced.
fn seed_assigned_mrs(h: &Handlers) {
    let g = h.search.try_begin_sync().unwrap();
    let mut mine = search_mr(1, "mine"); // project 1, iid 10
    mine.assignees = vec![crate::search::MrAssignee {
        id: 1,
        username: "me".into(),
    }];
    let mut mine_closed = search_mr(2, "mine but closed"); // iid 20
    mine_closed.assignees = vec![crate::search::MrAssignee {
        id: 1,
        username: "me".into(),
    }];
    mine_closed.state = "closed".into();
    let mut someone_elses = search_mr(3, "someone else's"); // iid 30
    someone_elses.assignees = vec![crate::search::MrAssignee {
        id: 9,
        username: "them".into(),
    }];
    g.upsert_mrs(&[mine, mine_closed, someone_elses]).unwrap();
    g.set_stamps(&SyncStamps {
        last_partial_sync_secs: 100,
        last_full_sync_secs: 100,
        degraded_to_member: false,
        schema_version: SEARCH_SCHEMA_VERSION,
        synced_user_id: 1,
    })
    .unwrap();
}

fn reply_mrs(call: &mut gitlab_trackr_api::AsyncCall) -> Vec<gitlab_trackr_api::MergeRequest> {
    let reply = call.take_reply().expect("a reply");
    assert!(
        reply.error.is_none(),
        "expected success, got {:?}",
        reply.error
    );
    let params: gitlab_trackr_api::GetAssignedMergeRequests_Reply =
        serde_json::from_value(reply.parameters.expect("parameters")).expect("parse reply");
    params.merge_requests
}

#[tokio::test]
async fn get_assigned_merge_requests_serves_open_assigned_from_cache_while_dormant() {
    use gitlab_trackr_api::AsyncCall;
    // Dormant on purpose: the view is a pure cache read.
    let (h, _dir) = dormant_handlers();
    seed_assigned_mrs(&h);

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(&mut call as &mut dyn Call_GetAssignedMergeRequests, None)
        .await
        .unwrap();

    let mrs = reply_mrs(&mut call);
    assert_eq!(mrs.len(), 1, "assigned-to-me AND opened only");
    assert_eq!(mrs[0].iid, 10);
    assert_eq!(mrs[0].assignees, vec!["me".to_string()]);
}

#[tokio::test]
async fn get_assigned_merge_requests_cold_cache_dormant_is_not_authenticated() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(&mut call as &mut dyn Call_GetAssignedMergeRequests, None)
        .await
        .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert_eq!(
        reply.error.as_deref(),
        Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
        "never-synced while dormant → honest auth error, not fake-empty"
    );
}

#[tokio::test]
async fn get_assigned_merge_requests_cold_cache_connected_is_empty() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = connected_handlers(FakeGitlab::default());

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(&mut call as &mut dyn Call_GetAssignedMergeRequests, None)
        .await
        .unwrap();

    assert!(
        reply_mrs(&mut call).is_empty(),
        "connected + first sync pending → empty reply"
    );
}

#[tokio::test]
async fn get_assigned_merge_requests_stale_schema_reads_as_cold() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_assigned_mrs(&h);
    // Downgrade the stamps to the pre-assignee schema: rows exist but their
    // assignee data can't be trusted yet.
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: 100,
            last_full_sync_secs: 100,
            degraded_to_member: false,
            schema_version: 0,
            synced_user_id: 0,
        })
        .unwrap();

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(&mut call as &mut dyn Call_GetAssignedMergeRequests, None)
        .await
        .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert_eq!(
        reply.error.as_deref(),
        Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
        "stale schema while dormant → cold-cache behavior"
    );
}

#[tokio::test]
async fn get_assigned_merge_requests_group_filter_matches_namespace() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    seed_assigned_mrs(&h); // web_urls live under gl/team/api/-/…

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(
        &mut call as &mut dyn Call_GetAssignedMergeRequests,
        Some(vec!["team".into()]),
    )
    .await
    .unwrap();
    assert_eq!(reply_mrs(&mut call).len(), 1, "subgroup-inclusive match");

    let mut call = AsyncCall::default();
    h.get_assigned_merge_requests(
        &mut call as &mut dyn Call_GetAssignedMergeRequests,
        Some(vec!["other".into()]),
    )
    .await
    .unwrap();
    assert!(reply_mrs(&mut call).is_empty(), "non-matching group filter");
}

// ── MR write reflection in the search cache ─────────────────────────────

#[tokio::test]
async fn close_mr_while_unreachable_queues_and_updates_search_row() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    seed_assigned_mrs(&h);
    seed_grouped_cache(&h); // issue cache must stay untouched by an MR close

    let mut call = AsyncCall::default();
    h.close(
        &mut call as &mut dyn Call_Close,
        1,
        10,
        IssuableKind::merge_request,
    )
    .await
    .unwrap();

    let reply = call.take_reply().expect("a reply");
    assert!(reply.error.is_none(), "unreachable → queued, success reply");

    let row = h
        .search
        .all_mrs()
        .unwrap()
        .into_iter()
        .find(|m| m.iid == 10)
        .unwrap();
    assert_eq!(row.state, "closed", "cached MR state flipped immediately");
    assert_eq!(
        h.cache.get().unwrap().unwrap().len(),
        3,
        "issue cache untouched by an MR close"
    );
}

#[tokio::test]
async fn unassign_mr_removes_synced_user_from_cached_assignees() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    seed_assigned_mrs(&h);

    let mut call = AsyncCall::default();
    h.unassign_self(
        &mut call as &mut dyn Call_UnassignSelf,
        1,
        10,
        IssuableKind::merge_request,
    )
    .await
    .unwrap();
    assert!(call.take_reply().unwrap().error.is_none());

    let row = h
        .search
        .all_mrs()
        .unwrap()
        .into_iter()
        .find(|m| m.iid == 10)
        .unwrap();
    assert!(
        row.assignees.is_empty(),
        "synced user removed from the cached row"
    );
}

#[tokio::test]
async fn mr_cache_update_skips_when_sync_gate_is_held() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    seed_assigned_mrs(&h);

    // Simulate an in-flight sync: the write handler must not block on the
    // gate — it skips the cache update and still replies success.
    let _guard = h.search.try_begin_sync().unwrap();

    let mut call = AsyncCall::default();
    h.close(
        &mut call as &mut dyn Call_Close,
        1,
        10,
        IssuableKind::merge_request,
    )
    .await
    .unwrap();
    assert!(call.take_reply().unwrap().error.is_none());

    let row = h
        .search
        .all_mrs()
        .unwrap()
        .into_iter()
        .find(|m| m.iid == 10)
        .unwrap();
    assert_eq!(
        row.state, "opened",
        "gate contended → cache update skipped, sync reconciles later"
    );
}

#[tokio::test]
async fn post_time_on_mr_defers_with_the_global_mr_id() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    seed_assigned_mrs(&h);

    let mut call = AsyncCall::default();
    h.post_time(
        &mut call as &mut dyn Call_PostTime,
        1,
        10,
        IssuableKind::merge_request,
        "30m".to_string(),
        None,
    )
    .await
    .unwrap();
    assert!(call.take_reply().unwrap().error.is_none());

    let pending = h.queue.pending_post_time().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].kind, crate::gitlab::Issuable::MergeRequest);
}

#[tokio::test]
async fn get_history_joins_queued_mr_titles_from_search_corpus() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = handlers_with(ConnState::Dormant(DormancyReason::Unreachable {
        host: "gitlab.example.com".into(),
        detail: "connection refused".into(),
    }));
    seed_assigned_mrs(&h);

    let mut call = AsyncCall::default();
    h.post_time(
        &mut call as &mut dyn Call_PostTime,
        1,
        10,
        IssuableKind::merge_request,
        "30m".to_string(),
        None,
    )
    .await
    .unwrap();
    call.take_reply();

    let mut call = AsyncCall::default();
    h.get_history(&mut call as &mut dyn Call_GetHistory, None)
        .await
        .unwrap();
    let reply = call.take_reply().expect("a reply");
    assert!(reply.error.is_none());
    let params: gitlab_trackr_api::GetHistory_Reply =
        serde_json::from_value(reply.parameters.expect("parameters")).unwrap();
    assert_eq!(params.events.len(), 1);
    let e = &params.events[0];
    assert_eq!(e.kind, IssuableKind::merge_request);
    assert_eq!(e.iid, 10);
    assert_eq!(e.title, "mine", "queued MR title joined from search corpus");
    assert_eq!(e.source, "queued");
}

#[tokio::test]
async fn search_sync_stale_schema_version_forces_full_resync() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;
    let now = now_secs();
    // Perfectly fresh stamps, but written under entry-schema version 0 (rows
    // predate assignee capture). Without the version check this sync would be
    // throttled outright.
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: now - 1,
            last_full_sync_secs: now - 1,
            degraded_to_member: false,
            schema_version: 0,
            synced_user_id: 0,
        })
        .unwrap();

    h.sync_search_cache().await;

    let calls = fake.search_issue_calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        &[(None, None)],
        "stale schema → treated as never synced → one full global fetch"
    );
    drop(calls);

    let stamps = h.search.stamps().unwrap();
    assert_eq!(stamps.schema_version, SEARCH_SCHEMA_VERSION);
    assert_eq!(
        stamps.synced_user_id, 1,
        "session user id persisted for the assigned-MR filter"
    );
}

#[tokio::test]
async fn search_sync_explicit_all_is_never_degraded() {
    let mut fake = canned_search_fake();
    fake.global_search_err = Some(FetchErr::Permanent);
    let fake = Arc::new(fake);
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::All;

    h.sync_search_cache().await;

    let calls = fake.search_issue_calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        &[(None, None)],
        "an explicit all is respected — no member fallback"
    );
    let stamps = h.search.stamps().unwrap();
    assert_eq!(
        stamps.last_partial_sync_secs, 0,
        "rejected sync does not stamp"
    );
    assert!(!stamps.degraded_to_member);
}

#[tokio::test]
async fn search_sync_member_population_fetches_per_project() {
    let fake = Arc::new(canned_search_fake());
    *fake.search_projects.lock().unwrap() =
        vec![search_project(1, "team/api"), search_project(2, "team/web")];
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.config.write().unwrap().search.population = SearchPopulation::Member;

    h.sync_search_cache().await;

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    let projects: Vec<Option<i64>> = issue_calls.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        projects,
        vec![Some(1), Some(2)],
        "member population → one issues fetch per member project"
    );
    let mr_calls = fake.search_mr_calls.lock().unwrap();
    let projects: Vec<Option<i64>> = mr_calls.iter().map(|(p, _)| *p).collect();
    assert_eq!(projects, vec![Some(1), Some(2)]);
}

// ── property tests: the varlink surface must be total ──────────────────
//
// The socket is the daemon's externally reachable boundary, so beyond the
// scenario tests above we fuzz it: for arbitrary arguments a handler must
// always produce a reply (never panic), reject invalid input with the
// documented error, and never let a read path touch GitLab. proptest bodies
// are sync, so each case runs on a small current-thread runtime; every case
// also builds the real handler stack on a fjall tempdir, so the case counts
// stay low.

use proptest::prelude::*;

fn prop_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Generalizes the old four-row bad-arguments table: any argument triple
    /// gets either a success reply or the documented eager rejection, and the
    /// read path never issues a GitLab call.
    #[test]
    fn search_replies_or_rejects_any_arguments_and_gates_the_live_lookup(
        query in ".{0,12}",
        kinds in proptest::option::of(proptest::collection::vec("[a-z_]{1,14}", 0..3)),
        limit in proptest::option::of(any::<i64>()),
    ) {
        use gitlab_trackr_api::AsyncCall;
        prop_rt().block_on(async {
            let fake = Arc::new(canned_search_fake());
            let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
            seed_search_cache(&h);

            let mut call = AsyncCall::default();
            h.search(
                &mut call as &mut dyn Call_Search,
                query.clone(),
                kinds.clone(),
                limit,
            )
            .await
            .unwrap();
            let reply = call.take_reply().expect("always a reply");

            let invalid = query.trim().is_empty()
                || matches!(limit, Some(n) if n <= 0)
                || kinds.iter().flatten().any(|k| {
                    !["issues", "merge_requests", "projects", "groups"].contains(&k.as_str())
                });
            if invalid {
                assert_eq!(
                    reply.error.as_deref(),
                    Some("org.thehoster.gitlab.trackrd.GitlabError"),
                    "bad args must be rejected eagerly"
                );
            } else {
                assert!(
                    reply.error.is_none(),
                    "valid args must succeed, got {:?}",
                    reply.error
                );
            }
            assert_eq!(
                fake.project_list_calls.load(Ordering::SeqCst)
                    + fake.group_list_calls.load(Ordering::SeqCst),
                0,
                "the read handler must never touch the eager fetch surface"
            );
            if invalid {
                assert_eq!(
                    fake.live_calls(),
                    0,
                    "bad args must be rejected before the live lookup runs"
                );
            }
            assert!(
                fake.search_issue_calls.lock().unwrap().is_empty()
                    && fake.search_mr_calls.lock().unwrap().is_empty(),
                "the read handler must never fetch issues or MRs"
            );
        });
    }

    /// Generalizes the cold-cache dormant example: whatever valid query the
    /// client sends, a dormant daemon with a never-synced cache replies with
    /// the honest auth error instead of fabricating an empty result.
    #[test]
    fn search_cold_cache_dormant_is_not_authenticated_for_any_query(
        query in "[a-zA-Z0-9#]{1,10}",
        limit in proptest::option::of(1i64..100),
    ) {
        use gitlab_trackr_api::AsyncCall;
        prop_rt().block_on(async {
            let (h, _dir) = dormant_handlers();
            let mut call = AsyncCall::default();
            h.search(&mut call as &mut dyn Call_Search, query.clone(), None, limit)
                .await
                .unwrap();
            let reply = call.take_reply().expect("a reply");
            assert_eq!(
                reply.error.as_deref(),
                Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
                "never-synced cache + no session → honest auth error"
            );
        });
    }

    /// Model check for the reader pipeline: results come from the seeded
    /// cache, every hit actually matches, newest wins, the per-kind limit
    /// holds.
    #[test]
    fn search_reader_filters_orders_and_limits_against_the_model(
        titles in proptest::collection::vec("[a-e]{2,5}", 1..8),
        needle in "[a-e]{1,3}",
        limit in 1i64..4,
    ) {
        prop_rt().block_on(async {
            let (h, _dir) = dormant_handlers();
            let issues: Vec<SearchIssue> = titles
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let mut s = search_issue(i as i64 + 1, t);
                    s.updated_at_secs = 1_000 - i as u64; // distinct, newest first
                    s
                })
                .collect();
            {
                let g = h.search.try_begin_sync().unwrap();
                g.upsert_issues(&issues).unwrap();
                g.set_stamps(&SyncStamps {
                    last_partial_sync_secs: 1,
                    last_full_sync_secs: 1,
                    ..Default::default()
                })
                .unwrap();
            }

            let r = run_search(&h, &needle, Some(vec!["issues".to_string()]), Some(limit)).await;

            let expected: Vec<i64> = issues
                .iter()
                .filter(|i| i.title.contains(&needle))
                .map(|i| i.id)
                .take(limit as usize)
                .collect();
            let got: Vec<i64> = r.issues.iter().map(|i| i.id).collect();
            assert_eq!(
                got, expected,
                "matches only, from the seeded set, newest first, limited"
            );
        });
    }

    /// The write path's eager pre-checks, fuzzed: invalid input is rejected
    /// with an error reply, valid input on a no-credentials session gets the
    /// honest auth error — and neither may enqueue anything (queueing only
    /// helps an unreachable session).
    #[test]
    fn post_time_never_queues_from_a_no_credentials_session(
        project_id in -2i64..6,
        issue_iid in -2i64..6,
        duration in prop_oneof!["[0-9]{1,3}[smhdw]", "[a-z]{0,4}", "[0-9]{1,2}x"],
    ) {
        use gitlab_trackr_api::AsyncCall;
        prop_rt().block_on(async {
            let (h, _dir) = dormant_handlers();
            let mut call = AsyncCall::default();
            h.post_time(
                &mut call as &mut dyn Call_PostTime,
                project_id,
                issue_iid,
                IssuableKind::issue,
                duration.clone(),
                None,
            )
            .await
            .unwrap();

            let reply = call.take_reply().expect("always a reply");
            let valid_args = issue_ref_error(project_id, issue_iid).is_none()
                && looks_like_duration(&duration);
            if valid_args {
                assert_eq!(
                    reply.error.as_deref(),
                    Some("org.thehoster.gitlab.trackrd.NotAuthenticated"),
                    "valid write on a credential-less session → honest auth error"
                );
            } else {
                assert_eq!(
                    reply.error.as_deref(),
                    Some("org.thehoster.gitlab.trackrd.GitlabError"),
                    "invalid input is rejected eagerly"
                );
            }
            assert!(
                h.queue.pending_post_time().unwrap().is_empty(),
                "nothing may be enqueued from a no-credentials session"
            );
        });
    }
}

// ── refresh stamp gating ───────────────────────────────────────────────

/// A `FakeGitlab` whose assigned-issue and timelog fetches serve one canned
/// entry each, so the stamp-gating tests can drive successful refreshes and
/// count the traffic.
fn canned_refresh_fake() -> FakeGitlab {
    let fake = FakeGitlab::default();
    *fake.assigned.lock().unwrap() = Some(vec![iwl(7, "opened", &["bug"])]);
    *fake.timelogs.lock().unwrap() = Some(vec![FetchedTimelog {
        timelog_id: 1,
        // Recent, so the prune following the slow-tier refreshes keeps it.
        spent_at_secs: now_secs(),
        kind: Issuable::Issue,
        project_id: 0,
        iid: 1,
        title: "t".into(),
        web_url: "u".into(),
        duration: "1h".into(),
        summary: String::new(),
    }]);
    fake
}

#[tokio::test]
async fn quick_refresh_throttled_while_stamp_fresh() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.refresh_meta
        .update(|s| s.last_quick_sync_secs = now_secs())
        .unwrap();

    h.refresh_cache().await;

    assert_eq!(
        fake.assigned_calls.load(Ordering::SeqCst),
        0,
        "fresh stamp → no GitLab traffic (the restart-storm guard)"
    );
    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn quick_refresh_runs_when_stale_and_stamps() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    // Past the default quick interval (300s).
    let stale = now_secs() - 10_000;
    h.refresh_meta
        .update(|s| s.last_quick_sync_secs = stale)
        .unwrap();

    h.refresh_cache().await;

    assert_eq!(fake.assigned_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.cache.get().unwrap().map(|v| v.len()),
        Some(1),
        "issue cache populated"
    );
    assert!(
        h.refresh_meta.stamps().unwrap().last_quick_sync_secs > stale,
        "successful refresh advances the quick stamp"
    );
}

#[tokio::test]
async fn quick_refresh_failure_leaves_stamp_unwritten() {
    let (h, _dir) = connected_handlers(FakeGitlab::failing(FetchErr::Transient));

    h.refresh_cache().await;

    assert_eq!(
        h.refresh_meta.stamps().unwrap().last_quick_sync_secs,
        0,
        "a failed refresh must not stamp, so the next tick retries"
    );
}

#[tokio::test]
async fn daily_refresh_throttled_while_slow_stamp_fresh() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    h.refresh_meta
        .update(|s| s.last_slow_sync_secs = now_secs())
        .unwrap();

    h.refresh_history_daily().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn daily_refresh_runs_when_stale_and_stamps() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    // Past the default slow interval (86400s).
    let stale = now_secs() - 100_000;
    h.refresh_meta
        .update(|s| s.last_slow_sync_secs = stale)
        .unwrap();

    h.refresh_history_daily().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 1);
    assert!(
        h.refresh_meta.stamps().unwrap().last_slow_sync_secs > stale,
        "successful daily refresh advances the slow stamp"
    );
}

#[tokio::test]
async fn backfill_skips_when_slow_stamp_fresh_and_retention_covered() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let retention_hours = h.config.read().unwrap().history.retention_hours;
    h.refresh_meta
        .update(|s| {
            s.last_slow_sync_secs = now_secs();
            s.backfilled_retention_hours = retention_hours;
            s.schema_version = crate::refresh_meta::HISTORY_SCHEMA_VERSION;
        })
        .unwrap();

    h.backfill_history().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn backfill_runs_when_history_schema_is_stale() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let retention_hours = h.config.read().unwrap().history.retention_hours;
    // Slow stamp fresh and retention covered, but the stored-timelog schema
    // predates kind support — one full-window re-backfill is due so old
    // MR-as-issue junk rows get overwritten by timelog_id.
    h.refresh_meta
        .update(|s| {
            s.last_slow_sync_secs = now_secs();
            s.backfilled_retention_hours = retention_hours;
            s.schema_version = 0;
        })
        .unwrap();

    h.backfill_history().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.refresh_meta.stamps().unwrap().schema_version,
        crate::refresh_meta::HISTORY_SCHEMA_VERSION,
        "successful backfill stamps the current schema version"
    );
}

#[tokio::test]
async fn backfill_reruns_when_retention_grew() {
    let fake = Arc::new(canned_refresh_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let retention_hours = h.config.read().unwrap().history.retention_hours;
    // Slow stamp fresh, but the recorded backfill width is narrower than the
    // configured retention → one re-backfill is due.
    h.refresh_meta
        .update(|s| {
            s.last_slow_sync_secs = now_secs();
            s.backfilled_retention_hours = retention_hours - 1;
        })
        .unwrap();

    h.backfill_history().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.refresh_meta.stamps().unwrap().backfilled_retention_hours,
        retention_hours,
        "the widened backfill is recorded"
    );
}

#[tokio::test]
async fn dormant_refresh_writes_no_stamp() {
    let (h, _dir) = dormant_handlers();

    h.refresh_cache().await;
    h.refresh_history_daily().await;
    h.backfill_history().await;

    let s = h.refresh_meta.stamps().unwrap();
    assert_eq!(
        s.last_quick_sync_secs, 0,
        "a dormant skip must not stamp — the post-reconnect warm-up must run"
    );
    assert_eq!(s.last_slow_sync_secs, 0);
}

#[tokio::test]
async fn clear_cache_zeroes_refresh_stamps() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    h.refresh_meta
        .update(|s| {
            s.last_quick_sync_secs = 5;
            s.last_slow_sync_secs = 5;
            s.backfilled_retention_hours = 5;
        })
        .unwrap();

    let mut call = AsyncCall::default();
    h.clear_cache(&mut call as &mut dyn Call_ClearCache, None)
        .await
        .unwrap();

    let s = h.refresh_meta.stamps().unwrap();
    assert_eq!(s.last_quick_sync_secs, 0);
    assert_eq!(s.last_slow_sync_secs, 0);
    assert_eq!(s.backfilled_retention_hours, 0);
}

#[tokio::test]
async fn clear_cache_issues_scope_zeroes_only_quick_stamp() {
    use gitlab_trackr_api::AsyncCall;
    let (h, _dir) = dormant_handlers();
    h.refresh_meta
        .update(|s| {
            s.last_quick_sync_secs = 5;
            s.last_slow_sync_secs = 5;
        })
        .unwrap();

    let mut call = AsyncCall::default();
    h.clear_cache(
        &mut call as &mut dyn Call_ClearCache,
        Some(vec!["issues".to_string()]),
    )
    .await
    .unwrap();

    let s = h.refresh_meta.stamps().unwrap();
    assert_eq!(s.last_quick_sync_secs, 0, "issues scope zeroes quick");
    assert_eq!(s.last_slow_sync_secs, 5, "history stamps untouched");
}
