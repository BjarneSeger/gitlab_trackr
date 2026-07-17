use super::refresh::{enrich_graph_status, enrich_timelog};
use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, RwLock};

use gitlab_trackr_api::{
    Call_ClearCache, Call_CloseIssue, Call_GetAssignedIssues, Call_PostTime, Issue,
    VarlinkInterface,
};

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::config::SharedConfig;
use crate::error::{DormancyReason, Result as TrackrResult};
use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};
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

/// How a `FakeGitlab` fails its issue / timelog fetches, so the background
/// refresh tests can drive the runtime-demotion logic.
#[derive(Clone, Copy)]
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
    /// Drives the auto-population degrade path.
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
        _project_id: i64,
        _issue_iid: i64,
        _duration: &str,
        _summary: Option<&str>,
    ) -> TrackrResult<()> {
        self.write_result()
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
        self.timelog_calls.fetch_add(1, Ordering::SeqCst);
        if self.fetch_err.is_some() {
            return self.fetch_result();
        }
        match &*self.timelogs.lock().unwrap() {
            Some(v) => Ok(v.clone()),
            None => self.fetch_result(),
        }
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
        Ok(self.search_mrs.lock().unwrap().clone())
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
        project_id: 1,
        issue_iid: 1,
        issue_title: "t".into(),
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
use crate::search::SyncStamps;

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
    let now = now_secs();
    // Partial overdue (default interval 1800s), full still fresh.
    let stamps = SyncStamps {
        last_partial_sync_secs: now - 10_000,
        last_full_sync_secs: now - 100,
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
async fn search_sync_auto_on_gitlab_com_never_tries_the_global_fetch() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_with_host(Arc::clone(&fake), "gitlab.com");

    h.sync_search_cache().await;

    let issue_calls = fake.search_issue_calls.lock().unwrap();
    assert!(!issue_calls.is_empty());
    assert!(
        issue_calls.iter().all(|(p, _)| p.is_some()),
        "gitlab.com resolves auto to member fetches only: {issue_calls:?}"
    );
    assert!(
        !h.search.stamps().unwrap().degraded_to_member,
        "the host rule is not the degrade flag"
    );
    assert_eq!(h.search.all_issues().unwrap().len(), 1);
}

#[tokio::test]
async fn search_sync_auto_degrades_to_member_on_global_rejection() {
    let mut fake = canned_search_fake();
    fake.global_search_err = Some(FetchErr::Permanent);
    let fake = Arc::new(fake);
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    {
        let calls = fake.search_issue_calls.lock().unwrap();
        assert_eq!(calls[0].0, None, "the global fetch is attempted first");
        assert!(
            calls.len() > 1 && calls[1..].iter().all(|(p, _)| p.is_some()),
            "rejection falls back to member fetches in the same sync: {calls:?}"
        );
    }
    let stamps = h.search.stamps().unwrap();
    assert!(stamps.degraded_to_member, "fallback is recorded sticky");
    assert!(
        stamps.last_full_sync_secs > 0,
        "the degraded sync still stamps"
    );
    assert_eq!(
        h.search.all_issues().unwrap().len(),
        1,
        "cache populated via the member path"
    );
    assert!(
        matches!(&*h.session.read().await, ConnState::Connected(_)),
        "a permanent rejection must not demote"
    );
}

#[tokio::test]
async fn search_sync_auto_does_not_degrade_on_transient_global_failure() {
    let mut fake = canned_search_fake();
    fake.global_search_err = Some(FetchErr::Transient);
    let fake = Arc::new(fake);
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));

    h.sync_search_cache().await;

    assert_eq!(
        fake.search_issue_calls.lock().unwrap().len(),
        1,
        "a network blip is not a scope problem — no member retry"
    );
    let stamps = h.search.stamps().unwrap();
    assert_eq!(
        stamps.last_partial_sync_secs, 0,
        "failed sync leaves stamps"
    );
    assert!(!stamps.degraded_to_member);
    assert!(
        matches!(&*h.session.read().await, ConnState::Dormant(_)),
        "transient failure demotes as usual"
    );
}

#[tokio::test]
async fn search_sync_degraded_flag_sticks_for_incremental_and_retries_on_full() {
    let fake = Arc::new(canned_search_fake());
    let (h, _dir) = connected_handlers_shared(Arc::clone(&fake));
    let now = now_secs();
    // Partial overdue, full fresh, degraded set by an earlier rejection.
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: now - 10_000,
            last_full_sync_secs: now - 100,
            degraded_to_member: true,
        })
        .unwrap();

    h.sync_search_cache().await;

    {
        let calls = fake.search_issue_calls.lock().unwrap();
        assert!(
            calls.iter().all(|(p, _)| p.is_some()),
            "incremental honors the degrade flag: {calls:?}"
        );
    }
    assert!(
        h.search.stamps().unwrap().degraded_to_member,
        "flag survives incremental syncs"
    );

    // Force the full cadence; the global fetch works now (no injected error),
    // so the full sync recovers to all-population and clears the flag.
    h.search
        .try_begin_sync()
        .unwrap()
        .set_stamps(&SyncStamps {
            last_partial_sync_secs: now - 10_000,
            last_full_sync_secs: 1,
            degraded_to_member: true,
        })
        .unwrap();
    fake.search_issue_calls.lock().unwrap().clear();

    h.sync_search_cache().await;

    assert_eq!(
        fake.search_issue_calls.lock().unwrap()[0].0,
        None,
        "a due full sync retries the global fetch"
    );
    assert!(
        !h.search.stamps().unwrap().degraded_to_member,
        "recovery clears the flag"
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
    fn search_replies_or_rejects_any_arguments_without_touching_gitlab(
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
                "the read handler must never touch GitLab"
            );
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
        issue_iid: 1,
        issue_title: "t".into(),
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
        })
        .unwrap();

    h.backfill_history().await;

    assert_eq!(fake.timelog_calls.load(Ordering::SeqCst), 0);
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
