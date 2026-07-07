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
    /// When set, `fetch_assigned_issues` / `fetch_my_timelogs` fail this way.
    fetch_err: Option<FetchErr>,
    /// When set, `add_spent_time` fails this way (drives the write-handler
    /// deferral path).
    write_err: Option<FetchErr>,
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
            Some(FetchErr::Transient) => Err(crate::error::Error::Transient("offline".into())),
            Some(FetchErr::Permanent) => {
                Err(crate::error::Error::Gitlab("500 Server Error".into()))
            }
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

#[async_trait::async_trait]
impl GitlabApi for FakeGitlab {
    async fn fetch_assigned_issues(
        &self,
        _group: Option<String>,
    ) -> TrackrResult<Vec<IssueWithLabels>> {
        self.fetch_result()
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
        self.fetch_result()
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
    let config: SharedConfig = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
    let queue = RetryQueue::new(Arc::clone(&session), &db, Arc::clone(&config)).unwrap();
    (
        Handlers {
            session,
            cache,
            boards,
            history,
            queue,
            config,
            reconnect_signal: Arc::new(Notify::new()),
        },
        dir,
    )
}

/// Build `Handlers` with a dormant (no-GitLab) session, so `clear_cache`
/// clears without the follow-up re-fetch — letting us assert the bands.
fn dormant_handlers() -> (Handlers, tempfile::TempDir) {
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
