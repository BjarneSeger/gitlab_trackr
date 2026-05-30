//! Background retry queue for outgoing write operations.
//!
//! Tasks are persisted via `KvStore` before being processed, so they survive
//! daemon restarts.  A background tokio task works through the queue with
//! exponential backoff (1 s base, 30 min cap).  Network errors trigger
//! retries for up to 7 days; a GitLab rejection or an exhausted retry window
//! moves the task to a persistent dead-letter store, surfaced via `tt queue`.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::db::KvStore;
use crate::error::{Error, Result};
use crate::handlers::SessionSlot;
use crate::impl_redb_json_value;

// Table name bumped from `post_time_queue` so older `StoredTask` records that
// don't carry the new `op` field don't crash deserialization on startup.
const QUEUE_TABLE: TableDefinition<u64, StoredTask> = TableDefinition::new("retry_queue_v4");
const DEAD_LETTER_TABLE: TableDefinition<u64, StoredFailure> =
    TableDefinition::new("dead_letter_v1");

/// Tunable retry-queue timing, sourced from the daemon config.
#[derive(Clone, Copy)]
pub struct QueueConfig {
    /// Initial exponential-backoff delay.
    pub base_delay: Duration,
    /// Exponential-backoff cap.
    pub max_delay: Duration,
    /// How long a task keeps retrying before it is dead-lettered.
    pub max_lifetime: Duration,
    /// How long the worker sleeps while dormant (no session) before retrying.
    pub session_wait: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_mins(30),
            max_lifetime: Duration::from_hours(168),
            session_wait: Duration::from_secs(30),
        }
    }
}

/// Set once by [`RetryQueue::new`]; read by the background worker. Falls back to
/// [`QueueConfig::default`] if the worker ever runs before it is set.
static QUEUE_CFG: std::sync::OnceLock<QueueConfig> = std::sync::OnceLock::new();

fn queue_cfg() -> QueueConfig {
    *QUEUE_CFG.get_or_init(QueueConfig::default)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum QueueOp {
    PostTime {
        duration: String,
        summary: Option<String>,
        /// Global numeric issue ID, resolved from the issue cache at enqueue
        /// time. When present, the worker uses GraphQL `timelogCreate` so it
        /// can submit the original `queued_at_secs` as `spentAt`. `None` means
        /// the cache didn't know the issue (or the entry survived a daemon
        /// upgrade), and the worker falls back to REST without `spent_at`.
        issue_id: Option<i64>,
    },
    CloseIssue,
    AssignSelf,
    UnassignSelf,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredTask {
    project_id: i64,
    issue_iid: i64,
    op: QueueOp,
    /// UNIX timestamp (seconds) when the task was first enqueued.
    queued_at_secs: u64,
}

impl_redb_json_value!(StoredTask, "StoredTaskV4");

/// A task the worker gave up on — GitLab rejected it outright, or it exhausted
/// the retry window. Persisted so the user can inspect, retry, or dismiss it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFailure {
    project_id: i64,
    issue_iid: i64,
    op: QueueOp,
    /// When the task was originally enqueued.
    queued_at_secs: u64,
    /// When the worker gave up.
    failed_at_secs: u64,
    /// The GitLab error or retry-window-exhaustion message.
    error: String,
}

impl_redb_json_value!(StoredFailure, "StoredFailureV1");

struct QueuedTask {
    id: u64,
    project_id: i64,
    issue_iid: i64,
    op: QueueOp,
    queued_at_secs: u64,
}

pub struct RetryQueue {
    sender: mpsc::Sender<QueuedTask>,
    store: KvStore<u64, StoredTask>,
    dead_letter: KvStore<u64, StoredFailure>,
    next_id: AtomicU64,
}

/// A PostTime task currently in the retry queue, projected for the history view.
pub struct PendingPostTime {
    pub project_id: i64,
    pub issue_iid: i64,
    pub duration: String,
    pub summary: Option<String>,
    pub queued_at_secs: u64,
}

/// A dead-lettered task, projected for the `tt queue` view.
pub struct FailedTaskView {
    pub id: u64,
    pub op_kind: &'static str,
    pub project_id: i64,
    pub issue_iid: i64,
    /// Human-readable op detail (e.g. PostTime's duration + summary).
    pub detail: String,
    pub error: String,
    pub queued_at_secs: u64,
    pub failed_at_secs: u64,
}

impl RetryQueue {
    /// Open (or create) the queue database at `db_path`, reload any tasks that
    /// survived a previous restart, and spawn the background worker.
    pub fn new(session: SessionSlot, db_path: &Path, cfg: QueueConfig) -> Result<Self> {
        let _ = QUEUE_CFG.set(cfg);
        let store = KvStore::open(db_path, QUEUE_TABLE)?;
        // The dead-letter store lives in its own redb file beside the queue —
        // redb takes an exclusive lock per file, so it can't share `db_path`.
        let dead_letter = KvStore::open(
            &db_path.with_file_name("dead_letter.redb"),
            DEAD_LETTER_TABLE,
        )?;

        let initial_tasks = store.scan(|id, stored| {
            Ok(QueuedTask {
                id,
                project_id: stored.project_id,
                issue_iid: stored.issue_iid,
                op: stored.op,
                queued_at_secs: stored.queued_at_secs,
            })
        })?;

        // Seed `next_id` above the max key across *both* tables so dead-letter
        // IDs stay monotonic across restarts (the `tt tick` notice dedupes by ID).
        let queued_max = initial_tasks.iter().map(|t| t.id).max().unwrap_or(0);
        let dead_max = dead_letter
            .scan(|id, _| Ok(id))?
            .into_iter()
            .max()
            .unwrap_or(0);
        let max_id = queued_max.max(dead_max);

        if !initial_tasks.is_empty() {
            info!(
                count = initial_tasks.len(),
                "reloaded pending tasks from queue database"
            );
        }

        let (tx, rx) = mpsc::channel(256);

        if !initial_tasks.is_empty() {
            let tx_init = tx.clone();
            tokio::spawn(async move {
                for task in initial_tasks {
                    if tx_init.send(task).await.is_err() {
                        break;
                    }
                }
            });
        }

        tokio::spawn(worker(session, store.clone(), dead_letter.clone(), rx));

        Ok(Self {
            sender: tx,
            store,
            dead_letter,
            next_id: AtomicU64::new(max_id + 1),
        })
    }

    /// Persist a `PostTime` task to disk and hand it to the background worker.
    /// Returns immediately; the caller does not wait for the network operation.
    ///
    /// `issue_id` is the global numeric ID (the one GraphQL embeds in
    /// `gid://gitlab/Issue/<id>`). When `Some`, the worker uses GraphQL
    /// `timelogCreate` and submits the original `queued_at` as `spentAt`, so a
    /// task held for hours/days still shows up in GitLab at the time it was
    /// actually logged. When `None`, it falls back to REST without `spent_at`.
    pub async fn post_time(
        &self,
        project_id: i64,
        issue_iid: i64,
        duration: String,
        summary: Option<String>,
        issue_id: Option<i64>,
    ) {
        self.enqueue(
            project_id,
            issue_iid,
            QueueOp::PostTime {
                duration,
                summary,
                issue_id,
            },
        )
        .await
    }

    /// Persist a `CloseIssue` task to disk and hand it to the background worker.
    /// Returns immediately; the caller does not wait for the network operation.
    pub async fn close_issue(&self, project_id: i64, issue_iid: i64) {
        self.enqueue(project_id, issue_iid, QueueOp::CloseIssue)
            .await
    }

    /// Persist an `AssignSelf` task to disk and hand it to the background worker.
    pub async fn assign_self(&self, project_id: i64, issue_iid: i64) {
        self.enqueue(project_id, issue_iid, QueueOp::AssignSelf)
            .await
    }

    /// Persist an `UnassignSelf` task to disk and hand it to the background worker.
    pub async fn unassign_self(&self, project_id: i64, issue_iid: i64) {
        self.enqueue(project_id, issue_iid, QueueOp::UnassignSelf)
            .await
    }

    /// Snapshot of PostTime tasks currently sitting in the queue.
    ///
    /// Used by the history view to prepend not-yet-flushed entries so the
    /// user sees their just-logged time before the round-trip completes.
    /// Once the worker succeeds and removes the task, the next history poll
    /// surfaces the canonical GitLab record in its place.
    pub fn pending_post_time(&self) -> Result<Vec<PendingPostTime>> {
        snapshot_pending(&self.store)
    }

    async fn enqueue(&self, project_id: i64, issue_iid: i64, op: QueueOp) {
        self.enqueue_stored(StoredTask {
            project_id,
            issue_iid,
            op,
            queued_at_secs: now_secs(),
        })
        .await
    }

    /// Persist `stored` under a fresh ID and hand it to the worker, keeping its
    /// `queued_at_secs` intact (so a retried PostTime keeps its original
    /// spent-at). Used by both fresh enqueues and dead-letter retries.
    async fn enqueue_stored(&self, stored: StoredTask) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let task = QueuedTask {
            id,
            project_id: stored.project_id,
            issue_iid: stored.issue_iid,
            op: stored.op.clone(),
            queued_at_secs: stored.queued_at_secs,
        };
        if let Err(e) = self.store.put(id, stored) {
            warn!(
                error = %e,
                "failed to persist task to queue db; it will not survive a restart"
            );
        }
        if self.sender.send(task).await.is_err() {
            error!("retry queue channel closed; dropping task");
        }
    }

    /// Snapshot of dead-lettered tasks (failed permanently or timed out),
    /// newest failure first.
    pub fn failures(&self) -> Result<Vec<FailedTaskView>> {
        let mut out = self.dead_letter.scan(|id, f| {
            Ok(FailedTaskView {
                id,
                op_kind: f.op.kind(),
                project_id: f.project_id,
                issue_iid: f.issue_iid,
                detail: f.op.detail(),
                error: f.error,
                queued_at_secs: f.queued_at_secs,
                failed_at_secs: f.failed_at_secs,
            })
        })?;
        out.sort_by_key(|f| std::cmp::Reverse(f.failed_at_secs));
        Ok(out)
    }

    /// Re-enqueue a dead-lettered task (preserving its original enqueue time)
    /// and drop it from the dead-letter store. Returns `false` if `id` is
    /// unknown.
    pub async fn retry_failure(&self, id: u64) -> Result<bool> {
        let Some(failure) = self.dead_letter.get(id)? else {
            return Ok(false);
        };
        self.enqueue_stored(StoredTask {
            project_id: failure.project_id,
            issue_iid: failure.issue_iid,
            op: failure.op,
            queued_at_secs: failure.queued_at_secs,
        })
        .await;
        self.dead_letter.remove(id)?;
        Ok(true)
    }

    /// Drop a single dead-lettered task without retrying. Returns `false` if
    /// `id` is unknown.
    pub fn dismiss_failure(&self, id: u64) -> Result<bool> {
        if self.dead_letter.get(id)?.is_none() {
            return Ok(false);
        }
        self.dead_letter.remove(id)?;
        Ok(true)
    }

    /// Drop every dead-lettered task.
    pub fn clear_failures(&self) -> Result<()> {
        self.dead_letter.clear()
    }
}

fn snapshot_pending(store: &KvStore<u64, StoredTask>) -> Result<Vec<PendingPostTime>> {
    let mut out = store
        .scan(|_, stored| {
            Ok(match stored.op {
                QueueOp::PostTime {
                    duration, summary, ..
                } => Some(PendingPostTime {
                    project_id: stored.project_id,
                    issue_iid: stored.issue_iid,
                    duration,
                    summary,
                    queued_at_secs: stored.queued_at_secs,
                }),
                QueueOp::CloseIssue | QueueOp::AssignSelf | QueueOp::UnassignSelf => None,
            })
        })?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    out.sort_by_key(|p| std::cmp::Reverse(p.queued_at_secs));
    Ok(out)
}

async fn worker(
    session: SessionSlot,
    store: KvStore<u64, StoredTask>,
    dead_letter: KvStore<u64, StoredFailure>,
    mut rx: mpsc::Receiver<QueuedTask>,
) {
    while let Some(task) = rx.recv().await {
        let mut delay = queue_cfg().base_delay;
        let mut attempt = 0u32;

        // `None` ⇒ succeeded; `Some(msg)` ⇒ gave up and should be dead-lettered.
        let failure: Option<String> = 'retry: loop {
            attempt += 1;
            let gitlab = match session.read().await.as_ref().map(|s| s.gitlab.clone()) {
                Some(g) => g,
                None => {
                    warn!(
                        attempt,
                        project_id = task.project_id,
                        issue_iid = task.issue_iid,
                        op = task.op.kind(),
                        "no active session; deferring task"
                    );
                    tokio::time::sleep(queue_cfg().session_wait).await;
                    continue 'retry;
                }
            };
            let outcome = match &task.op {
                QueueOp::PostTime {
                    duration,
                    summary,
                    issue_id,
                } => match issue_id {
                    Some(id) => {
                        let spent_at = chrono::DateTime::<chrono::Utc>::from_timestamp(
                            task.queued_at_secs as i64,
                            0,
                        )
                        .unwrap_or_else(chrono::Utc::now);
                        gitlab
                            .create_timelog(
                                *id,
                                duration,
                                summary.as_deref().unwrap_or(""),
                                spent_at,
                            )
                            .await
                    }
                    None => {
                        gitlab
                            .add_spent_time(
                                task.project_id,
                                task.issue_iid,
                                duration,
                                summary.as_deref(),
                            )
                            .await
                    }
                },
                QueueOp::CloseIssue => gitlab.close_issue(task.project_id, task.issue_iid).await,
                QueueOp::AssignSelf => gitlab.assign_self(task.project_id, task.issue_iid).await,
                QueueOp::UnassignSelf => {
                    gitlab.unassign_self(task.project_id, task.issue_iid).await
                }
            };

            match outcome {
                Ok(()) => {
                    if attempt > 1 {
                        info!(
                            attempt,
                            project_id = task.project_id,
                            issue_iid = task.issue_iid,
                            op = task.op.kind(),
                            "task succeeded after retry"
                        );
                    }
                    break 'retry None;
                }
                Err(e) if matches!(e, Error::Transient(_)) => {
                    let elapsed =
                        Duration::from_secs(now_secs().saturating_sub(task.queued_at_secs));
                    let max_lifetime = queue_cfg().max_lifetime;
                    if elapsed >= max_lifetime {
                        error!(
                            attempt,
                            error = %e,
                            project_id = task.project_id,
                            issue_iid = task.issue_iid,
                            op = task.op.kind(),
                            retry_window = max_lifetime.as_secs(),
                            "dropping task after retry window"
                        );
                        break 'retry Some(format!(
                            "timed out after {}, seconds retry window: {}",
                            max_lifetime.as_secs(),
                            e
                        ));
                    }
                    let sleep = delay.min(max_lifetime.checked_sub(elapsed).unwrap());
                    warn!(
                        attempt,
                        error = %e,
                        delay_secs = sleep.as_secs(),
                        project_id = task.project_id,
                        op = task.op.kind(),
                        "task network error, retrying"
                    );
                    tokio::time::sleep(sleep).await;
                    delay = (delay * 2).min(queue_cfg().max_delay);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        project_id = task.project_id,
                        issue_iid = task.issue_iid,
                        op = task.op.kind(),
                        "task rejected by GitLab; dropping"
                    );
                    break 'retry Some(e.to_string());
                }
            }
        };

        // The task left the live queue either way. If it failed permanently,
        // record it in the dead-letter store (keyed by its id) so the user can
        // see, retry, or dismiss it via `tt queue`.
        if let Some(error) = failure {
            let stored = StoredFailure {
                project_id: task.project_id,
                issue_iid: task.issue_iid,
                op: task.op.clone(),
                queued_at_secs: task.queued_at_secs,
                failed_at_secs: now_secs(),
                error,
            };
            if let Err(e) = dead_letter.put(task.id, stored) {
                warn!(
                    error = %e,
                    task_id = task.id,
                    "failed to record dead-letter entry"
                );
            }
        }

        if let Err(e) = store.remove(task.id) {
            warn!(
                error = %e,
                task_id = task.id,
                "failed to remove completed task from queue db"
            );
        }
    }
}

impl QueueOp {
    fn kind(&self) -> &'static str {
        match self {
            QueueOp::PostTime { .. } => "PostTime",
            QueueOp::CloseIssue => "CloseIssue",
            QueueOp::AssignSelf => "AssignSelf",
            QueueOp::UnassignSelf => "UnassignSelf",
        }
    }

    /// Human-readable op detail for the `tt queue` view. PostTime shows its
    /// duration (and summary, if any); the other ops carry no extra detail.
    fn detail(&self) -> String {
        match self {
            QueueOp::PostTime {
                duration, summary, ..
            } => match summary {
                Some(s) if !s.is_empty() => format!("{duration} – {s}"),
                _ => duration.clone(),
            },
            QueueOp::CloseIssue | QueueOp::AssignSelf | QueueOp::UnassignSelf => String::new(),
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as TrackrResult;
    use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};
    use crate::handlers::Session;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    // The max lifetime cutoff and exponential backoff are not exercised
    // here. Both depend on `SystemTime::now()` rather than tokio's mock clock,
    // so deterministically driving them would require a clock-injection
    // abstraction that isn't warranted for the gain.

    // ── QueueOp::kind ───────────────────────────────────────────────────────

    #[test]
    fn queue_op_kind() {
        assert_eq!(QueueOp::CloseIssue.kind(), "CloseIssue");
        assert_eq!(
            QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issue_id: None,
            }
            .kind(),
            "PostTime"
        );
    }

    // ── snapshot_pending ────────────────────────────────────────────────────

    fn store() -> (KvStore<u64, StoredTask>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.redb");
        (KvStore::open(&path, QUEUE_TABLE).unwrap(), dir)
    }

    fn post_task(id: i64, queued_at: u64) -> StoredTask {
        StoredTask {
            project_id: id,
            issue_iid: id,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issue_id: None,
            },
            queued_at_secs: queued_at,
        }
    }

    fn close_task(id: i64, queued_at: u64) -> StoredTask {
        StoredTask {
            project_id: id,
            issue_iid: id,
            op: QueueOp::CloseIssue,
            queued_at_secs: queued_at,
        }
    }

    #[test]
    fn snapshot_pending_filters_close_issue_and_sorts_newest_first() {
        let (s, _td) = store();
        s.put(1, post_task(1, 100)).unwrap();
        s.put(2, close_task(2, 150)).unwrap();
        s.put(3, post_task(3, 200)).unwrap();
        s.put(4, post_task(4, 50)).unwrap();

        let snap = snapshot_pending(&s).unwrap();
        let project_ids: Vec<i64> = snap.iter().map(|p| p.project_id).collect();
        assert_eq!(
            project_ids,
            vec![3, 1, 4],
            "PostTime only, sorted by queued_at desc"
        );
    }

    #[test]
    fn snapshot_pending_empty_store() {
        let (s, _td) = store();
        assert!(snapshot_pending(&s).unwrap().is_empty());
    }

    // ── Worker behavior with a fake GitLab ──────────────────────────────────

    /// Pre-cans responses for the three methods the worker calls and counts
    /// invocations. Other `GitlabApi` methods panic so unexpected use is loud.
    #[derive(Default)]
    struct FakeGitlab {
        add_spent_time: Mutex<VecDeque<TrackrResult<()>>>,
        create_timelog: Mutex<VecDeque<TrackrResult<()>>>,
        close_issue: Mutex<VecDeque<TrackrResult<()>>>,
        assign_self: Mutex<VecDeque<TrackrResult<()>>>,
        unassign_self: Mutex<VecDeque<TrackrResult<()>>>,
        add_spent_time_calls: AtomicUsize,
        create_timelog_calls: AtomicUsize,
        close_issue_calls: AtomicUsize,
        assign_self_calls: AtomicUsize,
        unassign_self_calls: AtomicUsize,
    }

    impl FakeGitlab {
        fn push_add_spent_time(&self, r: TrackrResult<()>) {
            self.add_spent_time.lock().unwrap().push_back(r);
        }
        fn push_close_issue(&self, r: TrackrResult<()>) {
            self.close_issue.lock().unwrap().push_back(r);
        }
        fn push_assign_self(&self, r: TrackrResult<()>) {
            self.assign_self.lock().unwrap().push_back(r);
        }
    }

    #[async_trait::async_trait]
    impl GitlabApi for FakeGitlab {
        async fn fetch_assigned_issues(
            &self,
            _g: Option<String>,
        ) -> TrackrResult<Vec<IssueWithLabels>> {
            unimplemented!()
        }
        async fn fetch_group_issues(&self, _g: Vec<String>) -> TrackrResult<Vec<IssueWithLabels>> {
            unimplemented!()
        }
        async fn add_spent_time(
            &self,
            _p: i64,
            _i: i64,
            _d: &str,
            _s: Option<&str>,
        ) -> TrackrResult<()> {
            self.add_spent_time_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.add_spent_time
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned add_spent_time response")
        }
        async fn create_timelog(
            &self,
            _id: i64,
            _d: &str,
            _s: &str,
            _at: chrono::DateTime<chrono::Utc>,
        ) -> TrackrResult<()> {
            self.create_timelog_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.create_timelog
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned create_timelog response")
        }
        async fn fetch_my_timelogs(
            &self,
            _since: chrono::DateTime<chrono::Utc>,
        ) -> TrackrResult<Vec<FetchedTimelog>> {
            unimplemented!()
        }
        async fn close_issue(&self, _p: i64, _i: i64) -> TrackrResult<()> {
            self.close_issue_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.close_issue
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned close_issue response")
        }
        async fn assign_self(&self, _p: i64, _i: i64) -> TrackrResult<()> {
            self.assign_self_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.assign_self
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned assign_self response")
        }
        async fn unassign_self(&self, _p: i64, _i: i64) -> TrackrResult<()> {
            self.unassign_self_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.unassign_self
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned unassign_self response")
        }
        async fn fetch_board_list_labels(&self, _p: i64) -> TrackrResult<Vec<String>> {
            unimplemented!()
        }
    }

    /// Spawn the worker with one task on the channel, close the sender so the
    /// worker exits after draining, and await its completion. Returns the
    /// dead-letter entries it recorded (projected), newest failure first.
    async fn run_worker_one_task(
        gitlab: Arc<dyn GitlabApi>,
        store: KvStore<u64, StoredTask>,
        task: QueuedTask,
    ) -> Vec<FailedTaskView> {
        let dir = tempfile::tempdir().unwrap();
        let dead_letter =
            KvStore::open(&dir.path().join("dead_letter.redb"), DEAD_LETTER_TABLE).unwrap();
        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(Some(Session {
            gitlab,
            host: "test".to_string(),
            user_id: 0,
        })));
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(worker(session, store, dead_letter.clone(), rx));
        tx.send(task).await.unwrap();
        drop(tx);
        handle.await.unwrap();

        let mut out = dead_letter
            .scan(|id, f| {
                Ok(FailedTaskView {
                    id,
                    op_kind: f.op.kind(),
                    project_id: f.project_id,
                    issue_iid: f.issue_iid,
                    detail: f.op.detail(),
                    error: f.error,
                    queued_at_secs: f.queued_at_secs,
                    failed_at_secs: f.failed_at_secs,
                })
            })
            .unwrap();
        out.sort_by_key(|f| std::cmp::Reverse(f.failed_at_secs));
        out
    }

    #[tokio::test]
    async fn worker_removes_task_on_success() {
        let (s, _td) = store();
        let stored = post_task(7, 100);
        s.put(1, stored).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issue_id: None,
            },
            queued_at_secs: 100,
        };
        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab
                .add_spent_time_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            snapshot_pending(&s).unwrap().is_empty(),
            "task removed after success"
        );
    }

    #[tokio::test]
    async fn worker_routes_post_time_with_issue_id_to_create_timelog() {
        let (s, _td) = store();
        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: Some("note".into()),
                issue_id: Some(999),
            },
            queued_at_secs: 100,
        };
        s.put(
            1,
            StoredTask {
                project_id: 7,
                issue_iid: 7,
                op: task.op.clone(),
                queued_at_secs: 100,
            },
        )
        .unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.create_timelog.lock().unwrap().push_back(Ok(()));

        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab
                .create_timelog_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "issue_id present → GraphQL timelogCreate"
        );
        assert_eq!(
            gitlab
                .add_spent_time_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn worker_drops_task_on_permanent_error() {
        let (s, _td) = store();
        s.put(1, post_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Err(Error::Gitlab("403".into())));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issue_id: None,
            },
            queued_at_secs: 100,
        };
        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab
                .add_spent_time_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "no retry on permanent error"
        );
        assert!(
            snapshot_pending(&s).unwrap().is_empty(),
            "task dropped after permanent rejection"
        );
    }

    #[tokio::test]
    async fn worker_close_issue_success() {
        let (s, _td) = store();
        s.put(1, close_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_close_issue(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::CloseIssue,
            queued_at_secs: 100,
        };
        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab
                .close_issue_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn worker_assign_self_success() {
        let (s, _td) = store();
        let stored = StoredTask {
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::AssignSelf,
            queued_at_secs: 100,
        };
        s.put(1, stored).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_assign_self(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::AssignSelf,
            queued_at_secs: 100,
        };
        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab
                .assign_self_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            snapshot_pending(&s).unwrap().is_empty(),
            "assign_self is not a PostTime; snapshot ignores it"
        );
    }

    // ── Dead-letter store ───────────────────────────────────────────────────

    fn fail_entry(id: i64) -> StoredFailure {
        StoredFailure {
            project_id: id,
            issue_iid: id,
            op: QueueOp::CloseIssue,
            queued_at_secs: 100,
            failed_at_secs: 200,
            error: "boom".into(),
        }
    }

    fn retry_queue() -> (RetryQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        // Dormant session: the worker defers every task (30s sleep) instead of
        // processing it, so re-enqueued tasks stay put for assertions.
        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(None));
        let q = RetryQueue::new(
            session,
            &dir.path().join("queue.redb"),
            QueueConfig::default(),
        )
        .unwrap();
        (q, dir)
    }

    #[tokio::test]
    async fn worker_dead_letters_on_permanent_error() {
        let (s, _td) = store();
        s.put(1, post_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Err(Error::Gitlab("403 Forbidden".into())));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            issue_iid: 7,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issue_id: None,
            },
            queued_at_secs: 100,
        };
        let failures = run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert!(
            snapshot_pending(&s).unwrap().is_empty(),
            "task removed from the live queue"
        );
        assert_eq!(failures.len(), 1, "one dead-letter entry recorded");
        assert_eq!(failures[0].id, 1);
        assert_eq!(failures[0].op_kind, "PostTime");
        assert!(
            failures[0].error.contains("403"),
            "error preserved: {}",
            failures[0].error
        );
    }

    #[tokio::test]
    async fn retry_failure_reenqueues_with_original_time_and_clears() {
        let (q, _td) = retry_queue();
        q.dead_letter
            .put(
                5,
                StoredFailure {
                    project_id: 7,
                    issue_iid: 9,
                    op: QueueOp::CloseIssue,
                    queued_at_secs: 1_000,
                    failed_at_secs: 2_000,
                    error: "403".into(),
                },
            )
            .unwrap();

        assert!(q.retry_failure(5).await.unwrap(), "known id retried");
        assert!(
            q.failures().unwrap().is_empty(),
            "dead-letter entry cleared"
        );

        let live: Vec<(i64, i64, u64)> = q
            .store
            .scan(|_, t| Ok((t.project_id, t.issue_iid, t.queued_at_secs)))
            .unwrap();
        assert_eq!(
            live,
            vec![(7, 9, 1_000)],
            "re-enqueued, preserving original queued_at"
        );

        assert!(!q.retry_failure(999).await.unwrap(), "unknown id → false");
    }

    #[tokio::test]
    async fn dismiss_and_clear_failures() {
        let (q, _td) = retry_queue();
        q.dead_letter.put(1, fail_entry(1)).unwrap();
        q.dead_letter.put(2, fail_entry(2)).unwrap();

        assert!(q.dismiss_failure(1).unwrap(), "known id dismissed");
        assert!(!q.dismiss_failure(1).unwrap(), "already gone → false");
        assert_eq!(q.failures().unwrap().len(), 1);

        q.clear_failures().unwrap();
        assert!(q.failures().unwrap().is_empty(), "all cleared");
    }

    #[tokio::test]
    async fn new_seeds_next_id_above_both_tables() {
        let dir = tempfile::tempdir().unwrap();
        let qpath = dir.path().join("queue.redb");
        {
            let store = KvStore::open(&qpath, QUEUE_TABLE).unwrap();
            store.put(3, post_task(1, 100)).unwrap();
            let dl = KvStore::open(&qpath.with_file_name("dead_letter.redb"), DEAD_LETTER_TABLE)
                .unwrap();
            dl.put(10, fail_entry(10)).unwrap();
        }
        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(None));
        let q = RetryQueue::new(session, &qpath, QueueConfig::default()).unwrap();
        assert_eq!(
            q.next_id.load(std::sync::atomic::Ordering::Relaxed),
            11,
            "max(queue=3, dead-letter=10) + 1"
        );
    }
}
