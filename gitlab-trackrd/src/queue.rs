//! Background retry queue for outgoing write operations.
//!
//! Tasks are persisted via `KvStore` before being processed, so they survive
//! daemon restarts.  A background tokio task works through the queue with
//! exponential backoff (1 s base, 30 min cap).  Network errors trigger
//! retries for up to 7 days; a GitLab rejection or an exhausted retry window
//! moves the task to a persistent dead-letter store, surfaced via `tt queue`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

use crate::config::{SharedConfig, next_backoff};
use crate::db::KvStore;
use crate::error::{Error, Result};
use crate::gitlab::Issuable;
use crate::handlers::SessionSlot;

const QUEUE_KEYSPACE: &str = "retry_queue_v1";
const DEAD_LETTER_KEYSPACE: &str = "dead_letter_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
enum QueueOp {
    PostTime {
        duration: String,
        summary: Option<String>,
        /// Global numeric issuable ID (issue or MR, per the task's `kind`),
        /// resolved from the caches at enqueue time. When present, the worker
        /// uses GraphQL `timelogCreate` so it can submit the original
        /// `queued_at_secs` as `spentAt`. `None` means the caches didn't know
        /// the issuable (or the entry survived a daemon upgrade), and the
        /// worker falls back to REST without `spent_at`. The alias keeps
        /// tasks persisted before MR support readable.
        #[serde(alias = "issue_id")]
        issuable_id: Option<i64>,
    },
    #[serde(alias = "CloseIssue")]
    Close,
    AssignSelf,
    UnassignSelf,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredTask {
    project_id: i64,
    /// Per-project issuable iid. The alias keeps tasks persisted before MR
    /// support readable; `kind` defaults to `Issue` for the same records.
    #[serde(alias = "issue_iid")]
    iid: i64,
    #[serde(default)]
    kind: Issuable,
    op: QueueOp,
    /// UNIX timestamp (seconds) when the task was first enqueued.
    queued_at_secs: u64,
}

/// A task the worker gave up on — GitLab rejected it outright, or it exhausted
/// the retry window. Persisted so the user can inspect, retry, or dismiss it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFailure {
    project_id: i64,
    /// See [`StoredTask::iid`] for the alias/default compat story.
    #[serde(alias = "issue_iid")]
    iid: i64,
    #[serde(default)]
    kind: Issuable,
    op: QueueOp,
    /// When the task was originally enqueued.
    queued_at_secs: u64,
    /// When the worker gave up.
    failed_at_secs: u64,
    /// The GitLab error or retry-window-exhaustion message.
    error: String,
}

struct QueuedTask {
    id: u64,
    project_id: i64,
    iid: i64,
    kind: Issuable,
    op: QueueOp,
    queued_at_secs: u64,
}

pub struct RetryQueue {
    sender: mpsc::Sender<QueuedTask>,
    store: KvStore<u64, StoredTask>,
    dead_letter: KvStore<u64, StoredFailure>,
    next_id: AtomicU64,
    /// Fired to wake the worker early while it is deferring a task for lack of a
    /// session, so a freshly re-established connection drains the queue at once
    /// instead of waiting out `session_wait`. See [`RetryQueue::drain_waker`].
    drain_wake: Arc<Notify>,
}

/// A PostTime task currently in the retry queue, projected for the history view.
pub struct PendingPostTime {
    pub project_id: i64,
    pub iid: i64,
    pub kind: Issuable,
    pub duration: String,
    pub summary: Option<String>,
    pub queued_at_secs: u64,
}

/// A dead-lettered task, projected for the `tt queue` view.
pub struct FailedTaskView {
    pub id: u64,
    pub op_kind: &'static str,
    pub project_id: i64,
    pub iid: i64,
    pub kind: Issuable,
    /// Human-readable op detail (e.g. PostTime's duration + summary).
    pub detail: String,
    pub error: String,
    pub queued_at_secs: u64,
    pub failed_at_secs: u64,
}

impl RetryQueue {
    /// Open (or create) the queue keyspaces in `db`, reload any tasks that
    /// survived a previous restart, and spawn the background worker.
    ///
    /// Both stores are durable — every mutation fsyncs — because queued writes
    /// must survive a crash, unlike the re-syncable caches.
    pub fn new(session: SessionSlot, db: &fjall::Database, config: SharedConfig) -> Result<Self> {
        let store: KvStore<u64, StoredTask> = KvStore::open_durable(db, QUEUE_KEYSPACE)?;
        let dead_letter: KvStore<u64, StoredFailure> =
            KvStore::open_durable(db, DEAD_LETTER_KEYSPACE)?;

        let initial_tasks = store.scan(|id, stored| {
            Ok(QueuedTask {
                id,
                project_id: stored.project_id,
                iid: stored.iid,
                kind: stored.kind,
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

        let drain_wake = Arc::new(Notify::new());

        tokio::spawn(worker(
            session,
            store.clone(),
            dead_letter.clone(),
            rx,
            config,
            Arc::clone(&drain_wake),
        ));

        Ok(Self {
            sender: tx,
            store,
            dead_letter,
            next_id: AtomicU64::new(max_id + 1),
            drain_wake,
        })
    }

    /// A handle to nudge the worker awake while it is deferring for lack of a
    /// session. The background reconnect task fires this the instant it flips the
    /// session back to `Connected`, so deferred writes flush immediately rather
    /// than after the next `session_wait` tick.
    pub fn drain_waker(&self) -> Arc<Notify> {
        Arc::clone(&self.drain_wake)
    }

    /// Persist a `PostTime` task to disk and hand it to the background worker.
    /// Returns immediately; the caller does not wait for the network operation.
    ///
    /// `issuable_id` is the global numeric ID (the one GraphQL embeds in
    /// `gid://gitlab/<Kind>/<id>`). When `Some`, the worker uses GraphQL
    /// `timelogCreate` and submits the original `queued_at` as `spentAt`, so a
    /// task held for hours/days still shows up in GitLab at the time it was
    /// actually logged. When `None`, it falls back to REST without `spent_at`.
    pub async fn post_time(
        &self,
        kind: Issuable,
        project_id: i64,
        iid: i64,
        duration: String,
        summary: Option<String>,
        issuable_id: Option<i64>,
    ) {
        self.enqueue(
            kind,
            project_id,
            iid,
            QueueOp::PostTime {
                duration,
                summary,
                issuable_id,
            },
        )
        .await
    }

    /// Persist a `Close` task to disk and hand it to the background worker.
    /// Returns immediately; the caller does not wait for the network operation.
    pub async fn close(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.enqueue(kind, project_id, iid, QueueOp::Close).await
    }

    /// Persist an `AssignSelf` task to disk and hand it to the background worker.
    pub async fn assign_self(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.enqueue(kind, project_id, iid, QueueOp::AssignSelf)
            .await
    }

    /// Persist an `UnassignSelf` task to disk and hand it to the background worker.
    pub async fn unassign_self(&self, kind: Issuable, project_id: i64, iid: i64) {
        self.enqueue(kind, project_id, iid, QueueOp::UnassignSelf)
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

    async fn enqueue(&self, kind: Issuable, project_id: i64, iid: i64, op: QueueOp) {
        self.enqueue_stored(StoredTask {
            project_id,
            iid,
            kind,
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
            iid: stored.iid,
            kind: stored.kind,
            op: stored.op.clone(),
            queued_at_secs: stored.queued_at_secs,
        };
        if let Err(e) = self.store.put(id, &stored) {
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
                iid: f.iid,
                kind: f.kind,
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
            iid: failure.iid,
            kind: failure.kind,
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
                    iid: stored.iid,
                    kind: stored.kind,
                    duration,
                    summary,
                    queued_at_secs: stored.queued_at_secs,
                }),
                QueueOp::Close | QueueOp::AssignSelf | QueueOp::UnassignSelf => None,
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
    config: SharedConfig,
    drain_wake: Arc<Notify>,
) {
    while let Some(task) = rx.recv().await {
        let mut delay = config.read().unwrap().queue.base_delay();
        let mut attempt = 0u32;

        // `None` ⇒ succeeded; `Some(msg)` ⇒ gave up and should be dead-lettered.
        let failure: Option<String> = 'retry: loop {
            attempt += 1;
            // Bind the clone in its own statement so the read guard is released
            // here — not held across the `select!` below or the API call. A
            // guard held across the defer would block every session *writer*
            // (the reconnect commit, `tt login`) for up to `session_wait`.
            let current = session.read().await.gitlab();
            let gitlab = match current {
                Some(g) => g,
                None => {
                    warn!(
                        attempt,
                        project_id = task.project_id,
                        iid = task.iid,
                        kind = ?task.kind,
                        op = task.op.kind(),
                        "no active session; deferring task"
                    );
                    let session_wait = config.read().unwrap().queue.session_wait();
                    // Wake early if a reconnect re-established the session, so a
                    // deferred task flushes at once instead of waiting out the
                    // full interval. The waker uses `notify_one`, which leaves a
                    // permit if we haven't parked here yet — so a nudge fired the
                    // instant before this `select!` is still delivered on entry.
                    tokio::select! {
                        _ = tokio::time::sleep(session_wait) => {}
                        _ = drain_wake.notified() => {}
                    }
                    continue 'retry;
                }
            };
            let outcome = match &task.op {
                QueueOp::PostTime {
                    duration,
                    summary,
                    issuable_id,
                } => match issuable_id {
                    Some(id) => {
                        let spent_at = chrono::DateTime::<chrono::Utc>::from_timestamp(
                            task.queued_at_secs as i64,
                            0,
                        )
                        .unwrap_or_else(chrono::Utc::now);
                        gitlab
                            .create_timelog(
                                task.kind,
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
                                task.kind,
                                task.project_id,
                                task.iid,
                                duration,
                                summary.as_deref(),
                            )
                            .await
                    }
                },
                QueueOp::Close => gitlab.close(task.kind, task.project_id, task.iid).await,
                QueueOp::AssignSelf => {
                    gitlab
                        .assign_self(task.kind, task.project_id, task.iid)
                        .await
                }
                QueueOp::UnassignSelf => {
                    gitlab
                        .unassign_self(task.kind, task.project_id, task.iid)
                        .await
                }
            };

            match outcome {
                Ok(()) => {
                    if attempt > 1 {
                        info!(
                            attempt,
                            project_id = task.project_id,
                            iid = task.iid,
                            kind = ?task.kind,
                            op = task.op.kind(),
                            "task succeeded after retry"
                        );
                    }
                    break 'retry None;
                }
                Err(e) if matches!(e, Error::Transient(_)) => {
                    let elapsed =
                        Duration::from_secs(now_secs().saturating_sub(task.queued_at_secs));
                    let max_lifetime = config.read().unwrap().queue.max_lifetime();
                    if elapsed >= max_lifetime {
                        error!(
                            attempt,
                            error = %e,
                            project_id = task.project_id,
                            iid = task.iid,
                            kind = ?task.kind,
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
                    let max = config.read().unwrap().queue.max_delay();
                    delay = next_backoff(delay, max);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        project_id = task.project_id,
                        iid = task.iid,
                        kind = ?task.kind,
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
                iid: task.iid,
                kind: task.kind,
                op: task.op.clone(),
                queued_at_secs: task.queued_at_secs,
                failed_at_secs: now_secs(),
                error,
            };
            if let Err(e) = dead_letter.put(task.id, &stored) {
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
            QueueOp::Close => "Close",
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
            QueueOp::Close | QueueOp::AssignSelf | QueueOp::UnassignSelf => String::new(),
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
    use crate::error::DormancyReason;
    use crate::error::Result as TrackrResult;
    use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};
    use crate::handlers::{ConnState, Session};
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
        assert_eq!(QueueOp::Close.kind(), "Close");
        assert_eq!(
            QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
            }
            .kind(),
            "PostTime"
        );
    }

    // ── Persisted-record compatibility ──────────────────────────────────────

    /// Records written before MR support carried `issue_iid`/`issue_id`, no
    /// `kind`, and the `CloseIssue` variant name. They must keep parsing —
    /// the retry queue is durable across upgrades by design.
    #[test]
    fn stored_task_written_before_mr_support_still_parses() {
        let old = r#"{"project_id":7,"issue_iid":9,"op":{"PostTime":{"duration":"1h","summary":null,"issue_id":42}},"queued_at_secs":100}"#;
        let t: StoredTask = serde_json::from_str(old).unwrap();
        assert_eq!(t.iid, 9);
        assert_eq!(t.kind, Issuable::Issue, "missing kind defaults to Issue");
        match t.op {
            QueueOp::PostTime { issuable_id, .. } => assert_eq!(issuable_id, Some(42)),
            other => panic!("expected PostTime, got {other:?}"),
        }

        let old_close = r#"{"project_id":7,"issue_iid":9,"op":"CloseIssue","queued_at_secs":100}"#;
        let t: StoredTask = serde_json::from_str(old_close).unwrap();
        assert!(matches!(t.op, QueueOp::Close), "CloseIssue alias parses");
    }

    #[test]
    fn stored_failure_written_before_mr_support_still_parses() {
        let old = r#"{"project_id":7,"issue_iid":9,"op":"CloseIssue","queued_at_secs":100,"failed_at_secs":200,"error":"403"}"#;
        let f: StoredFailure = serde_json::from_str(old).unwrap();
        assert_eq!(f.iid, 9);
        assert_eq!(f.kind, Issuable::Issue);
        assert!(matches!(f.op, QueueOp::Close));
    }

    // ── snapshot_pending ────────────────────────────────────────────────────

    fn test_db(dir: &tempfile::TempDir) -> fjall::Database {
        fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap()
    }

    fn store() -> (KvStore<u64, StoredTask>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db(&dir);
        (KvStore::open_durable(&db, QUEUE_KEYSPACE).unwrap(), dir)
    }

    fn post_task(id: i64, queued_at: u64) -> StoredTask {
        StoredTask {
            project_id: id,
            iid: id,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
            },
            queued_at_secs: queued_at,
        }
    }

    fn close_task(id: i64, queued_at: u64) -> StoredTask {
        StoredTask {
            project_id: id,
            iid: id,
            kind: Issuable::Issue,
            op: QueueOp::Close,
            queued_at_secs: queued_at,
        }
    }

    #[test]
    fn snapshot_pending_filters_close_and_sorts_newest_first() {
        let (s, _td) = store();
        s.put(1, &post_task(1, 100)).unwrap();
        s.put(2, &close_task(2, 150)).unwrap();
        s.put(3, &post_task(3, 200)).unwrap();
        s.put(4, &post_task(4, 50)).unwrap();

        let snap = snapshot_pending(&s).unwrap();
        let project_ids: Vec<i64> = snap.iter().map(|p| p.project_id).collect();
        assert_eq!(
            project_ids,
            vec![3, 1, 4],
            "PostTime only, sorted by queued_at desc"
        );
        assert!(
            snap.iter().all(|p| p.kind == Issuable::Issue),
            "kind carried into the projection"
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
        close: Mutex<VecDeque<TrackrResult<()>>>,
        assign_self: Mutex<VecDeque<TrackrResult<()>>>,
        unassign_self: Mutex<VecDeque<TrackrResult<()>>>,
        add_spent_time_calls: AtomicUsize,
        create_timelog_calls: AtomicUsize,
        close_calls: AtomicUsize,
        assign_self_calls: AtomicUsize,
        unassign_self_calls: AtomicUsize,
        /// `(method, kind)` log so tests can assert the worker threads the
        /// task's issuable kind through to the GitLab call.
        kinds: Mutex<Vec<(&'static str, Issuable)>>,
    }

    impl FakeGitlab {
        fn push_add_spent_time(&self, r: TrackrResult<()>) {
            self.add_spent_time.lock().unwrap().push_back(r);
        }
        fn push_close(&self, r: TrackrResult<()>) {
            self.close.lock().unwrap().push_back(r);
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
        async fn add_spent_time(
            &self,
            kind: Issuable,
            _p: i64,
            _i: i64,
            _d: &str,
            _s: Option<&str>,
        ) -> TrackrResult<()> {
            self.kinds.lock().unwrap().push(("add_spent_time", kind));
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
            kind: Issuable,
            _id: i64,
            _d: &str,
            _s: &str,
            _at: chrono::DateTime<chrono::Utc>,
        ) -> TrackrResult<()> {
            self.kinds.lock().unwrap().push(("create_timelog", kind));
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
        async fn close(&self, kind: Issuable, _p: i64, _i: i64) -> TrackrResult<()> {
            self.kinds.lock().unwrap().push(("close", kind));
            self.close_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.close
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned close response")
        }
        async fn assign_self(&self, kind: Issuable, _p: i64, _i: i64) -> TrackrResult<()> {
            self.kinds.lock().unwrap().push(("assign_self", kind));
            self.assign_self_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.assign_self
                .lock()
                .unwrap()
                .pop_front()
                .expect("FakeGitlab: no canned assign_self response")
        }
        async fn unassign_self(&self, kind: Issuable, _p: i64, _i: i64) -> TrackrResult<()> {
            self.kinds.lock().unwrap().push(("unassign_self", kind));
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
        async fn fetch_issues_for_search(
            &self,
            _p: Option<i64>,
            _after: Option<chrono::DateTime<chrono::Utc>>,
        ) -> TrackrResult<Vec<crate::search::SearchIssue>> {
            unimplemented!()
        }
        async fn fetch_merge_requests_for_search(
            &self,
            _p: Option<i64>,
            _after: Option<chrono::DateTime<chrono::Utc>>,
        ) -> TrackrResult<Vec<crate::search::SearchMr>> {
            unimplemented!()
        }
        async fn fetch_assigned_merge_requests(
            &self,
        ) -> TrackrResult<Vec<crate::search::SearchMr>> {
            unimplemented!()
        }
        async fn fetch_member_projects(&self) -> TrackrResult<Vec<crate::search::SearchProject>> {
            unimplemented!()
        }
        async fn fetch_member_groups(&self) -> TrackrResult<Vec<crate::search::SearchGroup>> {
            unimplemented!()
        }
        async fn search_issues_live(
            &self,
            _q: &str,
            _l: usize,
        ) -> TrackrResult<Vec<crate::search::SearchIssue>> {
            unimplemented!()
        }
        async fn search_mrs_live(
            &self,
            _q: &str,
            _l: usize,
        ) -> TrackrResult<Vec<crate::search::SearchMr>> {
            unimplemented!()
        }
        async fn search_projects_live(
            &self,
            _q: &str,
            _l: usize,
        ) -> TrackrResult<Vec<crate::search::SearchProject>> {
            unimplemented!()
        }
        async fn search_groups_live(
            &self,
            _q: &str,
            _l: usize,
        ) -> TrackrResult<Vec<crate::search::SearchGroup>> {
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
        let dead_letter = KvStore::open_durable(&test_db(&dir), DEAD_LETTER_KEYSPACE).unwrap();
        let session: SessionSlot =
            Arc::new(tokio::sync::RwLock::new(ConnState::Connected(Session {
                gitlab,
                host: "test".to_string(),
                user_id: 0,
            })));
        let (tx, rx) = mpsc::channel(8);
        let config = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
        let drain_wake = Arc::new(Notify::new());
        let handle = tokio::spawn(worker(
            session,
            store,
            dead_letter.clone(),
            rx,
            config,
            drain_wake,
        ));
        tx.send(task).await.unwrap();
        drop(tx);
        handle.await.unwrap();

        let mut out = dead_letter
            .scan(|id, f| {
                Ok(FailedTaskView {
                    id,
                    op_kind: f.op.kind(),
                    project_id: f.project_id,
                    iid: f.iid,
                    kind: f.kind,
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
        s.put(1, &post_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
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
    async fn worker_defers_while_dormant_then_drains_on_reconnect_nudge() {
        let (s, _td) = store();
        s.put(1, &post_task(7, 100)).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dead_letter = KvStore::open_durable(&test_db(&dir), DEAD_LETTER_KEYSPACE).unwrap();

        // Start dormant so the task can't run yet. `session_wait` is set to an
        // hour so the defer branch's timeout can't possibly fire during the test:
        // the only thing that can wake the worker in time is the reconnect nudge,
        // so if the nudge regresses this test hangs and the `timeout` below fails.
        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(ConnState::Dormant(
            DormancyReason::NoCredentials,
        )));
        let mut cfg = crate::config::defaults();
        cfg.queue.session_wait_secs = 3600;
        let config = Arc::new(std::sync::RwLock::new(cfg));
        let drain_wake = Arc::new(Notify::new());

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Ok(()));

        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(worker(
            session.clone(),
            s.clone(),
            dead_letter,
            rx,
            config,
            Arc::clone(&drain_wake),
        ));

        tx.send(QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
            },
            queued_at_secs: 100,
        })
        .await
        .unwrap();

        // Give the worker a beat to reach the defer branch, then "reconnect":
        // flip the session live and nudge the worker to drain immediately.
        tokio::time::sleep(Duration::from_millis(10)).await;
        *session.write().await = ConnState::Connected(Session {
            gitlab: gitlab.clone(),
            host: "test".into(),
            user_id: 0,
        });
        drain_wake.notify_one();
        drop(tx);

        // The worker must drain via the nudge, not the (1-hour) session_wait
        // timeout: bound the join so a lost wakeup fails the test instead of
        // hanging it. With the nudge working this completes in well under a ms.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("worker drained via the reconnect nudge, not the session_wait timeout")
            .unwrap();

        assert_eq!(
            gitlab
                .add_spent_time_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            snapshot_pending(&s).unwrap().is_empty(),
            "deferred task drained after reconnect"
        );
    }

    #[tokio::test]
    async fn worker_routes_post_time_with_issue_id_to_create_timelog() {
        let (s, _td) = store();
        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: Some("note".into()),
                issuable_id: Some(999),
            },
            queued_at_secs: 100,
        };
        s.put(
            1,
            &StoredTask {
                project_id: 7,
                iid: 7,
                kind: Issuable::Issue,
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
        s.put(1, &post_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Err(Error::Gitlab("403".into())));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
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
    async fn worker_close_success() {
        let (s, _td) = store();
        s.put(1, &close_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_close(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::Close,
            queued_at_secs: 100,
        };
        run_worker_one_task(gitlab.clone(), s.clone(), task).await;

        assert_eq!(
            gitlab.close_calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    /// The worker must thread a task's `MergeRequest` kind into every GitLab
    /// call — a dropped kind would silently act on the wrong resource class.
    #[tokio::test]
    async fn worker_passes_mr_kind_through_to_gitlab_calls() {
        let (s, _td) = store();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_close(Ok(()));
        gitlab.push_add_spent_time(Ok(()));
        gitlab.create_timelog.lock().unwrap().push_back(Ok(()));

        for (id, op) in [
            (1u64, QueueOp::Close),
            (
                2,
                QueueOp::PostTime {
                    duration: "1h".into(),
                    summary: None,
                    issuable_id: None,
                },
            ),
            (
                3,
                QueueOp::PostTime {
                    duration: "1h".into(),
                    summary: None,
                    issuable_id: Some(999),
                },
            ),
        ] {
            let task = QueuedTask {
                id,
                project_id: 7,
                iid: 42,
                kind: Issuable::MergeRequest,
                op,
                queued_at_secs: 100,
            };
            run_worker_one_task(gitlab.clone(), s.clone(), task).await;
        }

        assert_eq!(
            *gitlab.kinds.lock().unwrap(),
            vec![
                ("close", Issuable::MergeRequest),
                ("add_spent_time", Issuable::MergeRequest),
                ("create_timelog", Issuable::MergeRequest),
            ]
        );
    }

    #[tokio::test]
    async fn worker_assign_self_success() {
        let (s, _td) = store();
        s.put(
            1,
            &StoredTask {
                project_id: 7,
                iid: 7,
                kind: Issuable::Issue,
                op: QueueOp::AssignSelf,
                queued_at_secs: 100,
            },
        )
        .unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_assign_self(Ok(()));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
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
            iid: id,
            kind: Issuable::Issue,
            op: QueueOp::Close,
            queued_at_secs: 100,
            failed_at_secs: 200,
            error: "boom".into(),
        }
    }

    fn retry_queue() -> (RetryQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        // Dormant session: the worker defers every task (30s sleep) instead of
        // processing it, so re-enqueued tasks stay put for assertions.
        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(ConnState::Dormant(
            DormancyReason::NoCredentials,
        )));
        let config = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
        let q = RetryQueue::new(session, &test_db(&dir), config).unwrap();
        (q, dir)
    }

    #[tokio::test]
    async fn worker_dead_letters_on_permanent_error() {
        let (s, _td) = store();
        s.put(1, &post_task(7, 100)).unwrap();

        let gitlab = Arc::new(FakeGitlab::default());
        gitlab.push_add_spent_time(Err(Error::Gitlab("403 Forbidden".into())));

        let task = QueuedTask {
            id: 1,
            project_id: 7,
            iid: 7,
            kind: Issuable::Issue,
            op: QueueOp::PostTime {
                duration: "1h".into(),
                summary: None,
                issuable_id: None,
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
        assert_eq!(failures[0].kind, Issuable::Issue);
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
                &StoredFailure {
                    project_id: 7,
                    iid: 9,
                    kind: Issuable::Issue,
                    op: QueueOp::Close,
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
            .scan(|_, t| Ok((t.project_id, t.iid, t.queued_at_secs)))
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
        q.dead_letter.put(1, &fail_entry(1)).unwrap();
        q.dead_letter.put(2, &fail_entry(2)).unwrap();

        assert!(q.dismiss_failure(1).unwrap(), "known id dismissed");
        assert!(!q.dismiss_failure(1).unwrap(), "already gone → false");
        assert_eq!(q.failures().unwrap().len(), 1);

        q.clear_failures().unwrap();
        assert!(q.failures().unwrap().is_empty(), "all cleared");
    }

    #[tokio::test]
    async fn new_seeds_next_id_above_both_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db = test_db(&dir);
        let store: KvStore<u64, StoredTask> = KvStore::open_durable(&db, QUEUE_KEYSPACE).unwrap();
        store.put(3, &post_task(1, 100)).unwrap();
        let dl: KvStore<u64, StoredFailure> =
            KvStore::open_durable(&db, DEAD_LETTER_KEYSPACE).unwrap();
        dl.put(10, &fail_entry(10)).unwrap();

        let session: SessionSlot = Arc::new(tokio::sync::RwLock::new(ConnState::Dormant(
            DormancyReason::NoCredentials,
        )));
        let config = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
        let q = RetryQueue::new(session, &db, config).unwrap();
        assert_eq!(
            q.next_id.load(std::sync::atomic::Ordering::Relaxed),
            11,
            "max(queue=3, dead-letter=10) + 1"
        );
    }
}
