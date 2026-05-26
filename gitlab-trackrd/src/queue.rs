//! Background retry queue for outgoing write operations.
//!
//! Tasks are persisted via `KvStore` before being processed, so they survive
//! daemon restarts.  A background tokio task works through the queue with
//! exponential backoff (1 s base, 30 min cap).  Network errors trigger
//! retries for up to 7 days; GitLab rejections drop the task immediately.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::db::KvStore;
use crate::error::{Error, Result};
use crate::gitlab::GitlabApi;
use crate::impl_redb_json_value;

// Table name bumped from `post_time_queue` so older `StoredTask` records that
// don't carry the new `op` field don't crash deserialization on startup.
const QUEUE_TABLE: TableDefinition<u64, StoredTask> = TableDefinition::new("retry_queue_v3");

const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_mins(30);
const MAX_LIFETIME: Duration = Duration::from_hours(168);

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
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredTask {
    project_id: i64,
    issue_iid: i64,
    op: QueueOp,
    /// UNIX timestamp (seconds) when the task was first enqueued.
    queued_at_secs: u64,
}

impl_redb_json_value!(StoredTask, "StoredTaskV3");

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

impl RetryQueue {
    /// Open (or create) the queue database at `db_path`, reload any tasks that
    /// survived a previous restart, and spawn the background worker.
    pub fn new(gitlab: Arc<dyn GitlabApi>, db_path: &Path) -> Result<Self> {
        let store = KvStore::open(db_path, QUEUE_TABLE)?;

        let initial_tasks = store.scan(|id, stored| {
            Ok(QueuedTask {
                id,
                project_id: stored.project_id,
                issue_iid: stored.issue_iid,
                op: stored.op,
                queued_at_secs: stored.queued_at_secs,
            })
        })?;

        let max_id = initial_tasks.iter().map(|t| t.id).max().unwrap_or(0);

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

        tokio::spawn(worker(gitlab, store.clone(), rx));

        Ok(Self {
            sender: tx,
            store,
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
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let queued_at_secs = now_secs();

        let stored = StoredTask {
            project_id,
            issue_iid,
            op: op.clone(),
            queued_at_secs,
        };
        if let Err(e) = self.store.put(id, stored) {
            warn!(
                error = %e,
                "failed to persist task to queue db; it will not survive a restart"
            );
        }

        let task = QueuedTask {
            id,
            project_id,
            issue_iid,
            op,
            queued_at_secs,
        };
        if self.sender.send(task).await.is_err() {
            error!("retry queue channel closed; dropping task");
        }
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
                QueueOp::CloseIssue => None,
            })
        })?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    out.sort_by_key(|p| std::cmp::Reverse(p.queued_at_secs));
    Ok(out)
}

async fn worker(
    gitlab: Arc<dyn GitlabApi>,
    store: KvStore<u64, StoredTask>,
    mut rx: mpsc::Receiver<QueuedTask>,
) {
    while let Some(task) = rx.recv().await {
        let mut delay = BASE_DELAY;
        let mut attempt = 0u32;

        'retry: loop {
            attempt += 1;
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
                    break 'retry;
                }
                Err(e) if matches!(e, Error::Transient(_)) => {
                    let elapsed =
                        Duration::from_secs(now_secs().saturating_sub(task.queued_at_secs));
                    if elapsed >= MAX_LIFETIME {
                        error!(
                            attempt,
                            error = %e,
                            project_id = task.project_id,
                            issue_iid = task.issue_iid,
                            op = task.op.kind(),
                            "dropping task after 7-day retry window"
                        );
                        break 'retry;
                    }
                    let sleep = delay.min(MAX_LIFETIME.checked_sub(elapsed).unwrap());
                    warn!(
                        attempt,
                        error = %e,
                        delay_secs = sleep.as_secs(),
                        project_id = task.project_id,
                        op = task.op.kind(),
                        "task network error, retrying"
                    );
                    tokio::time::sleep(sleep).await;
                    delay = (delay * 2).min(MAX_DELAY);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        project_id = task.project_id,
                        issue_iid = task.issue_iid,
                        op = task.op.kind(),
                        "task rejected by GitLab; dropping"
                    );
                    break 'retry;
                }
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
    use crate::gitlab::{FetchedTimelog, IssueWithLabels};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    // The 7-day MAX_LIFETIME cutoff and exponential backoff are not exercised
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
        add_spent_time_calls: AtomicUsize,
        create_timelog_calls: AtomicUsize,
        close_issue_calls: AtomicUsize,
    }

    impl FakeGitlab {
        fn push_add_spent_time(&self, r: TrackrResult<()>) {
            self.add_spent_time.lock().unwrap().push_back(r);
        }
        fn push_close_issue(&self, r: TrackrResult<()>) {
            self.close_issue.lock().unwrap().push_back(r);
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
        async fn fetch_group_issues(
            &self,
            _g: Vec<String>,
        ) -> TrackrResult<Vec<IssueWithLabels>> {
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
        async fn fetch_board_list_labels(&self, _p: i64) -> TrackrResult<Vec<String>> {
            unimplemented!()
        }
    }

    /// Spawn the worker with one task on the channel, close the sender so the
    /// worker exits after draining, and await its completion.
    async fn run_worker_one_task(
        gitlab: Arc<dyn GitlabApi>,
        store: KvStore<u64, StoredTask>,
        task: QueuedTask,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(worker(gitlab, store, rx));
        tx.send(task).await.unwrap();
        drop(tx);
        handle.await.unwrap();
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
}
