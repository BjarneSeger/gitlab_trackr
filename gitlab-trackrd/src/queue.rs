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
use crate::gitlab::GitlabClient;
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

impl RetryQueue {
    /// Open (or create) the queue database at `db_path`, reload any tasks that
    /// survived a previous restart, and spawn the background worker.
    pub fn new(gitlab: Arc<GitlabClient>, db_path: &Path) -> Result<Self> {
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

async fn worker(
    gitlab: Arc<GitlabClient>,
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
