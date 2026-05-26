//! Background retry queue for outgoing write operations.
//!
//! Tasks are persisted via `KvStore` before being processed, so they survive
//! daemon restarts.  A background tokio task works through the queue with
//! exponential backoff (1 s base, 30 min cap).  Network errors trigger
//! retries for up to 7 days; GitLab rejections drop the task immediately.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::path::Path;

use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::db::KvStore;
use crate::error::{Error, Result};
use crate::impl_redb_json_value;
use crate::gitlab::GitlabClient;

const QUEUE_TABLE: TableDefinition<u64, StoredTask> =
    TableDefinition::new("post_time_queue");

const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_mins(30);
const MAX_LIFETIME: Duration = Duration::from_hours(168);

#[derive(Debug, Serialize, Deserialize)]
struct StoredTask {
    project_id: i64,
    issue_iid: i64,
    duration: String,
    summary: Option<String>,
    /// UNIX timestamp (seconds) when the task was first enqueued.
    queued_at_secs: u64,
}

impl_redb_json_value!(StoredTask, "StoredTask");

struct QueuedTask {
    id: u64,
    project_id: i64,
    issue_iid: i64,
    duration: String,
    summary: Option<String>,
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
                duration: stored.duration,
                summary: stored.summary,
                queued_at_secs: stored.queued_at_secs,
            })
        })?;

        let max_id = initial_tasks.iter().map(|t| t.id).max().unwrap_or(0);

        if !initial_tasks.is_empty() {
            info!(
                count = initial_tasks.len(),
                "reloaded pending PostTime tasks from queue database"
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

    /// Persist `task` to disk and hand it to the background worker.
    /// Returns immediately; the caller does not wait for the network operation.
    pub async fn post_time(
        &self,
        project_id: i64,
        issue_iid: i64,
        duration: String,
        summary: Option<String>,
    ) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let queued_at_secs = now_secs();

        let stored = StoredTask {
            project_id,
            issue_iid,
            duration: duration.clone(),
            summary: summary.clone(),
            queued_at_secs,
        };
        if let Err(e) = self.store.put(id, stored) {
            warn!(
                error = %e,
                "failed to persist PostTime task to queue db; it will not survive a restart"
            );
        }

        let task = QueuedTask {
            id,
            project_id,
            issue_iid,
            duration,
            summary,
            queued_at_secs,
        };
        if self.sender.send(task).await.is_err() {
            error!("retry queue channel closed; dropping PostTime task");
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
            match gitlab
                .add_spent_time(
                    task.project_id,
                    task.issue_iid,
                    &task.duration,
                    task.summary.as_deref(),
                )
                .await
            {
                Ok(()) => {
                    if attempt > 1 {
                        info!(
                            attempt,
                            project_id = task.project_id,
                            issue_iid = task.issue_iid,
                            "PostTime succeeded after retry"
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
                            duration = %task.duration,
                            "PostTime dropping task after 7-day retry window"
                        );
                        break 'retry;
                    }
                    let sleep = delay.min(MAX_LIFETIME.checked_sub(elapsed).unwrap());
                    warn!(
                        attempt,
                        error = %e,
                        delay_secs = sleep.as_secs(),
                        project_id = task.project_id,
                        "PostTime network error, retrying"
                    );
                    tokio::time::sleep(sleep).await;
                    delay = (delay * 2).min(MAX_DELAY);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        project_id = task.project_id,
                        issue_iid = task.issue_iid,
                        duration = %task.duration,
                        "PostTime rejected by GitLab; dropping task"
                    );
                    break 'retry;
                }
            }
        }

        if let Err(e) = store.remove(task.id) {
            warn!(
                error = %e,
                task_id = task.id,
                "failed to remove completed PostTime task from queue db"
            );
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
