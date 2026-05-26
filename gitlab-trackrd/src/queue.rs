//! Background retry queue for outgoing write operations.
//!
//! Tasks are persisted to a redb database before being processed, so they
//! survive daemon restarts. A background tokio task works through the queue
//! with exponential backoff (1 s base, 30 min cap). Network errors trigger
//! retries for up to 7 days; GitLab rejections drop the task immediately.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::error::{Error, Result};
use crate::gitlab::GitlabClient;

const QUEUE_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("post_time_queue");

const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_mins(30);
const MAX_LIFETIME: Duration = Duration::from_hours(168);

#[derive(Serialize, Deserialize)]
struct StoredTask {
    project_id: i64,
    issue_iid: i64,
    duration: String,
    summary: Option<String>,
    /// UNIX timestamp (seconds) when the task was first enqueued.
    queued_at_secs: u64,
}

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
    db: Arc<Database>,
    next_id: AtomicU64,
}

impl RetryQueue {
    /// Open (or create) the queue database at `db_path`, reload any tasks that
    /// survived a previous restart, and spawn the background worker.
    pub fn new(gitlab: Arc<GitlabClient>, db_path: &Path) -> Result<Self> {
        let parent = db_path
            .parent()
            .ok_or(Error::Cache("queue db path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;

        let db = Database::create(db_path)?;
        {
            let txn = db.begin_write()?;
            txn.open_table(QUEUE_TABLE)?;
            txn.commit()?;
        }

        // Load tasks that survived a previous daemon run.
        let mut initial_tasks: Vec<QueuedTask> = Vec::new();
        let mut max_id = 0u64;
        {
            let txn = db.begin_read()?;
            let table = txn.open_table(QUEUE_TABLE)?;
            for result in table.iter()? {
                let (k, v) = result?;
                let id = k.value();
                let stored: StoredTask = serde_json::from_slice(v.value())?;
                max_id = max_id.max(id);
                initial_tasks.push(QueuedTask {
                    id,
                    project_id: stored.project_id,
                    issue_iid: stored.issue_iid,
                    duration: stored.duration,
                    summary: stored.summary,
                    queued_at_secs: stored.queued_at_secs,
                });
            }
        }

        if !initial_tasks.is_empty() {
            info!(
                count = initial_tasks.len(),
                "reloaded pending PostTime tasks from queue database"
            );
        }

        let db = Arc::new(db);
        let (tx, rx) = mpsc::channel(256);

        // Feed reloaded tasks into the channel before the worker starts consuming.
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

        tokio::spawn(worker(gitlab, Arc::clone(&db), rx));

        Ok(Self {
            sender: tx,
            db,
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
        if let Err(e) = self.persist(id, &stored) {
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

    fn persist(&self, id: u64, stored: &StoredTask) -> Result<()> {
        let bytes = serde_json::to_vec(stored)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(QUEUE_TABLE)?;
            table.insert(id, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }
}

async fn worker(gitlab: Arc<GitlabClient>, db: Arc<Database>, mut rx: mpsc::Receiver<QueuedTask>) {
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

        if let Err(e) = remove_task(&db, task.id) {
            warn!(
                error = %e,
                task_id = task.id,
                "failed to remove completed PostTime task from queue db"
            );
        }
    }
}

fn remove_task(db: &Database, id: u64) -> Result<()> {
    let txn = db.begin_write()?;
    {
        let mut table = txn.open_table(QUEUE_TABLE)?;
        table.remove(id)?;
    }
    txn.commit()?;
    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
