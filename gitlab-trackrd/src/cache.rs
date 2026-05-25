//! redb-backed cache for the assigned-issues list.
//!
//! Hides redb behind a small [`IssueCache`] interface; callers see plain
//! `Result<Option<_>>` / `Result<()>` and never touch a transaction.

use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use gitlab_trackr_api::Issue;

/// redb table that stores the serialised [`CachedData`] blob under `"assigned"`.
const ISSUES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("issues_cache");

/// On-disk representation of a cached issue list, stored as JSON in redb.
#[derive(Serialize, Deserialize)]
struct CachedData {
    /// Unix timestamp (seconds) when this entry was written.
    timestamp: u64,
    issues: Vec<Issue>,
}

pub struct IssueCache {
    db: Mutex<Database>,
    ttl_secs: u64,
}

impl IssueCache {
    /// Open (or create) the cache database at `path`, ensuring the table exists.
    pub fn open(path: &Path, ttl_secs: u64) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or(Error::Cache("db path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        let db = Database::create(path)?;
        // Materialise the table so later read transactions don't error on a
        // fresh database.
        {
            let txn = db.begin_write()?;
            txn.open_table(ISSUES_TABLE)?;
            txn.commit()?;
        }
        Ok(Self {
            db: Mutex::new(db),
            ttl_secs,
        })
    }

    /// Return a fresh-enough cached issue list, or `None` if absent/stale.
    pub fn get(&self) -> Result<Option<Vec<Issue>>> {
        let db = self.db.lock().map_err(|_| Error::CachePoisoned)?;
        let txn = db.begin_read()?;
        let table = txn.open_table(ISSUES_TABLE)?;
        let Some(guard) = table.get("assigned")? else {
            return Ok(None);
        };
        let data: CachedData = serde_json::from_slice(guard.value())?;
        Ok((now_secs().saturating_sub(data.timestamp) < self.ttl_secs).then_some(data.issues))
    }

    /// Replace the cached issue list with `issues`, stamped at the current time.
    pub fn put(&self, issues: &[Issue]) -> Result<()> {
        let bytes = serde_json::to_vec(&CachedData {
            timestamp: now_secs(),
            issues: issues.to_vec(),
        })?;
        let db = self.db.lock().map_err(|_| Error::CachePoisoned)?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(ISSUES_TABLE)?;
            table.insert("assigned", bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drop the cached entry so the next `get` returns `None`.
    pub fn clear(&self) -> Result<()> {
        let db = self.db.lock().map_err(|_| Error::CachePoisoned)?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(ISSUES_TABLE)?;
            table.remove("assigned")?;
        }
        txn.commit()?;
        Ok(())
    }
}

/// Current unix time in seconds; clock skew before the epoch collapses to 0
/// so cache-age arithmetic saturates rather than panics.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
