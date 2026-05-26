//! redb-backed cache for the assigned-issues list.
//!
//! Hides `KvStore` behind a small [`IssueCache`] interface; callers see plain
//! `Result<Option<_>>` / `Result<()>` and never touch a transaction.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;
use crate::impl_redb_json_value;
use gitlab_trackr_api::Issue;

/// On-disk representation of a cached issue list, stored as JSON in redb.
#[derive(Debug, Serialize, Deserialize)]
struct CachedData {
    /// Unix timestamp (seconds) when this entry was written.
    timestamp: u64,
    issues: Vec<Issue>,
}

impl_redb_json_value!(CachedData, "CachedData");

const ISSUES_TABLE: TableDefinition<&str, CachedData> =
    TableDefinition::new("issues_cache");

pub struct IssueCache {
    store: KvStore<&'static str, CachedData>,
    ttl_secs: u64,
}

impl IssueCache {
    /// Open (or create) the cache database at `path`.
    pub fn open(path: &Path, ttl_secs: u64) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(path, ISSUES_TABLE)?,
            ttl_secs,
        })
    }

    /// Return a fresh-enough cached issue list, or `None` if absent/stale.
    pub fn get(&self) -> Result<Option<Vec<Issue>>> {
        let Some(data) = self.store.get("assigned")? else {
            return Ok(None);
        };
        Ok((now_secs().saturating_sub(data.timestamp) < self.ttl_secs).then_some(data.issues))
    }

    /// Replace the cached issue list with `issues`, stamped at the current time.
    pub fn put(&self, issues: &[Issue]) -> Result<()> {
        self.store.put(
            "assigned",
            CachedData {
                timestamp: now_secs(),
                issues: issues.to_vec(),
            },
        )
    }

    /// Drop the cached entry so the next `get` returns `None`.
    pub fn clear(&self) -> Result<()> {
        self.store.remove("assigned")
    }
}

/// Current unix time in seconds; clock skew before the epoch collapses to 0.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
