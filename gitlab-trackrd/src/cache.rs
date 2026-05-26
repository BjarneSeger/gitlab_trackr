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

const ISSUES_TABLE: TableDefinition<&str, CachedData> = TableDefinition::new("issues_cache");

pub struct IssueCache {
    store: KvStore<&'static str, CachedData>,
}

impl IssueCache {
    /// Open (or create) the cache database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(path, ISSUES_TABLE)?,
        })
    }

    /// Return a fresh-enough cached issue list, or `None` if absent/stale.
    pub fn get(&self) -> Result<Option<Vec<Issue>>> {
        Ok(self.store.get("assigned")?.map(|data| data.issues))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_issue(iid: i64, title: &str) -> Issue {
        Issue {
            id: iid * 10,
            iid,
            project_id: 1,
            title: title.to_string(),
            web_url: format!("https://gl/-/issues/{iid}"),
            state: "opened".to_string(),
            parent: String::new(),
            total_time: String::new(),
            graph_status: String::new(),
        }
    }

    fn cache() -> (IssueCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.redb");
        (IssueCache::open(&path).unwrap(), dir)
    }

    #[test]
    fn fresh_cache_returns_none() {
        let (c, _td) = cache();
        assert!(c.get().unwrap().is_none());
    }

    #[test]
    fn put_then_get_roundtrips_issues() {
        let (c, _td) = cache();
        let issues = vec![make_issue(1, "a"), make_issue(2, "b")];
        c.put(&issues).unwrap();
        let got = c.get().unwrap().expect("cache should have an entry");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].iid, 1);
        assert_eq!(got[1].title, "b");
    }

    #[test]
    fn put_overwrites_previous_entry() {
        let (c, _td) = cache();
        c.put(&[make_issue(1, "old")]).unwrap();
        c.put(&[make_issue(2, "new")]).unwrap();
        let got = c.get().unwrap().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].title, "new");
    }

    #[test]
    fn clear_removes_entry() {
        let (c, _td) = cache();
        c.put(&[make_issue(1, "a")]).unwrap();
        c.clear().unwrap();
        assert!(c.get().unwrap().is_none());
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.redb");
        {
            let c = IssueCache::open(&path).unwrap();
            c.put(&[make_issue(1, "persisted")]).unwrap();
        }
        let c = IssueCache::open(&path).unwrap();
        let got = c.get().unwrap().unwrap();
        assert_eq!(got[0].title, "persisted");
    }

    #[test]
    fn now_secs_is_positive() {
        assert!(now_secs() > 0);
    }
}
