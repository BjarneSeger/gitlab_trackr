//! fjall-backed cache for project board-list labels.
//!
//! Keyed by `project_id`. No TTL — entries live until [`BoardCache::clear`]
//! wipes them (called from the `ClearCache` varlink method). The daemon
//! relies on its regular refresh cycle to keep this fresh enough.

use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;

#[derive(Debug, Serialize, Deserialize)]
struct ProjectBoardLabels {
    labels: Vec<String>,
}

const BOARDS_KEYSPACE: &str = "project_board_labels_v1";

pub struct BoardCache {
    store: KvStore<i64, ProjectBoardLabels>,
}

impl BoardCache {
    /// Open (or create) the board-cache keyspace in `db`.
    pub fn open(db: &fjall::Database) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(db, BOARDS_KEYSPACE)?,
        })
    }

    /// Return the cached label list for `project_id`, or `None` if absent.
    pub fn get(&self, project_id: i64) -> Result<Option<Vec<String>>> {
        Ok(self.store.get(project_id)?.map(|d| d.labels))
    }

    /// Insert or overwrite the label list for `project_id`.
    pub fn put(&self, project_id: i64, labels: Vec<String>) -> Result<()> {
        self.store.put(project_id, &ProjectBoardLabels { labels })
    }

    /// Drop every entry so the next `get` for any project returns `None`.
    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> (BoardCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap();
        (BoardCache::open(&db).unwrap(), dir)
    }

    #[test]
    fn fresh_cache_returns_none() {
        let (c, _td) = cache();
        assert!(c.get(42).unwrap().is_none());
    }

    #[test]
    fn put_then_get_roundtrips_per_project() {
        let (c, _td) = cache();
        c.put(1, vec!["Doing".into(), "Done".into()]).unwrap();
        c.put(2, vec!["Review".into()]).unwrap();
        assert_eq!(c.get(1).unwrap(), Some(vec!["Doing".into(), "Done".into()]));
        assert_eq!(c.get(2).unwrap(), Some(vec!["Review".into()]));
        assert!(c.get(3).unwrap().is_none());
    }

    #[test]
    fn put_overwrites_same_project() {
        let (c, _td) = cache();
        c.put(1, vec!["old".into()]).unwrap();
        c.put(1, vec!["new".into()]).unwrap();
        assert_eq!(c.get(1).unwrap(), Some(vec!["new".into()]));
    }

    #[test]
    fn clear_empties_all_projects() {
        let (c, _td) = cache();
        c.put(1, vec!["a".into()]).unwrap();
        c.put(2, vec!["b".into()]).unwrap();
        c.clear().unwrap();
        assert!(c.get(1).unwrap().is_none());
        assert!(c.get(2).unwrap().is_none());
    }
}
