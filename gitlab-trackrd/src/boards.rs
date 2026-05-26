//! redb-backed cache for project board-list labels.
//!
//! Keyed by `project_id`. No TTL — entries live until [`BoardCache::clear`]
//! wipes them (called from the `ClearCache` varlink method). The daemon
//! relies on its regular refresh cycle to keep this fresh enough.

use std::path::Path;

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;
use crate::impl_redb_json_value;

#[derive(Debug, Serialize, Deserialize)]
struct ProjectBoardLabels {
    labels: Vec<String>,
}

impl_redb_json_value!(ProjectBoardLabels, "ProjectBoardLabels");

const BOARDS_TABLE: TableDefinition<i64, ProjectBoardLabels> =
    TableDefinition::new("project_board_labels");

pub struct BoardCache {
    store: KvStore<i64, ProjectBoardLabels>,
}

impl BoardCache {
    /// Open (or create) the board-cache database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(path, BOARDS_TABLE)?,
        })
    }

    /// Return the cached label list for `project_id`, or `None` if absent.
    pub fn get(&self, project_id: i64) -> Result<Option<Vec<String>>> {
        Ok(self.store.get(project_id)?.map(|d| d.labels))
    }

    /// Insert or overwrite the label list for `project_id`.
    pub fn put(&self, project_id: i64, labels: Vec<String>) -> Result<()> {
        self.store.put(project_id, ProjectBoardLabels { labels })
    }

    /// Drop every entry so the next `get` for any project returns `None`.
    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }
}
