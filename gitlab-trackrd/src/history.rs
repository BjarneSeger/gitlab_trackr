//! redb-backed store for the tiered timelog history.
//!
//! Keyed by the GitLab Timelog ID so repeated polls dedupe naturally. The store
//! holds a single set of entries spanning up to [`STALE_WINDOW`]; the handler
//! refresh cycle re-polls different `spent_at` bands at different cadences
//! ([`ACTIVE_WINDOW`] every few minutes, [`SEMI_WINDOW`] once a day, the rest
//! fetched once at startup). [`HistoryCache::upsert`] writes whatever a poll
//! returned and [`HistoryCache::prune`] drops anything past the stale window.

use std::path::Path;
use std::time::Duration;

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;
use crate::impl_redb_json_value;

/// Active tier: the most volatile band, re-polled on the fast refresh interval.
pub const ACTIVE_WINDOW: Duration = Duration::from_hours(24);
/// Semi-active tier: re-polled once a day. Spans back to here from `now`.
pub const SEMI_WINDOW: Duration = Duration::from_hours(24 * 30);
/// Stale tier / overall retention: fetched once at startup, never re-polled.
/// Entries older than this are pruned.
pub const STALE_WINDOW: Duration = Duration::from_hours(24 * 90);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTimelog {
    pub timelog_id: u64,
    pub spent_at_secs: u64,
    pub project_id: i64,
    pub issue_iid: i64,
    pub issue_title: String,
    pub web_url: String,
    pub duration: String,
    pub summary: String,
}

impl_redb_json_value!(StoredTimelog, "StoredTimelog");

const HISTORY_TABLE: TableDefinition<u64, StoredTimelog> =
    TableDefinition::new("timelog_history");

pub struct HistoryCache {
    store: KvStore<u64, StoredTimelog>,
}

impl HistoryCache {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(path, HISTORY_TABLE)?,
        })
    }

    /// Insert or overwrite each event keyed by its `timelog_id`.
    pub fn upsert(&self, events: &[StoredTimelog]) -> Result<()> {
        for event in events {
            self.store.put(event.timelog_id, event.clone())?;
        }
        Ok(())
    }

    /// All stored entries with `spent_at_secs >= cutoff`, newest first.
    pub fn all_since(&self, cutoff_secs: u64) -> Result<Vec<StoredTimelog>> {
        let mut entries = self
            .store
            .scan(|_, v| Ok(v))?
            .into_iter()
            .filter(|e| e.spent_at_secs >= cutoff_secs)
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| std::cmp::Reverse(e.spent_at_secs));
        Ok(entries)
    }

    /// Drop every entry whose `spent_at_secs` is older than `cutoff_secs`.
    /// Returns the number of removed entries.
    pub fn prune(&self, cutoff_secs: u64) -> Result<usize> {
        self.clear_between(0, cutoff_secs)
    }

    /// Remove every entry whose `spent_at_secs` falls in `[min_secs, max_secs)`.
    /// Returns the number of removed entries. Used to clear a single tier:
    /// active is `[now-24h, u64::MAX)`, semi `[now-30d, now-24h)`, stale
    /// `[0, now-30d)`.
    pub fn clear_between(&self, min_secs: u64, max_secs: u64) -> Result<usize> {
        let matched: Vec<u64> = self
            .store
            .scan(|k, v| {
                Ok((v.spent_at_secs >= min_secs && v.spent_at_secs < max_secs).then_some(k))
            })?
            .into_iter()
            .flatten()
            .collect();

        let count = matched.len();
        for id in matched {
            self.store.remove(id)?;
        }
        Ok(count)
    }

    /// Drop every stored entry across all tiers.
    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(timelog_id: u64, spent_at: u64, title: &str) -> StoredTimelog {
        StoredTimelog {
            timelog_id,
            spent_at_secs: spent_at,
            project_id: 1,
            issue_iid: 1,
            issue_title: title.to_string(),
            web_url: "https://gl/-/issues/1".to_string(),
            duration: "1h".to_string(),
            summary: String::new(),
        }
    }

    fn cache() -> (HistoryCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.redb");
        (HistoryCache::open(&path).unwrap(), dir)
    }

    #[test]
    fn upsert_dedupes_by_timelog_id() {
        let (h, _td) = cache();
        h.upsert(&[entry(1, 100, "old")]).unwrap();
        h.upsert(&[entry(1, 100, "new")]).unwrap();
        let all = h.all_since(0).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].issue_title, "new");
    }

    #[test]
    fn all_since_filters_and_sorts() {
        let (h, _td) = cache();
        h.upsert(&[
            entry(1, 100, "old"),
            entry(2, 200, "mid"),
            entry(3, 300, "new"),
            entry(4, 50, "stale"),
        ])
        .unwrap();

        let recent = h.all_since(100).unwrap();
        let ids: Vec<u64> = recent.iter().map(|e| e.timelog_id).collect();
        assert_eq!(ids, vec![3, 2, 1], "newest first, below-cutoff removed");
    }

    #[test]
    fn prune_removes_only_stale_entries() {
        let (h, _td) = cache();
        h.upsert(&[
            entry(1, 50, "stale"),
            entry(2, 200, "fresh"),
            entry(3, 99, "barely-stale"),
        ])
        .unwrap();

        let removed = h.prune(100).unwrap();
        assert_eq!(removed, 2, "two entries below cutoff");

        let remaining = h.all_since(0).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].timelog_id, 2);
    }

    #[test]
    fn prune_on_empty_store_returns_zero() {
        let (h, _td) = cache();
        assert_eq!(h.prune(100).unwrap(), 0);
    }

    #[test]
    fn clear_between_is_half_open() {
        let (h, _td) = cache();
        h.upsert(&[
            entry(1, 100, "below"),
            entry(2, 200, "min-inclusive"),
            entry(3, 250, "inside"),
            entry(4, 300, "max-exclusive"),
            entry(5, 400, "above"),
        ])
        .unwrap();

        let removed = h.clear_between(200, 300).unwrap();
        assert_eq!(removed, 2, "200 and 250 fall in [200, 300)");

        let remaining: Vec<u64> = h.all_since(0).unwrap().iter().map(|e| e.timelog_id).collect();
        assert_eq!(remaining, vec![5, 4, 1], "100, 300 and 400 survive");
    }

    #[test]
    fn clear_between_open_top_clears_active_band() {
        let (h, _td) = cache();
        h.upsert(&[entry(1, 100, "old"), entry(2, 500, "recent")])
            .unwrap();
        assert_eq!(h.clear_between(200, u64::MAX).unwrap(), 1);
        let remaining = h.all_since(0).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].timelog_id, 1);
    }

    #[test]
    fn clear_empties_every_tier() {
        let (h, _td) = cache();
        h.upsert(&[entry(1, 100, "a"), entry(2, 999_999, "b")])
            .unwrap();
        h.clear().unwrap();
        assert!(h.all_since(0).unwrap().is_empty());
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.redb");
        {
            let h = HistoryCache::open(&path).unwrap();
            h.upsert(&[entry(1, 100, "persisted")]).unwrap();
        }
        let h = HistoryCache::open(&path).unwrap();
        let all = h.all_since(0).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].issue_title, "persisted");
    }
}
