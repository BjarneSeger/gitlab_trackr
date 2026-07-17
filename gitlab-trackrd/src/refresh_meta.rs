//! Persisted last-run bookkeeping for the issue/history refresh tiers.
//!
//! The quick and slow tiers in `handlers/refresh` gate themselves on
//! [`RefreshStamps`] the same way the search sync gates on
//! [`SyncStamps`](crate::search::SyncStamps): a stamp is advanced only after a
//! successful run, so a daemon restart inside a tier's interval serves the
//! persisted caches instead of re-polling GitLab, and a failed run leaves the
//! stamp untouched for the next tick to retry. Zeroed stamps mean "never
//! synced" — `ClearCache` zeroes them so its warm-up repopulates in full.

use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;

/// Refresh-tier bookkeeping. `0` means "never" — both for a fresh database and
/// after `ClearCache` — which makes the next run unconditionally due.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RefreshStamps {
    /// Last successful quick-tier refresh (issues, boards, recent history).
    pub last_quick_sync_secs: u64,
    /// Last successful slow-tier history pull — shared by the daily refresh
    /// and the startup backfill, whose windows overlap (upserts dedupe).
    pub last_slow_sync_secs: u64,
    /// Widest history window (hours) ever successfully backfilled, so a
    /// retention increase forces one re-backfill even while the slow stamp is
    /// fresh. `serde(default)` keeps stamps written before this field readable.
    #[serde(default)]
    pub backfilled_retention_hours: u64,
}

const REFRESH_META_KEYSPACE: &str = "refresh_meta_v1";
const STAMPS_KEY: &str = "stamps";

/// fjall-backed store for the single [`RefreshStamps`] record.
///
/// Unlike [`SearchCache`](crate::search::SearchCache) there is no sync gate:
/// the tiers only upsert (no deletion diffs to serialize), so a concurrent run
/// merely duplicates work. A `ClearCache` racing an in-flight refresh can
/// stamp over the just-zeroed record, but the racing refresh fetched moments
/// ago, so the data it stamps as fresh really is.
pub struct RefreshMeta {
    meta: KvStore<&'static str, RefreshStamps>,
    /// Serializes the read-modify-write in [`Self::update`] so the quick and
    /// slow tiers can't clobber each other's fields in the shared record.
    write_lock: std::sync::Mutex<()>,
}

impl RefreshMeta {
    /// Lazy durability — losing the newest stamp on power failure only costs
    /// one redundant refetch.
    pub fn open(db: &fjall::Database) -> Result<Self> {
        Ok(Self {
            meta: KvStore::open(db, REFRESH_META_KEYSPACE)?,
            write_lock: std::sync::Mutex::new(()),
        })
    }

    pub fn stamps(&self) -> Result<RefreshStamps> {
        Ok(self.meta.get(STAMPS_KEY)?.unwrap_or_default())
    }

    /// Read-modify-write the stamps record under the internal lock.
    pub fn update(&self, f: impl FnOnce(&mut RefreshStamps)) -> Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut stamps = self.meta.get(STAMPS_KEY)?.unwrap_or_default();
        f(&mut stamps);
        self.meta.put(STAMPS_KEY, &stamps)
    }

    /// Drop the stamps, so every tier's next run is due.
    pub fn clear(&self) -> Result<()> {
        self.meta.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> (RefreshMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap();
        (RefreshMeta::open(&db).unwrap(), dir)
    }

    #[test]
    fn stamps_default_to_never_synced() {
        let (m, _td) = meta();
        let s = m.stamps().unwrap();
        assert_eq!(s.last_quick_sync_secs, 0);
        assert_eq!(s.last_slow_sync_secs, 0);
        assert_eq!(s.backfilled_retention_hours, 0);
    }

    #[test]
    fn update_roundtrips_and_preserves_other_fields() {
        let (m, _td) = meta();
        m.update(|s| s.last_quick_sync_secs = 123).unwrap();
        m.update(|s| {
            s.last_slow_sync_secs = 45;
            s.backfilled_retention_hours = 2160;
        })
        .unwrap();
        let s = m.stamps().unwrap();
        assert_eq!(s.last_quick_sync_secs, 123, "untouched by second update");
        assert_eq!(s.last_slow_sync_secs, 45);
        assert_eq!(s.backfilled_retention_hours, 2160);
    }

    #[test]
    fn clear_resets_to_never_synced() {
        let (m, _td) = meta();
        m.update(|s| {
            s.last_quick_sync_secs = 1;
            s.last_slow_sync_secs = 1;
        })
        .unwrap();
        m.clear().unwrap();
        let s = m.stamps().unwrap();
        assert_eq!(s.last_quick_sync_secs, 0);
        assert_eq!(s.last_slow_sync_secs, 0);
    }

    #[test]
    fn stamps_without_backfill_field_still_parse() {
        // Stamps written before `backfilled_retention_hours` existed must stay
        // readable: the field defaults to zero, forcing one re-backfill.
        let s: RefreshStamps =
            serde_json::from_str(r#"{"last_quick_sync_secs": 7, "last_slow_sync_secs": 7}"#)
                .unwrap();
        assert_eq!(s.backfilled_retention_hours, 0);
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // Both the store and the Database must drop before reopening — fjall
        // holds a single-process lock on the directory.
        {
            let db = fjall::Database::builder(&path).open().unwrap();
            let m = RefreshMeta::open(&db).unwrap();
            m.update(|s| s.last_quick_sync_secs = 7).unwrap();
        }
        let db = fjall::Database::builder(&path).open().unwrap();
        let m = RefreshMeta::open(&db).unwrap();
        assert_eq!(m.stamps().unwrap().last_quick_sync_secs, 7);
    }
}
