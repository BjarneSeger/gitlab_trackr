//! fjall-backed store for the search corpus: issues, merge requests,
//! projects, and groups, each kept as an individual entry keyed by its
//! global GitLab id so incremental re-polls dedupe naturally.
//!
//! Freshness is owned by the background sync (`handlers/search_sync`), which
//! gates itself on the cache-global [`SyncStamps`]: an incremental
//! `updated_after` poll advances `last_partial_sync_secs`, a periodic full
//! resync (which also reconciles deletions via [`SearchCache::retain_issues`]
//! and friends) advances both stamps. Zeroed stamps mean "never synced", so
//! [`SearchCache::clear`] wiping them makes the next sync a full one.
//!
//! Also home to the pure text matchers the `Search` handler uses, kept here so
//! they are unit-testable without handler scaffolding.

use std::collections::HashSet;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchIssue {
    pub id: i64,
    pub iid: i64,
    pub project_id: i64,
    pub title: String,
    pub web_url: String,
    pub state: String,
    pub labels: Vec<String>,
    /// Epic URL, empty when the issue has no parent.
    pub parent: String,
    /// Human-readable `total_time_spent`, e.g. `"1h 30m"`.
    pub total_time: String,
    pub updated_at_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMr {
    pub id: i64,
    pub iid: i64,
    pub project_id: i64,
    pub title: String,
    pub web_url: String,
    pub state: String,
    pub labels: Vec<String>,
    pub updated_at_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchProject {
    pub id: i64,
    pub name: String,
    /// `path_with_namespace`, e.g. `"team/backend/api"`.
    pub path: String,
    pub web_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchGroup {
    pub id: i64,
    pub name: String,
    /// `full_path`, e.g. `"team/backend"`.
    pub path: String,
    pub web_url: String,
}

/// Cache-global sync bookkeeping. `0` means "never" — both for a fresh cache
/// and after [`SearchCache::clear`] — which forces the next sync to be full.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SyncStamps {
    pub last_partial_sync_secs: u64,
    pub last_full_sync_secs: u64,
}

const SEARCH_ISSUES_KEYSPACE: &str = "search_issues_v1";
const SEARCH_MRS_KEYSPACE: &str = "search_mrs_v1";
const SEARCH_PROJECTS_KEYSPACE: &str = "search_projects_v1";
const SEARCH_GROUPS_KEYSPACE: &str = "search_groups_v1";
const SEARCH_META_KEYSPACE: &str = "search_meta_v1";
const STAMPS_KEY: &str = "stamps";

/// Entry types keyed by their global GitLab id.
trait HasId {
    fn id(&self) -> i64;
}

macro_rules! impl_has_id {
    ($($t:ty),*) => {$(
        impl HasId for $t {
            fn id(&self) -> i64 {
                self.id
            }
        }
    )*};
}
impl_has_id!(SearchIssue, SearchMr, SearchProject, SearchGroup);

pub struct SearchCache {
    issues: KvStore<u64, SearchIssue>,
    mrs: KvStore<u64, SearchMr>,
    projects: KvStore<u64, SearchProject>,
    groups: KvStore<u64, SearchGroup>,
    meta: KvStore<&'static str, SyncStamps>,
}

impl SearchCache {
    pub fn open(db: &fjall::Database) -> Result<Self> {
        Ok(Self {
            issues: KvStore::open(db, SEARCH_ISSUES_KEYSPACE)?,
            mrs: KvStore::open(db, SEARCH_MRS_KEYSPACE)?,
            projects: KvStore::open(db, SEARCH_PROJECTS_KEYSPACE)?,
            groups: KvStore::open(db, SEARCH_GROUPS_KEYSPACE)?,
            meta: KvStore::open(db, SEARCH_META_KEYSPACE)?,
        })
    }

    pub fn stamps(&self) -> Result<SyncStamps> {
        Ok(self.meta.get(STAMPS_KEY)?.unwrap_or_default())
    }

    pub fn set_stamps(&self, stamps: &SyncStamps) -> Result<()> {
        self.meta.put(STAMPS_KEY, stamps)
    }

    pub fn upsert_issues(&self, items: &[SearchIssue]) -> Result<()> {
        upsert(&self.issues, items)
    }

    pub fn upsert_mrs(&self, items: &[SearchMr]) -> Result<()> {
        upsert(&self.mrs, items)
    }

    pub fn upsert_projects(&self, items: &[SearchProject]) -> Result<()> {
        upsert(&self.projects, items)
    }

    pub fn upsert_groups(&self, items: &[SearchGroup]) -> Result<()> {
        upsert(&self.groups, items)
    }

    pub fn all_issues(&self) -> Result<Vec<SearchIssue>> {
        self.issues.scan(|_, v| Ok(v))
    }

    pub fn all_mrs(&self) -> Result<Vec<SearchMr>> {
        self.mrs.scan(|_, v| Ok(v))
    }

    pub fn all_projects(&self) -> Result<Vec<SearchProject>> {
        self.projects.scan(|_, v| Ok(v))
    }

    pub fn all_groups(&self) -> Result<Vec<SearchGroup>> {
        self.groups.scan(|_, v| Ok(v))
    }

    pub fn retain_issues(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.issues, keep)
    }

    pub fn retain_mrs(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.mrs, keep)
    }

    pub fn retain_projects(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.projects, keep)
    }

    pub fn retain_groups(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.groups, keep)
    }

    /// Drop every entry of every kind *and* the sync stamps, so the next sync
    /// runs full.
    pub fn clear(&self) -> Result<()> {
        self.issues.clear()?;
        self.mrs.clear()?;
        self.projects.clear()?;
        self.groups.clear()?;
        self.meta.clear()
    }
}

/// Insert or overwrite each entry keyed by its global id.
fn upsert<T: HasId + Serialize + DeserializeOwned>(
    store: &KvStore<u64, T>,
    items: &[T],
) -> Result<()> {
    for item in items {
        store.put(item.id() as u64, item)?;
    }
    Ok(())
}

/// Remove every entry whose key is not in `keep` — the deletion half of a
/// full resync. Returns the number of removed entries.
fn retain<T: Serialize + DeserializeOwned>(
    store: &KvStore<u64, T>,
    keep: &HashSet<u64>,
) -> Result<usize> {
    let stale: Vec<u64> = store
        .scan(|k, _| Ok((!keep.contains(&k)).then_some(k)))?
        .into_iter()
        .flatten()
        .collect();
    let count = stale.len();
    for key in stale {
        store.remove(key)?;
    }
    Ok(count)
}

/// Parse an issue/MR-reference query: `"#123"` → `Some(123)`. Anything else —
/// no leading `#`, non-digits, empty — is not a reference query.
pub fn parse_iid_query(query: &str) -> Option<i64> {
    let digits = query.trim().strip_prefix('#')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Case-insensitive substring match. The needle must already be lowercased —
/// callers lowercase the query once, not per entry.
pub fn text_matches(needle_lower: &str, hay: &str) -> bool {
    hay.to_lowercase().contains(needle_lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(id: i64, title: &str) -> SearchIssue {
        SearchIssue {
            id,
            iid: id * 10,
            project_id: 1,
            title: title.to_string(),
            web_url: format!("https://gl/p/-/issues/{id}"),
            state: "opened".to_string(),
            labels: vec!["bug".to_string()],
            parent: String::new(),
            total_time: String::new(),
            updated_at_secs: 100,
        }
    }

    fn mr(id: i64, title: &str) -> SearchMr {
        SearchMr {
            id,
            iid: id * 10,
            project_id: 1,
            title: title.to_string(),
            web_url: format!("https://gl/p/-/merge_requests/{id}"),
            state: "opened".to_string(),
            labels: vec![],
            updated_at_secs: 100,
        }
    }

    fn project(id: i64, path: &str) -> SearchProject {
        SearchProject {
            id,
            name: path.rsplit('/').next().unwrap().to_string(),
            path: path.to_string(),
            web_url: format!("https://gl/{path}"),
        }
    }

    fn group(id: i64, path: &str) -> SearchGroup {
        SearchGroup {
            id,
            name: path.rsplit('/').next().unwrap().to_string(),
            path: path.to_string(),
            web_url: format!("https://gl/{path}"),
        }
    }

    fn cache() -> (SearchCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap();
        (SearchCache::open(&db).unwrap(), dir)
    }

    #[test]
    fn upsert_dedupes_by_id() {
        let (c, _td) = cache();
        c.upsert_issues(&[issue(1, "old")]).unwrap();
        c.upsert_issues(&[issue(1, "new")]).unwrap();
        let all = c.all_issues().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].title, "new");
    }

    #[test]
    fn each_kind_roundtrips_independently() {
        let (c, _td) = cache();
        c.upsert_issues(&[issue(1, "i")]).unwrap();
        c.upsert_mrs(&[mr(1, "m")]).unwrap();
        c.upsert_projects(&[project(1, "team/p")]).unwrap();
        c.upsert_groups(&[group(1, "team")]).unwrap();

        assert_eq!(c.all_issues().unwrap()[0].title, "i");
        assert_eq!(c.all_mrs().unwrap()[0].title, "m");
        assert_eq!(c.all_projects().unwrap()[0].path, "team/p");
        assert_eq!(c.all_groups().unwrap()[0].path, "team");
    }

    #[test]
    fn retain_removes_exactly_the_missing_keys() {
        let (c, _td) = cache();
        c.upsert_issues(&[issue(1, "keep"), issue(2, "drop"), issue(3, "keep")])
            .unwrap();

        let removed = c.retain_issues(&HashSet::from([1, 3])).unwrap();
        assert_eq!(removed, 1);

        let mut ids: Vec<i64> = c.all_issues().unwrap().iter().map(|i| i.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn retain_with_empty_keep_empties_the_kind() {
        let (c, _td) = cache();
        c.upsert_mrs(&[mr(1, "a"), mr(2, "b")]).unwrap();
        assert_eq!(c.retain_mrs(&HashSet::new()).unwrap(), 2);
        assert!(c.all_mrs().unwrap().is_empty());
    }

    #[test]
    fn stamps_default_to_never_synced() {
        let (c, _td) = cache();
        let s = c.stamps().unwrap();
        assert_eq!(s.last_partial_sync_secs, 0);
        assert_eq!(s.last_full_sync_secs, 0);
    }

    #[test]
    fn stamps_roundtrip() {
        let (c, _td) = cache();
        c.set_stamps(&SyncStamps {
            last_partial_sync_secs: 123,
            last_full_sync_secs: 45,
        })
        .unwrap();
        let s = c.stamps().unwrap();
        assert_eq!(s.last_partial_sync_secs, 123);
        assert_eq!(s.last_full_sync_secs, 45);
    }

    #[test]
    fn clear_wipes_all_kinds_and_resets_stamps() {
        let (c, _td) = cache();
        c.upsert_issues(&[issue(1, "i")]).unwrap();
        c.upsert_mrs(&[mr(1, "m")]).unwrap();
        c.upsert_projects(&[project(1, "team/p")]).unwrap();
        c.upsert_groups(&[group(1, "team")]).unwrap();
        c.set_stamps(&SyncStamps {
            last_partial_sync_secs: 1,
            last_full_sync_secs: 1,
        })
        .unwrap();

        c.clear().unwrap();

        assert!(c.all_issues().unwrap().is_empty());
        assert!(c.all_mrs().unwrap().is_empty());
        assert!(c.all_projects().unwrap().is_empty());
        assert!(c.all_groups().unwrap().is_empty());
        assert_eq!(c.stamps().unwrap().last_partial_sync_secs, 0);
        assert_eq!(c.stamps().unwrap().last_full_sync_secs, 0);
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // Both the store and the Database must drop before reopening — fjall
        // holds a single-process lock on the directory.
        {
            let db = fjall::Database::builder(&path).open().unwrap();
            let c = SearchCache::open(&db).unwrap();
            c.upsert_issues(&[issue(1, "persisted")]).unwrap();
            c.set_stamps(&SyncStamps {
                last_partial_sync_secs: 7,
                last_full_sync_secs: 7,
            })
            .unwrap();
        }
        let db = fjall::Database::builder(&path).open().unwrap();
        let c = SearchCache::open(&db).unwrap();
        assert_eq!(c.all_issues().unwrap()[0].title, "persisted");
        assert_eq!(c.stamps().unwrap().last_full_sync_secs, 7);
    }

    #[test]
    fn parse_iid_query_accepts_only_hash_number() {
        assert_eq!(parse_iid_query("#123"), Some(123));
        assert_eq!(parse_iid_query("  #7 "), Some(7));
        assert_eq!(parse_iid_query("123"), None);
        assert_eq!(parse_iid_query("#12x"), None);
        assert_eq!(parse_iid_query("#"), None);
        assert_eq!(parse_iid_query(""), None);
        assert_eq!(parse_iid_query("#-1"), None);
    }

    #[test]
    fn text_matches_is_case_insensitive() {
        assert!(text_matches("auth", "OAuth token refresh"));
        assert!(text_matches("team/back", "Team/Backend"));
        assert!(!text_matches("missing", "nothing here"));
    }
}
