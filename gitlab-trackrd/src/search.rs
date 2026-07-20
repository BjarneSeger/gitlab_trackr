//! fjall-backed store for the search corpus: issues, merge requests,
//! projects, and groups, each kept as an individual entry keyed by its
//! global GitLab id so incremental re-polls dedupe naturally.
//!
//! Freshness is owned by the background sync (`handlers/search_sync`), which
//! gates itself on the cache-global [`SyncStamps`]: an incremental
//! `updated_after` poll advances `last_partial_sync_secs`, a periodic full
//! resync (which also reconciles deletions via [`SyncGuard::retain_issues`]
//! and friends) advances both stamps. Zeroed stamps mean "never synced", so
//! [`SyncGuard::clear`] wiping them makes the next sync a full one.
//!
//! Mutations are only reachable through a [`SyncGuard`], handed out by the
//! cache's internal sync gate — see [`SearchCache::try_begin_sync`] — so the
//! compiler enforces that every write path is serialized against the others.
//! Reads take no lock: fjall is thread-safe per operation, and readers are
//! pure cache readers that tolerate a mid-sync corpus.
//!
//! Also home to the pure text matchers the `Search` handler uses, kept here so
//! they are unit-testable without handler scaffolding.

use std::collections::HashSet;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};

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

/// One MR assignee, captured at sync time. The id drives the assigned-to-me
/// filter; the username is what the wire `MergeRequest` exposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MrAssignee {
    pub id: i64,
    pub username: String,
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
    /// `serde(default)` keeps rows written before assignee capture readable;
    /// they read as unassigned until the schema-version bump's full resync
    /// rewrites them (see [`SyncStamps::schema_version`]).
    #[serde(default)]
    pub assignees: Vec<MrAssignee>,
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

/// Bumped when the per-entry schema gains data that only a full resync can
/// backfill (a delta sync rewrites only recently-updated entries, so an
/// old row could otherwise keep its `serde(default)` value for up to a full
/// sync interval). A stamp with an older version is treated as never-synced,
/// forcing one full resync.
pub const SEARCH_SCHEMA_VERSION: u32 = 1;

/// Cache-global sync bookkeeping. `0` means "never" — both for a fresh cache
/// and after [`SyncGuard::clear`] — which forces the next sync to be full.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SyncStamps {
    pub last_partial_sync_secs: u64,
    pub last_full_sync_secs: u64,
    /// Vestigial: the pre-tracked `auto` population set this when the
    /// instance rejected the global `scope=all` fetch. `auto` no longer
    /// attempts global fetches, so the flag is always written `false`; it
    /// stays on the struct so persisted stamps from either era parse.
    #[serde(default)]
    pub degraded_to_member: bool,
    /// The entry-schema version the corpus was written under; see
    /// [`SEARCH_SCHEMA_VERSION`]. Defaults to 0 for pre-existing stamps,
    /// which reads as "stale schema".
    #[serde(default)]
    pub schema_version: u32,
    /// Numeric id of the user whose session ran the sync. Drives the
    /// assigned-to-me MR filter at read time — a pure cache read that must
    /// work while dormant, so the id is persisted here rather than taken
    /// from the live session. `0` means "never synced under this schema".
    #[serde(default)]
    pub synced_user_id: i64,
}

const SEARCH_ISSUES_KEYSPACE: &str = "search_issues_v1";
const SEARCH_MRS_KEYSPACE: &str = "search_mrs_v1";
const SEARCH_PROJECTS_KEYSPACE: &str = "search_projects_v1";
const SEARCH_GROUPS_KEYSPACE: &str = "search_groups_v1";
const SEARCH_META_KEYSPACE: &str = "search_meta_v1";
const SEARCH_TRACKED_KEYSPACE: &str = "search_tracked_v1";
const STAMPS_KEY: &str = "stamps";

/// Per-project bookkeeping for the tracked population mode: a project is
/// "tracked" while there is recent local evidence of relevance (an assigned
/// issue/MR, a history entry, or a live-search hit). Keyed by project id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackedProject {
    /// UNIX seconds of the most recent evidence; drives inactivity eviction.
    /// Deliberately local recency — GitLab's `updated_at` says nothing about
    /// whether *this user* still cares about the project.
    pub last_evidence_secs: u64,
}

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
    tracked: KvStore<u64, TrackedProject>,
    /// Serializes mutations: warm-up, the periodic loop, and `ClearCache` can
    /// all race. Sync losers [`SearchCache::try_begin_sync`] and skip — a
    /// second concurrent sync would only duplicate work — while `ClearCache`'s
    /// clear [`SearchCache::begin_sync`]s to wait out an in-flight sync.
    sync_gate: Mutex<()>,
}

/// Exclusive write permission for the search cache, held for the duration of
/// one sync (or clear). All mutations live here; reads stay lock-free on
/// [`SearchCache`] itself.
pub struct SyncGuard<'a> {
    cache: &'a SearchCache,
    _gate: MutexGuard<'a, ()>,
}

impl SearchCache {
    pub fn open(db: &fjall::Database) -> Result<Self> {
        Ok(Self {
            issues: KvStore::open(db, SEARCH_ISSUES_KEYSPACE)?,
            mrs: KvStore::open(db, SEARCH_MRS_KEYSPACE)?,
            projects: KvStore::open(db, SEARCH_PROJECTS_KEYSPACE)?,
            groups: KvStore::open(db, SEARCH_GROUPS_KEYSPACE)?,
            meta: KvStore::open(db, SEARCH_META_KEYSPACE)?,
            tracked: KvStore::open(db, SEARCH_TRACKED_KEYSPACE)?,
            sync_gate: Mutex::new(()),
        })
    }

    /// Non-blocking gate acquisition — the sync path's losers-skip semantics.
    pub fn try_begin_sync(&self) -> Option<SyncGuard<'_>> {
        Some(SyncGuard {
            cache: self,
            _gate: self.sync_gate.try_lock().ok()?,
        })
    }

    /// Awaiting gate acquisition — for callers that must not skip, like
    /// `ClearCache` waiting out an in-flight sync.
    pub async fn begin_sync(&self) -> SyncGuard<'_> {
        SyncGuard {
            cache: self,
            _gate: self.sync_gate.lock().await,
        }
    }

    pub fn stamps(&self) -> Result<SyncStamps> {
        Ok(self.meta.get(STAMPS_KEY)?.unwrap_or_default())
    }

    pub fn all_issues(&self) -> Result<Vec<SearchIssue>> {
        self.issues.scan(|_, v| Ok(v))
    }

    pub fn all_mrs(&self) -> Result<Vec<SearchMr>> {
        self.mrs.scan(|_, v| Ok(v))
    }

    /// The global numeric MR ID (the one GraphQL embeds in
    /// `gid://gitlab/MergeRequest/<id>`) for a cached `(project, iid)` — the
    /// MR counterpart of `IssueCache::issue_id`. `None` when the MR isn't in
    /// the search corpus.
    pub fn mr_id(&self, project_id: i64, iid: i64) -> Result<Option<i64>> {
        Ok(self
            .mrs
            .scan(
                |_, m: SearchMr| Ok((m.project_id == project_id && m.iid == iid).then_some(m.id)),
            )?
            .into_iter()
            .flatten()
            .next())
    }

    pub fn all_projects(&self) -> Result<Vec<SearchProject>> {
        self.projects.scan(|_, v| Ok(v))
    }

    pub fn all_groups(&self) -> Result<Vec<SearchGroup>> {
        self.groups.scan(|_, v| Ok(v))
    }

    /// Every tracked project with its bookkeeping, unordered.
    pub fn tracked_projects(&self) -> Result<Vec<(i64, TrackedProject)>> {
        self.tracked.scan(|k, v| Ok((k as i64, v)))
    }

    /// Point read of one issue by global id — the live search uses it to
    /// patch fields `/search` omits (epic, time stats) from an existing
    /// richer row before upserting.
    pub fn issue_by_id(&self, id: i64) -> Result<Option<SearchIssue>> {
        self.issues.get(id as u64)
    }
}

impl SyncGuard<'_> {
    pub fn set_stamps(&self, stamps: &SyncStamps) -> Result<()> {
        self.cache.meta.put(STAMPS_KEY, stamps)
    }

    pub fn upsert_issues(&self, items: &[SearchIssue]) -> Result<()> {
        upsert(&self.cache.issues, items)
    }

    pub fn upsert_mrs(&self, items: &[SearchMr]) -> Result<()> {
        upsert(&self.cache.mrs, items)
    }

    pub fn upsert_projects(&self, items: &[SearchProject]) -> Result<()> {
        upsert(&self.cache.projects, items)
    }

    pub fn upsert_groups(&self, items: &[SearchGroup]) -> Result<()> {
        upsert(&self.cache.groups, items)
    }

    pub fn retain_issues(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.cache.issues, keep)
    }

    pub fn retain_mrs(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.cache.mrs, keep)
    }

    pub fn retain_projects(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.cache.projects, keep)
    }

    pub fn retain_groups(&self, keep: &HashSet<u64>) -> Result<usize> {
        retain(&self.cache.groups, keep)
    }

    /// Record evidence of relevance for `project_ids` at `now`. Existing
    /// entries keep their most recent evidence (max-merge), so replaying an
    /// old evidence source can never age a project.
    pub fn note_tracked(&self, project_ids: impl IntoIterator<Item = i64>, now: u64) -> Result<()> {
        for id in project_ids {
            let key = id as u64;
            let last = self
                .cache
                .tracked
                .get(key)?
                .map(|t| t.last_evidence_secs)
                .unwrap_or(0);
            if now > last {
                self.cache.tracked.put(
                    key,
                    &TrackedProject {
                        last_evidence_secs: now,
                    },
                )?;
            }
        }
        Ok(())
    }

    /// Drop tracked entries whose last evidence predates `cutoff_secs` and
    /// return their project ids — the inactivity half of retention. The
    /// caller prunes the corpus via [`SyncGuard::prune_untracked`].
    pub fn evict_tracked(&self, cutoff_secs: u64) -> Result<Vec<i64>> {
        let stale: Vec<u64> = self
            .cache
            .tracked
            .scan(|k, t: TrackedProject| Ok((t.last_evidence_secs < cutoff_secs).then_some(k)))?
            .into_iter()
            .flatten()
            .collect();
        let mut evicted = Vec::with_capacity(stale.len());
        for key in stale {
            self.cache.tracked.remove(key)?;
            evicted.push(key as i64);
        }
        Ok(evicted)
    }

    /// Remove issues of `project_id` whose id is not in `keep` — the
    /// per-project deletion half of a tracked full sync. Entries of other
    /// projects are untouched.
    pub fn retain_issues_in_project(&self, project_id: i64, keep: &HashSet<u64>) -> Result<usize> {
        retain_in_project(&self.cache.issues, project_id, keep, |i| i.project_id)
    }

    /// MR counterpart of [`SyncGuard::retain_issues_in_project`].
    pub fn retain_mrs_in_project(&self, project_id: i64, keep: &HashSet<u64>) -> Result<usize> {
        retain_in_project(&self.cache.mrs, project_id, keep, |m| m.project_id)
    }

    /// Drop every issue and MR whose project is not in `tracked` — the
    /// corpus half of the eviction sweep. Returns `(issues, mrs)` removed.
    pub fn prune_untracked(&self, tracked: &HashSet<i64>) -> Result<(usize, usize)> {
        let issues = prune_by_project(&self.cache.issues, tracked, |i| i.project_id)?;
        let mrs = prune_by_project(&self.cache.mrs, tracked, |m| m.project_id)?;
        Ok((issues, mrs))
    }

    /// Apply `f` to the cached MR at `(project_id, iid)`, if any. Returns
    /// whether a row was updated. Used by the write handlers to reflect a
    /// close/unassign immediately instead of waiting for the next sync.
    pub fn update_mr(
        &self,
        project_id: i64,
        iid: i64,
        f: impl FnOnce(&mut SearchMr),
    ) -> Result<bool> {
        let Some(mut mr) = self
            .cache
            .mrs
            .scan(|_, m: SearchMr| Ok((m.project_id == project_id && m.iid == iid).then_some(m)))?
            .into_iter()
            .flatten()
            .next()
        else {
            return Ok(false);
        };
        f(&mut mr);
        self.cache.mrs.put(mr.id as u64, &mr)?;
        Ok(true)
    }

    /// Drop every entry of every kind, the tracked set, *and* the sync
    /// stamps, so the next sync runs full.
    pub fn clear(&self) -> Result<()> {
        self.cache.issues.clear()?;
        self.cache.mrs.clear()?;
        self.cache.projects.clear()?;
        self.cache.groups.clear()?;
        self.cache.tracked.clear()?;
        self.cache.meta.clear()
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

/// Remove entries of `project_id` whose key is not in `keep` — the scoped
/// variant of [`retain`] used by tracked full syncs, which only ever fetch
/// one project at a time and so can only vouch for that project.
fn retain_in_project<T: Serialize + DeserializeOwned>(
    store: &KvStore<u64, T>,
    project_id: i64,
    keep: &HashSet<u64>,
    project_of: impl Fn(&T) -> i64,
) -> Result<usize> {
    let stale: Vec<u64> = store
        .scan(|k, v| Ok((project_of(&v) == project_id && !keep.contains(&k)).then_some(k)))?
        .into_iter()
        .flatten()
        .collect();
    let count = stale.len();
    for key in stale {
        store.remove(key)?;
    }
    Ok(count)
}

/// Remove entries whose project is not in `tracked`. Returns the removed
/// count.
fn prune_by_project<T: Serialize + DeserializeOwned>(
    store: &KvStore<u64, T>,
    tracked: &HashSet<i64>,
    project_of: impl Fn(&T) -> i64,
) -> Result<usize> {
    let stale: Vec<u64> = store
        .scan(|k, v| Ok((!tracked.contains(&project_of(&v))).then_some(k)))?
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
    use proptest::prelude::*;

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
            assignees: vec![],
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
        let g = c.try_begin_sync().unwrap();
        g.upsert_issues(&[issue(1, "old")]).unwrap();
        g.upsert_issues(&[issue(1, "new")]).unwrap();
        let all = c.all_issues().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].title, "new");
    }

    #[test]
    fn each_kind_roundtrips_independently() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.upsert_issues(&[issue(1, "i")]).unwrap();
        g.upsert_mrs(&[mr(1, "m")]).unwrap();
        g.upsert_projects(&[project(1, "team/p")]).unwrap();
        g.upsert_groups(&[group(1, "team")]).unwrap();

        assert_eq!(c.all_issues().unwrap()[0].title, "i");
        assert_eq!(c.all_mrs().unwrap()[0].title, "m");
        assert_eq!(c.all_projects().unwrap()[0].path, "team/p");
        assert_eq!(c.all_groups().unwrap()[0].path, "team");
    }

    #[test]
    fn retain_removes_exactly_the_missing_keys() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.upsert_issues(&[issue(1, "keep"), issue(2, "drop"), issue(3, "keep")])
            .unwrap();

        let removed = g.retain_issues(&HashSet::from([1, 3])).unwrap();
        assert_eq!(removed, 1);

        let mut ids: Vec<i64> = c.all_issues().unwrap().iter().map(|i| i.id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn retain_with_empty_keep_empties_the_kind() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.upsert_mrs(&[mr(1, "a"), mr(2, "b")]).unwrap();
        assert_eq!(g.retain_mrs(&HashSet::new()).unwrap(), 2);
        assert!(c.all_mrs().unwrap().is_empty());
    }

    #[test]
    fn sync_gate_is_exclusive_until_dropped() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        assert!(c.try_begin_sync().is_none());
        drop(g);
        assert!(c.try_begin_sync().is_some());
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
        c.try_begin_sync()
            .unwrap()
            .set_stamps(&SyncStamps {
                last_partial_sync_secs: 123,
                last_full_sync_secs: 45,
                degraded_to_member: true,
                schema_version: SEARCH_SCHEMA_VERSION,
                synced_user_id: 42,
            })
            .unwrap();
        let s = c.stamps().unwrap();
        assert_eq!(s.last_partial_sync_secs, 123);
        assert_eq!(s.last_full_sync_secs, 45);
        assert!(s.degraded_to_member);
        assert_eq!(s.schema_version, SEARCH_SCHEMA_VERSION);
        assert_eq!(s.synced_user_id, 42);
    }

    #[test]
    fn stamps_without_degraded_field_still_parse() {
        // Stamps written before `degraded_to_member` existed must stay
        // readable: the field defaults to false.
        let s: SyncStamps =
            serde_json::from_str(r#"{"last_partial_sync_secs": 7, "last_full_sync_secs": 7}"#)
                .unwrap();
        assert!(!s.degraded_to_member);
        assert_eq!(s.schema_version, 0, "pre-versioned stamps read as stale");
        assert_eq!(s.synced_user_id, 0);
    }

    #[test]
    fn search_mr_without_assignees_field_still_parses() {
        // Rows written before assignee capture must stay readable: they read
        // as unassigned until the forced full resync rewrites them.
        let m: SearchMr = serde_json::from_str(
            r#"{"id":1,"iid":10,"project_id":1,"title":"t","web_url":"u",
                "state":"opened","labels":[],"updated_at_secs":100}"#,
        )
        .unwrap();
        assert!(m.assignees.is_empty());
    }

    #[test]
    fn mr_id_resolves_cached_project_iid_pairs() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.upsert_mrs(&[mr(3, "a"), mr(4, "b")]).unwrap();
        drop(g);

        // mr() sets iid = id * 10 and project_id = 1.
        assert_eq!(c.mr_id(1, 30).unwrap(), Some(3));
        assert_eq!(c.mr_id(1, 40).unwrap(), Some(4));
        assert_eq!(c.mr_id(1, 99).unwrap(), None, "unknown iid");
        assert_eq!(c.mr_id(2, 30).unwrap(), None, "wrong project");
    }

    #[test]
    fn update_mr_mutates_exactly_the_matching_row() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        let mut assigned = mr(3, "mine");
        assigned.assignees = vec![MrAssignee {
            id: 42,
            username: "me".into(),
        }];
        g.upsert_mrs(&[assigned, mr(4, "other")]).unwrap();

        assert!(
            g.update_mr(1, 30, |m| {
                m.state = "closed".into();
                m.assignees.retain(|a| a.id != 42);
            })
            .unwrap()
        );
        assert!(!g.update_mr(1, 99, |_| ()).unwrap(), "missing row → false");
        drop(g);

        let mrs = c.all_mrs().unwrap();
        let updated = mrs.iter().find(|m| m.id == 3).unwrap();
        assert_eq!(updated.state, "closed");
        assert!(updated.assignees.is_empty());
        let untouched = mrs.iter().find(|m| m.id == 4).unwrap();
        assert_eq!(untouched.state, "opened");
    }

    #[test]
    fn clear_wipes_all_kinds_and_resets_stamps() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.upsert_issues(&[issue(1, "i")]).unwrap();
        g.upsert_mrs(&[mr(1, "m")]).unwrap();
        g.upsert_projects(&[project(1, "team/p")]).unwrap();
        g.upsert_groups(&[group(1, "team")]).unwrap();
        g.note_tracked([1], 100).unwrap();
        g.set_stamps(&SyncStamps {
            last_partial_sync_secs: 1,
            last_full_sync_secs: 1,
            ..Default::default()
        })
        .unwrap();

        g.clear().unwrap();

        assert!(c.all_issues().unwrap().is_empty());
        assert!(c.all_mrs().unwrap().is_empty());
        assert!(c.all_projects().unwrap().is_empty());
        assert!(c.all_groups().unwrap().is_empty());
        assert!(c.tracked_projects().unwrap().is_empty());
        assert_eq!(c.stamps().unwrap().last_partial_sync_secs, 0);
        assert_eq!(c.stamps().unwrap().last_full_sync_secs, 0);
    }

    #[test]
    fn note_tracked_max_merges_evidence() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.note_tracked([1, 2], 100).unwrap();
        g.note_tracked([1], 50).unwrap(); // older evidence must not age it
        g.note_tracked([2], 200).unwrap();

        let mut tracked = c.tracked_projects().unwrap();
        tracked.sort_by_key(|(id, _)| *id);
        assert_eq!(
            tracked
                .iter()
                .map(|(id, t)| (*id, t.last_evidence_secs))
                .collect::<Vec<_>>(),
            vec![(1, 100), (2, 200)]
        );
    }

    #[test]
    fn evict_tracked_drops_only_stale_entries() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        g.note_tracked([1], 100).unwrap();
        g.note_tracked([2], 500).unwrap();

        let evicted = g.evict_tracked(300).unwrap();
        assert_eq!(evicted, vec![1]);

        let tracked = c.tracked_projects().unwrap();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].0, 2);
    }

    #[test]
    fn retain_in_project_touches_only_that_project() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        let mut foreign = issue(3, "other project");
        foreign.project_id = 2;
        g.upsert_issues(&[issue(1, "keep"), issue(2, "drop"), foreign])
            .unwrap();
        let mut foreign_mr = mr(30, "other project");
        foreign_mr.project_id = 2;
        g.upsert_mrs(&[mr(10, "drop"), foreign_mr]).unwrap();

        assert_eq!(
            g.retain_issues_in_project(1, &HashSet::from([1])).unwrap(),
            1,
            "only issue 2 (project 1, not kept) goes"
        );
        assert_eq!(
            g.retain_mrs_in_project(1, &HashSet::new()).unwrap(),
            1,
            "only MR 10 (project 1) goes"
        );

        let mut ids: Vec<i64> = c.all_issues().unwrap().iter().map(|i| i.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 3], "the other project's issue is untouched");
        let ids: Vec<i64> = c.all_mrs().unwrap().iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![30], "the other project's MR is untouched");
    }

    #[test]
    fn prune_untracked_drops_foreign_projects() {
        let (c, _td) = cache();
        let g = c.try_begin_sync().unwrap();
        let mut foreign = issue(2, "untracked");
        foreign.project_id = 9;
        g.upsert_issues(&[issue(1, "tracked"), foreign]).unwrap();
        let mut foreign_mr = mr(20, "untracked");
        foreign_mr.project_id = 9;
        g.upsert_mrs(&[mr(10, "tracked"), foreign_mr]).unwrap();

        let (issues, mrs) = g.prune_untracked(&HashSet::from([1])).unwrap();
        assert_eq!((issues, mrs), (1, 1));
        assert_eq!(c.all_issues().unwrap()[0].id, 1);
        assert_eq!(c.all_mrs().unwrap()[0].id, 10);
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
            let g = c.try_begin_sync().unwrap();
            g.upsert_issues(&[issue(1, "persisted")]).unwrap();
            g.note_tracked([5], 7).unwrap();
            g.set_stamps(&SyncStamps {
                last_partial_sync_secs: 7,
                last_full_sync_secs: 7,
                ..Default::default()
            })
            .unwrap();
        }
        let db = fjall::Database::builder(&path).open().unwrap();
        let c = SearchCache::open(&db).unwrap();
        assert_eq!(c.all_issues().unwrap()[0].title, "persisted");
        assert_eq!(c.stamps().unwrap().last_full_sync_secs, 7);
        assert_eq!(
            c.tracked_projects().unwrap(),
            vec![(
                5,
                TrackedProject {
                    last_evidence_secs: 7
                }
            )]
        );
    }

    #[test]
    fn parse_iid_query_rejects_a_bare_hash() {
        assert_eq!(parse_iid_query("#"), None);
    }

    proptest! {
        #[test]
        fn parse_iid_query_roundtrips_any_padded_reference(
            n in 0..=i64::MAX,
            pad_left in " {0,3}",
            pad_right in " {0,3}",
        ) {
            prop_assert_eq!(parse_iid_query(&format!("{pad_left}#{n}{pad_right}")), Some(n));
        }

        #[test]
        fn parse_iid_query_rejects_anything_without_a_leading_hash(s in "[^#]*") {
            prop_assert_eq!(parse_iid_query(&s), None);
        }

        #[test]
        fn parse_iid_query_rejects_non_digit_tails(
            digits in "[0-9]{0,4}",
            junk in "[a-z#-]{1,3}",
            more in "[0-9]{0,3}",
        ) {
            prop_assert_eq!(parse_iid_query(&format!("#{digits}{junk}{more}")), None);
        }

        #[test]
        fn text_matches_finds_an_inserted_needle_in_any_case(
            needle in "[a-zA-Z]{1,6}",
            prefix in "[a-zA-Z0-9 ]{0,8}",
            suffix in "[a-zA-Z0-9 ]{0,8}",
        ) {
            let needle_lower = needle.to_lowercase();
            let hay = format!("{prefix}{needle}{suffix}");
            prop_assert!(text_matches(&needle_lower, &hay));
            prop_assert!(text_matches(&needle_lower, &hay.to_uppercase()));
            prop_assert!(text_matches(&needle_lower, &hay.to_lowercase()));
        }

        #[test]
        fn text_matches_rejects_a_needle_absent_from_the_haystack(
            needle in "[a-z]{2,6}",
            hay in "[0-9 ]{0,10}",
        ) {
            prop_assert!(!text_matches(&needle, &hay));
        }
    }
}
