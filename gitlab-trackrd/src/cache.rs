//! fjall-backed cache for the assigned-issues list, keyed by group.
//!
//! Hides `KvStore` behind a small [`IssueCache`] interface; callers see plain
//! `Result<Option<_>>` / `Result<()>` and never touch the storage layer.
//!
//! The background refresh loop fetches every assigned issue and hands the whole
//! list to [`IssueCache::put`], which buckets them by group (parsed from each
//! issue's `web_url`). `tt list` reads every bucket; `tt list <group>` reads the
//! buckets under that group. There is no TTL — the background loop owns freshness
//! and readers always serve whatever was last synced.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::db::KvStore;
use crate::error::Result;
use gitlab_trackr_api::Issue;

/// On-disk cache: assigned issues bucketed by their group namespace.
#[derive(Debug, Default, Serialize, Deserialize)]
struct IssueBuckets {
    /// Group namespace path (e.g. `"team/backend"`) -> issues under it.
    by_group: BTreeMap<String, Vec<Issue>>,
}

const ISSUES_KEYSPACE: &str = "issues_cache_v1";

const KEY: &str = "assigned";

pub struct IssueCache {
    store: KvStore<&'static str, IssueBuckets>,
}

impl IssueCache {
    /// Open (or create) the issue-cache keyspace in `db`.
    pub fn open(db: &fjall::Database) -> Result<Self> {
        Ok(Self {
            store: KvStore::open(db, ISSUES_KEYSPACE)?,
        })
    }

    /// Every cached issue across all groups, or `None` if nothing is cached.
    pub fn get(&self) -> Result<Option<Vec<Issue>>> {
        Ok(self
            .buckets()?
            .map(|b| b.by_group.into_values().flatten().collect()))
    }

    /// Issues in `group` and its subgroups, matching GitLab's subgroup-inclusive
    /// group filter. Empty when the cache is cold or the group has none.
    pub fn get_group(&self, group: &str) -> Result<Vec<Issue>> {
        let Some(b) = self.buckets()? else {
            return Ok(Vec::new());
        };
        Ok(b.by_group
            .into_iter()
            .filter(|(ns, _)| in_group(ns, group))
            .flat_map(|(_, issues)| issues)
            .collect())
    }

    /// Replace the cache with `issues`, bucketed by group namespace.
    pub fn put(&self, issues: &[Issue]) -> Result<()> {
        let mut by_group: BTreeMap<String, Vec<Issue>> = BTreeMap::new();
        for issue in issues {
            by_group
                .entry(namespace_of(&issue.web_url))
                .or_default()
                .push(issue.clone());
        }
        self.store.put(KEY, &IssueBuckets { by_group })
    }

    /// Drop the cached entry so the next `get` returns `None`.
    pub fn clear(&self) -> Result<()> {
        self.store.remove(KEY)
    }

    /// Drop the issue identified by `(project_id, issue_iid)` from whichever
    /// group bucket holds it, if present. Returns whether anything was removed.
    /// Lets a close/unassign disappear from `tt list` immediately instead of
    /// waiting for the next refresh.
    pub fn remove_issue(&self, project_id: i64, issue_iid: i64) -> Result<bool> {
        let Some(mut b) = self.buckets()? else {
            return Ok(false);
        };
        let mut removed = false;
        for issues in b.by_group.values_mut() {
            let before = issues.len();
            issues.retain(|i| !(i.project_id == project_id && i.iid == issue_iid));
            removed |= issues.len() != before;
        }
        if !removed {
            return Ok(false);
        }
        b.by_group.retain(|_, issues| !issues.is_empty());
        self.store.put(KEY, &b)?;
        Ok(true)
    }

    /// The global numeric `id` (the value GraphQL embeds as
    /// `gid://gitlab/Issue/<id>`) for the cached issue identified by
    /// `(project_id, issue_iid)`, or `None` when it isn't cached. Shares the
    /// `(project_id, iid)` key predicate with [`Self::remove_issue`].
    pub fn issue_id(&self, project_id: i64, issue_iid: i64) -> Result<Option<i64>> {
        let Some(b) = self.buckets()? else {
            return Ok(None);
        };
        Ok(b.by_group
            .into_values()
            .flatten()
            .find(|i| i.project_id == project_id && i.iid == issue_iid)
            .map(|i| i.id))
    }

    fn buckets(&self) -> Result<Option<IssueBuckets>> {
        self.store.get(KEY)
    }
}

/// The group namespace an issue belongs to, parsed from its `web_url`
/// (`https://host/<namespace>/-/issues/<iid>`). Returns `""` when there is no
/// namespace to parse — such issues still show in `tt list`, they just don't
/// match any `tt list <group>` filter.
fn namespace_of(web_url: &str) -> String {
    web_url
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(web_url)
        .split_once('/')
        .map(|(_, path)| path)
        .unwrap_or("")
        .split("/-/")
        .next()
        .unwrap_or("")
        .trim_matches('/')
        .to_string()
}

/// Whether `namespace` falls under `group`, matching GitLab's subgroup-inclusive
/// `.group(g)` filter: an exact match or a `group/…` descendant.
fn in_group(namespace: &str, group: &str) -> bool {
    let group = group.trim_matches('/');
    !group.is_empty() && (namespace == group || namespace.starts_with(&format!("{group}/")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue_at(web_url: &str, iid: i64, title: &str) -> Issue {
        Issue {
            id: iid * 10,
            iid,
            project_id: 1,
            title: title.to_string(),
            web_url: web_url.to_string(),
            state: "opened".to_string(),
            parent: String::new(),
            total_time: String::new(),
            graph_status: String::new(),
        }
    }

    /// Default helper: every issue lives in the same project `grp/proj`.
    fn make_issue(iid: i64, title: &str) -> Issue {
        issue_at(&format!("https://gl/grp/proj/-/issues/{iid}"), iid, title)
    }

    fn cache() -> (IssueCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap();
        (IssueCache::open(&db).unwrap(), dir)
    }

    #[test]
    fn namespace_of_parses_group_path() {
        assert_eq!(namespace_of("https://gl/team/proj/-/issues/5"), "team/proj");
        assert_eq!(
            namespace_of("https://gl/team/sub/proj/-/issues/5"),
            "team/sub/proj"
        );
    }

    #[test]
    fn namespace_of_empty_without_path() {
        assert_eq!(namespace_of("https://gl"), "");
        assert_eq!(namespace_of(""), "");
    }

    #[test]
    fn in_group_matches_exact_and_descendants_only() {
        assert!(in_group("team/proj", "team"));
        assert!(in_group("team/sub/proj", "team/sub"));
        assert!(in_group("team", "team"));
        assert!(!in_group("teamfoo/proj", "team"), "not a prefix boundary");
        assert!(!in_group("other/proj", "team"));
        assert!(!in_group("team/proj", ""), "empty filter matches nothing");
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
    fn get_group_matches_group_and_subgroups() {
        let (c, _td) = cache();
        c.put(&[
            issue_at("https://gl/team/api/-/issues/1", 1, "api"),
            issue_at("https://gl/team/sub/web/-/issues/2", 2, "web"),
            issue_at("https://gl/other/x/-/issues/3", 3, "other"),
        ])
        .unwrap();

        let team: Vec<i64> = c.get_group("team").unwrap().iter().map(|i| i.iid).collect();
        assert_eq!(team, vec![1, 2], "team includes its subgroup");

        let sub: Vec<i64> = c
            .get_group("team/sub")
            .unwrap()
            .iter()
            .map(|i| i.iid)
            .collect();
        assert_eq!(sub, vec![2]);

        assert!(c.get_group("missing").unwrap().is_empty());
    }

    #[test]
    fn get_group_empty_on_cold_cache() {
        let (c, _td) = cache();
        assert!(c.get_group("team").unwrap().is_empty());
    }

    #[test]
    fn clear_removes_entry() {
        let (c, _td) = cache();
        c.put(&[make_issue(1, "a")]).unwrap();
        c.clear().unwrap();
        assert!(c.get().unwrap().is_none());
    }

    #[test]
    fn remove_issue_drops_only_the_match() {
        let (c, _td) = cache();
        // make_issue uses project_id = 1 for every issue, all in one bucket.
        c.put(&[make_issue(1, "a"), make_issue(2, "b"), make_issue(3, "c")])
            .unwrap();

        assert!(c.remove_issue(1, 2).unwrap(), "iid 2 was present");
        let got = c.get().unwrap().unwrap();
        let iids: Vec<i64> = got.iter().map(|i| i.iid).collect();
        assert_eq!(iids, vec![1, 3], "only iid 2 removed");
    }

    #[test]
    fn remove_issue_across_buckets_drops_emptied_bucket() {
        let (c, _td) = cache();
        c.put(&[
            issue_at("https://gl/a/p/-/issues/1", 1, "a"),
            issue_at("https://gl/b/p/-/issues/2", 2, "b"),
        ])
        .unwrap();

        assert!(c.remove_issue(1, 1).unwrap());
        let got: Vec<i64> = c.get().unwrap().unwrap().iter().map(|i| i.iid).collect();
        assert_eq!(got, vec![2], "bucket a emptied");
        assert!(
            c.get_group("a").unwrap().is_empty(),
            "emptied bucket dropped"
        );
    }

    #[test]
    fn remove_issue_is_noop_when_absent() {
        let (c, _td) = cache();
        c.put(&[make_issue(1, "a")]).unwrap();
        assert!(!c.remove_issue(1, 99).unwrap(), "no matching iid");
        assert!(
            !c.remove_issue(7, 1).unwrap(),
            "iid matches but project_id does not"
        );
        assert_eq!(c.get().unwrap().unwrap().len(), 1, "list unchanged");
    }

    #[test]
    fn remove_issue_on_empty_cache_returns_false() {
        let (c, _td) = cache();
        assert!(!c.remove_issue(1, 1).unwrap());
    }

    #[test]
    fn issue_id_resolves_by_project_and_iid() {
        let (c, _td) = cache();
        // issue_at uses project_id = 1 and sets id = iid * 10.
        c.put(&[make_issue(1, "a"), make_issue(2, "b")]).unwrap();

        assert_eq!(
            c.issue_id(1, 2).unwrap(),
            Some(20),
            "matched issue's global id"
        );
        assert_eq!(c.issue_id(1, 99).unwrap(), None, "no such iid");
        assert_eq!(
            c.issue_id(7, 1).unwrap(),
            None,
            "iid matches but project_id does not"
        );
    }

    #[test]
    fn issue_id_on_empty_cache_is_none() {
        let (c, _td) = cache();
        assert_eq!(c.issue_id(1, 1).unwrap(), None);
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        // Both the store and the Database must drop before reopening — fjall
        // holds a single-process lock on the directory.
        {
            let db = fjall::Database::builder(&path).open().unwrap();
            let c = IssueCache::open(&db).unwrap();
            c.put(&[make_issue(1, "persisted")]).unwrap();
        }
        let db = fjall::Database::builder(&path).open().unwrap();
        let c = IssueCache::open(&db).unwrap();
        let got = c.get().unwrap().unwrap();
        assert_eq!(got[0].title, "persisted");
    }
}
