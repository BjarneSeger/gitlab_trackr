//! Shared scaffolding for the Criterion benches: a dormant `Handlers` on a
//! temp-dir fjall database plus scale-parameterized corpus generators.
//!
//! Deliberately independent of the `#[cfg(test)]` fixtures in
//! `handlers/tests.rs` — benches are separate compilation units that link the
//! library without `cfg(test)`, so those helpers are invisible here. The
//! session is always dormant: every benched path is a pure cache read or
//! write, and dormancy proves no network access is possible.
#![allow(dead_code)] // each bench target compiles this module independently

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, RwLock};

use gitlab_trackr_api::Issue;
use gitlab_trackrd::boards::BoardCache;
use gitlab_trackrd::cache::IssueCache;
use gitlab_trackrd::config::SharedConfig;
use gitlab_trackrd::error::DormancyReason;
use gitlab_trackrd::gitlab::Issuable;
use gitlab_trackrd::handlers::{ConnState, Handlers, SessionSlot};
use gitlab_trackrd::history::{HistoryCache, StoredTimelog};
use gitlab_trackrd::queue::RetryQueue;
use gitlab_trackrd::refresh_meta::RefreshMeta;
use gitlab_trackrd::search::{
    MrAssignee, SEARCH_SCHEMA_VERSION, SearchGroup, SearchIssue, SearchMr, SearchProject,
    SyncStamps,
};

/// The user id the seeded stamps claim ran the sync; assigned-MR benches
/// filter for this id.
pub const SYNCED_USER_ID: i64 = 1;

/// A `Handlers` on a fresh temp-dir fjall database. The `TempDir` is bundled
/// so it outlives the stores — dropping it deletes the database out from
/// under fjall. The runtime is bundled too: `RetryQueue::new` spawns its
/// worker task, so construction must happen inside a runtime context, and the
/// async handler benches drive their futures on the same runtime
/// (`b.to_async(&env.rt)`).
pub struct BenchEnv {
    pub h: Handlers,
    pub rt: tokio::runtime::Runtime,
    _dir: tempfile::TempDir,
}

pub fn dormant_env() -> BenchEnv {
    // The benched read paths never await real IO; the time driver is for the
    // (idle) queue worker's backoff sleeps.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::Database::builder(dir.path().join("db"))
        .open()
        .unwrap();
    let session: SessionSlot = Arc::new(RwLock::new(ConnState::Dormant(
        DormancyReason::NoCredentials,
    )));
    let cache = Arc::new(IssueCache::open(&db).unwrap());
    let boards = Arc::new(BoardCache::open(&db).unwrap());
    let history = Arc::new(HistoryCache::open(&db).unwrap());
    let search = Arc::new(gitlab_trackrd::search::SearchCache::open(&db).unwrap());
    let refresh_meta = Arc::new(RefreshMeta::open(&db).unwrap());
    let config: SharedConfig = Arc::new(std::sync::RwLock::new(gitlab_trackrd::config::defaults()));
    let queue = RetryQueue::new(Arc::clone(&session), &db, Arc::clone(&config)).unwrap();
    drop(_guard);
    BenchEnv {
        h: Handlers {
            session,
            cache,
            boards,
            history,
            search,
            refresh_meta,
            queue,
            config,
            reconnect_signal: Arc::new(Notify::new()),
        },
        rt,
        _dir: dir,
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Knuth-multiplicative shuffle so values derived from a sequential `i` are
/// non-monotonic — fjall iterates in key order, and a pre-sorted corpus would
/// flatter the `sort_by_key(Reverse(..))` in the handlers.
pub fn shuffled(i: u64, range: u64) -> u64 {
    i.wrapping_mul(2654435761) % range
}

const LABELS: [&str; 6] = ["bug", "feature", "backend", "urgent", "docs", "infra"];

/// 0–3 labels per entry so label matching does real work.
fn label_set(i: u64) -> Vec<String> {
    (0..i % 4)
        .map(|k| LABELS[((i + k) % 6) as usize].to_string())
        .collect()
}

/// ~1% of titles contain the marker `flaky` (the hit-variant needle); the
/// rest say `steady`. Component suffix varies so substring scans can't
/// short-circuit on a shared prefix.
fn title(i: u64, noun: &str) -> String {
    let marker = if i % 100 == 0 { "flaky" } else { "steady" };
    format!("{noun} {i}: {marker} sync in component {}", i % 97)
}

/// Half the corpus lives under the `team` namespace, half under `other`, so
/// group filters are selective.
fn namespace(i: u64) -> &'static str {
    if i % 2 == 0 { "team" } else { "other" }
}

pub fn search_issue(i: u64) -> SearchIssue {
    SearchIssue {
        id: i as i64,
        iid: (i % 1_000 + 1) as i64,
        project_id: (i % 50 + 1) as i64,
        title: title(i, "Issue"),
        web_url: format!(
            "https://gl/{}/proj{}/-/issues/{}",
            namespace(i),
            i % 50,
            i % 1_000 + 1
        ),
        state: if i % 5 == 0 { "closed" } else { "opened" }.to_string(),
        labels: label_set(i),
        parent: String::new(),
        total_time: String::new(),
        updated_at_secs: shuffled(i, 1_000_000) + 1,
    }
}

pub fn search_mr(i: u64) -> SearchMr {
    // A fixed handful assigned to the synced user (the assigned-MR view);
    // some assigned to someone else so the assignee filter does real work.
    let assignees = if i < 10 {
        vec![MrAssignee {
            id: SYNCED_USER_ID,
            username: "me".to_string(),
        }]
    } else if i % 3 == 0 {
        vec![MrAssignee {
            id: 999,
            username: "other".to_string(),
        }]
    } else {
        Vec::new()
    };
    SearchMr {
        id: i as i64,
        iid: (i % 1_000 + 1) as i64,
        project_id: (i % 50 + 1) as i64,
        title: title(i, "MR"),
        web_url: format!(
            "https://gl/{}/proj{}/-/merge_requests/{}",
            namespace(i),
            i % 50,
            i % 1_000 + 1
        ),
        state: if i >= 10 && i % 5 == 0 {
            "merged"
        } else {
            "opened"
        }
        .to_string(),
        labels: label_set(i),
        assignees,
        updated_at_secs: shuffled(i, 1_000_000) + 1,
    }
}

pub fn search_project(i: u64) -> SearchProject {
    SearchProject {
        id: i as i64,
        name: format!("proj{i}"),
        path: format!("{}/proj{i}", namespace(i)),
        web_url: format!("https://gl/{}/proj{i}", namespace(i)),
    }
}

pub fn search_group(i: u64) -> SearchGroup {
    SearchGroup {
        id: i as i64,
        name: format!("group{i}"),
        path: format!("{}/group{i}", namespace(i)),
        web_url: format!("https://gl/{}/group{i}", namespace(i)),
    }
}

pub fn wire_issue(i: u64) -> Issue {
    Issue {
        id: i as i64,
        iid: (i % 1_000 + 1) as i64,
        project_id: (i % 50 + 1) as i64,
        title: title(i, "Issue"),
        web_url: format!(
            "https://gl/{}/proj{}/-/issues/{}",
            namespace(i),
            i % 50,
            i % 1_000 + 1
        ),
        state: "opened".to_string(),
        parent: String::new(),
        total_time: String::new(),
        graph_status: String::new(),
    }
}

/// A stored timelog with `spent_at_secs` spread uniformly over the 30 days
/// before `now`, shuffled so store order is not time order.
pub fn stored_timelog(i: u64, now: u64) -> StoredTimelog {
    StoredTimelog {
        timelog_id: i,
        spent_at_secs: now - shuffled(i, 30 * 86_400),
        kind: if i % 4 == 0 {
            Issuable::MergeRequest
        } else {
            Issuable::Issue
        },
        project_id: (i % 50 + 1) as i64,
        iid: (i % 1_000 + 1) as i64,
        title: title(i, "Issue"),
        web_url: format!("https://gl/team/proj{}/-/issues/{}", i % 50, i % 1_000 + 1),
        duration: "1h 30m".to_string(),
        summary: "worked on it".to_string(),
    }
}

/// Sync stamps that pass every cold-cache guard in the read handlers
/// (non-zero partial stamp, current schema, non-zero synced user).
pub fn valid_stamps() -> SyncStamps {
    SyncStamps {
        last_partial_sync_secs: 1_700_000_000,
        last_full_sync_secs: 1_700_000_000,
        degraded_to_member: false,
        schema_version: SEARCH_SCHEMA_VERSION,
        synced_user_id: SYNCED_USER_ID,
    }
}

/// Seed the full search corpus through the real write paths: `n` issues,
/// `n/2` MRs, `n/50` projects, `n/100` groups, plus board labels for every
/// project id the issues reference (so the per-hit `boards.get` in
/// `wire_search_issue` finds something) and stamps that unlock the readers.
pub fn seed_search_corpus(env: &BenchEnv, n: u64) {
    let guard = env.h.search.try_begin_sync().unwrap();
    let issues: Vec<_> = (0..n).map(search_issue).collect();
    guard.upsert_issues(&issues).unwrap();
    let mrs: Vec<_> = (0..n / 2).map(search_mr).collect();
    guard.upsert_mrs(&mrs).unwrap();
    let projects: Vec<_> = (0..(n / 50).max(1)).map(search_project).collect();
    guard.upsert_projects(&projects).unwrap();
    let groups: Vec<_> = (0..(n / 100).max(1)).map(search_group).collect();
    guard.upsert_groups(&groups).unwrap();
    guard.set_stamps(&valid_stamps()).unwrap();
    for pid in 1..=50 {
        env.h
            .boards
            .put(pid, vec!["Doing".into(), "Review".into(), "Done".into()])
            .unwrap();
    }
}

/// Seed only MRs + stamps — for the assigned-MR benches, where issues would
/// just slow down the (unmeasured) setup.
pub fn seed_mr_corpus(env: &BenchEnv, n: u64) {
    let guard = env.h.search.try_begin_sync().unwrap();
    let mrs: Vec<_> = (0..n).map(search_mr).collect();
    guard.upsert_mrs(&mrs).unwrap();
    guard.set_stamps(&valid_stamps()).unwrap();
}

pub fn seed_history(env: &BenchEnv, n: u64, now: u64) {
    let entries: Vec<_> = (0..n).map(|i| stored_timelog(i, now)).collect();
    env.h.history.upsert(&entries).unwrap();
}

/// One whole-corpus blob write through `IssueCache::put`.
pub fn seed_issue_cache(env: &BenchEnv, n: u64) {
    let issues: Vec<_> = (0..n).map(wire_issue).collect();
    env.h.cache.put(&issues).unwrap();
}
