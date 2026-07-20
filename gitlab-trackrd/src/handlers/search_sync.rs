//! Background sync for the search cache (issues, MRs, projects, groups).
//!
//! Unlike the issue/history tiers in [`super::refresh`], this owns its own
//! schedule via the cache-global [`SyncStamps`]: an incremental sync
//! (`updated_after` deltas, upsert-only) runs at most once per
//! `search.partial_interval_secs`, and a full resync — which also reconciles
//! deletions — once per `search.full_interval_secs`. Both stamps persist, so
//! a daemon restart inside the partial window costs GitLab nothing, and the
//! very first sync (zeroed stamps) is automatically a full one.
//!
//! Issue/MR population is governed by `search.population`. The default
//! (`auto`, resolving to `tracked`) never enumerates the instance or the
//! membership: it refreshes only *tracked* projects — those with recent
//! local evidence of relevance (assigned issues/MRs, time-tracking history,
//! member-project live-search hits; see [`crate::search::TrackedProject`])
//! — and a full sync additionally evicts projects whose evidence went
//! stale, pruning their corpus entries. A tracked project that permanently
//! rejects its fetch (403/404) is skipped, never fatal. The eager modes (`all`, `member`) fetch instance-
//! or membership-wide and reconcile deletions with global keep-sets.
//! Assigned MRs are always fetched directly (`scope=assigned_to_me`), so the
//! assigned-MR view never depends on population coverage.
//!
//! [`Handlers::sync_search_cache`] is entirely self-gating, so the warm-up,
//! the periodic loop, and `ClearCache` can all call it unconditionally. The
//! gate lives in [`SearchCache`](crate::search::SearchCache) itself: every
//! mutation requires a [`SyncGuard`], so no write path can bypass it.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::config::SearchPopulation;
use crate::error::Error;
use crate::gitlab::GitlabApi;
use crate::search::{SEARCH_SCHEMA_VERSION, SearchMr, SearchProject, SyncGuard, SyncStamps};

use super::{Handlers, now_secs};

/// Safety margin subtracted from the incremental cursor so entries updated
/// while the previous sync was in flight (or under modest clock skew against
/// GitLab) are fetched again instead of missed. Upserts are idempotent, so
/// the overlap only costs a little re-download.
const UPDATED_AFTER_OVERLAP: Duration = Duration::from_secs(300);

/// Issue count after a full `all`-population sync above which we warn that
/// a leaner population is probably the better fit. Near gitlab.com's
/// offset-pagination cap; a corpus this size works but strains the instance
/// and the initial sync.
const LARGE_CORPUS_WARN: usize = 50_000;

/// Tracked-project count above which we warn: per-project refreshes at this
/// scale approach the eager `member` cost the tracked mode exists to avoid.
const LARGE_TRACKED_WARN: usize = 500;

/// What `SearchPopulation` resolved to for one sync run — `Auto` is gone by
/// the time fetches happen.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Population {
    All,
    Member,
    Tracked,
}

/// How one [`Handlers::sync_all_kinds`] run ended.
#[derive(PartialEq)]
enum Outcome {
    Ok,
    Failed,
    /// The *global* (`scope=all`) issues/MR fetch was rejected outright
    /// (permanent error, e.g. gitlab.com's 500 on unfiltered `scope=all`).
    /// Only reachable under an explicit `population = "all"`; the caller
    /// logs advice to switch populations.
    GlobalRejected,
}

impl Handlers {
    /// Sync the search cache if a cadence is due. Self-gating: no-ops when
    /// throttled (neither cadence due — the restart-storm guard), dormant, or
    /// when another sync is already in flight. Errors are logged and not
    /// propagated; a failed sync leaves the stamps untouched so the next tick
    /// retries.
    ///
    /// The gate is taken before the stamps are read, so a concurrent
    /// `ClearCache` can't zero the stamps between the read and the sync — a
    /// due sync always sees the stamps it will overwrite.
    pub async fn sync_search_cache(&self) {
        let (partial_secs, full_secs, population) = {
            let c = self.config.read().unwrap();
            (
                c.search.partial_interval().as_secs(),
                c.search.full_interval().as_secs(),
                c.search.population,
            )
        };
        let Some(guard) = self.search.try_begin_sync() else {
            debug!("search sync already in flight; skipping");
            return;
        };
        let mut stamps = match self.search.stamps() {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "search sync: stamp read failed");
                return;
            }
        };
        // A stamp from an older entry schema is treated as never-synced: only
        // a full resync rewrites every row, so rows carrying `serde(default)`
        // values for the new fields would otherwise linger until the next
        // scheduled full sync (up to a week).
        if stamps.schema_version < SEARCH_SCHEMA_VERSION {
            if stamps.last_partial_sync_secs != 0 {
                info!(
                    from = stamps.schema_version,
                    to = SEARCH_SCHEMA_VERSION,
                    "search entry schema changed; forcing a full resync"
                );
            }
            stamps = SyncStamps::default();
        }
        let now = now_secs();
        let full_due = stamps.last_full_sync_secs == 0
            || now.saturating_sub(stamps.last_full_sync_secs) >= full_secs;
        let partial_due =
            full_due || now.saturating_sub(stamps.last_partial_sync_secs) >= partial_secs;
        if !partial_due {
            debug!("search sync throttled; last sync is fresh");
            return;
        }
        let session = match self.current_session().await {
            Ok(s) => s,
            Err(_) => {
                debug!("dormant; skipping search sync");
                return;
            }
        };
        let user_id = session.user_id;
        let gitlab = session.gitlab;

        let effective = match population {
            SearchPopulation::All => Population::All,
            SearchPopulation::Member => Population::Member,
            SearchPopulation::Auto | SearchPopulation::Tracked => Population::Tracked,
        };

        // Captured before any fetch so the next incremental cursor overlaps
        // everything updated while this sync was running.
        let started = now_secs();
        let updated_after = (!full_due).then(|| {
            let cursor = stamps
                .last_partial_sync_secs
                .saturating_sub(UPDATED_AFTER_OVERLAP.as_secs());
            chrono::DateTime::<chrono::Utc>::from_timestamp(cursor as i64, 0)
                .unwrap_or_else(chrono::Utc::now)
        });

        let outcome = self
            .sync_all_kinds(&guard, &gitlab, effective, updated_after)
            .await;
        if outcome == Outcome::GlobalRejected {
            warn!(
                "instance rejected the global scope=all fetch; \
                 consider [search] population = \"tracked\" or \"member\""
            );
        }
        if outcome != Outcome::Ok {
            return;
        }

        let fresh = SyncStamps {
            last_partial_sync_secs: started,
            last_full_sync_secs: if full_due {
                started
            } else {
                stamps.last_full_sync_secs
            },
            degraded_to_member: false,
            schema_version: SEARCH_SCHEMA_VERSION,
            synced_user_id: user_id,
        };
        if let Err(e) = guard.set_stamps(&fresh) {
            warn!(error = %e, "search sync: stamp write failed");
        }
    }

    /// Fetch and store every kind. `updated_after = None` is the full resync:
    /// issues and MRs are fetched in full and entries GitLab no longer returns
    /// are dropped (globally for the eager populations, per tracked project
    /// otherwise). `Some(cursor)` is the incremental sync: issues and MRs are
    /// delta-fetched and upserted only. Projects and groups are always the
    /// full membership lists — they are small and have no reliable delta
    /// filter — so they stay exact on every sync, as do the directly-fetched
    /// assigned MRs.
    ///
    /// A permanent rejection of a *global* (`Population::All`) issue/MR fetch
    /// returns [`Outcome::GlobalRejected`]; any other fetch failure is
    /// [`Outcome::Failed`]. Already-landed upserts are idempotent and
    /// harmless, and each kind's deletion diff runs only after its own
    /// successful fetch.
    async fn sync_all_kinds(
        &self,
        guard: &SyncGuard<'_>,
        gitlab: &Arc<dyn GitlabApi>,
        population: Population,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Outcome {
        let projects = match gitlab.fetch_member_projects().await {
            Ok(p) => p,
            Err(e) => return self.fail_search_fetch(gitlab, "projects", &e).await,
        };
        log_store("projects", guard.upsert_projects(&projects));
        let keep: HashSet<u64> = projects.iter().map(|p| p.id as u64).collect();
        log_retain("projects", guard.retain_projects(&keep));

        let groups = match gitlab.fetch_member_groups().await {
            Ok(g) => g,
            Err(e) => return self.fail_search_fetch(gitlab, "groups", &e).await,
        };
        log_store("groups", guard.upsert_groups(&groups));
        let keep: HashSet<u64> = groups.iter().map(|g| g.id as u64).collect();
        log_retain("groups", guard.retain_groups(&keep));

        // Assigned MRs are fetched directly (`scope=assigned_to_me`) so the
        // assigned-MR view never depends on how broadly the population below
        // covers the instance. Runs for every population mode — one cheap
        // call — and its ids join the full-sync keep-sets so a population
        // fetch that misses them can't prune them right back out.
        let assigned_mrs = match gitlab.fetch_assigned_merge_requests().await {
            Ok(m) => m,
            Err(e) => {
                return self
                    .fail_search_fetch(gitlab, "assigned merge requests", &e)
                    .await;
            }
        };
        log_store("assigned merge requests", guard.upsert_mrs(&assigned_mrs));

        match population {
            Population::Tracked => {
                self.sync_tracked(guard, gitlab, updated_after, &assigned_mrs)
                    .await
            }
            Population::All => {
                self.sync_eager(guard, gitlab, true, updated_after, &assigned_mrs, &projects)
                    .await
            }
            Population::Member => {
                self.sync_eager(
                    guard,
                    gitlab,
                    false,
                    updated_after,
                    &assigned_mrs,
                    &projects,
                )
                .await
            }
        }
    }

    /// The tracked population: refresh only projects with recent local
    /// evidence of relevance. Never enumerates the membership, so the cost
    /// scales with what the user actually touches, not with the instance.
    async fn sync_tracked(
        &self,
        guard: &SyncGuard<'_>,
        gitlab: &Arc<dyn GitlabApi>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
        assigned_mrs: &[SearchMr],
    ) -> Outcome {
        let full = updated_after.is_none();
        let retention = { self.config.read().unwrap().search.tracked_retention() };
        let now = now_secs();
        let cutoff = now.saturating_sub(retention.as_secs());

        // Evidence pass: every project seen in the assigned-issue cache, the
        // direct assigned-MR fetch, or the recent history window re-earns its
        // tracked slot now. Live-search hits add theirs at search time. Local
        // read failures only cost evidence freshness, never the sync.
        let mut evidence: HashSet<i64> = assigned_mrs.iter().map(|m| m.project_id).collect();
        match self.cache.get() {
            Ok(issues) => evidence.extend(issues.into_iter().flatten().map(|i| i.project_id)),
            Err(e) => {
                warn!(error = %e, "tracked sync: issue cache read failed; evidence incomplete");
            }
        }
        match self.history.all_since(cutoff) {
            Ok(events) => evidence.extend(events.iter().map(|t| t.project_id)),
            Err(e) => warn!(error = %e, "tracked sync: history read failed; evidence incomplete"),
        }
        if let Err(e) = guard.note_tracked(evidence, now) {
            warn!(error = %e, "tracked sync: evidence write failed");
        }

        // Retention sweep, full tier only: drop projects whose evidence went
        // stale, then their (now unrefreshed-forever) corpus leftovers. Also
        // the migration path that shrinks a corpus inherited from an eager
        // population down to the tracked set.
        if full {
            match guard.evict_tracked(cutoff) {
                Ok(evicted) if evicted.is_empty() => {}
                Ok(evicted) => info!(
                    count = evicted.len(),
                    "evicted tracked projects without recent evidence"
                ),
                Err(e) => warn!(error = %e, "tracked sync: eviction failed"),
            }
        }

        let mut tracked: Vec<i64> = match self.search.tracked_projects() {
            Ok(t) => t.into_iter().map(|(id, _)| id).collect(),
            Err(e) => {
                warn!(error = %e, "tracked sync: tracked set read failed");
                return Outcome::Failed;
            }
        };
        tracked.sort_unstable();

        if full {
            let keep: HashSet<i64> = tracked.iter().copied().collect();
            match guard.prune_untracked(&keep) {
                Ok((0, 0)) => {}
                Ok((issues, mrs)) => {
                    info!(issues, mrs, "pruned corpus entries of untracked projects");
                }
                Err(e) => warn!(error = %e, "tracked sync: corpus prune failed"),
            }
        }

        if tracked.len() > LARGE_TRACKED_WARN {
            warn!(
                count = tracked.len(),
                "tracked project set is very large; per-project refreshes will strain GitLab"
            );
        }

        // A permanent rejection of one project's fetch (403 when a feature
        // is disabled or access was lost, 404 after deletion) must not
        // abort the run — that would starve every other tracked project's
        // refresh forever, since the stamps only advance on success. Skip
        // the kind and move on: access coming back heals on a later sync,
        // and a project that stops earning evidence ages out via retention.
        // Transient failures still abort (and demote) — the network being
        // down is not a per-project condition.
        let mut issue_count = 0usize;
        let mut mr_count = 0usize;
        let mut skipped = 0usize;
        for &pid in &tracked {
            match gitlab
                .fetch_issues_for_search(Some(pid), updated_after)
                .await
            {
                Ok(issues) => {
                    issue_count += issues.len();
                    log_store("issues", guard.upsert_issues(&issues));
                    if full {
                        let keep: HashSet<u64> = issues.iter().map(|i| i.id as u64).collect();
                        log_retain("issues", guard.retain_issues_in_project(pid, &keep));
                    }
                }
                Err(e @ Error::Gitlab(_)) => {
                    warn!(
                        error = %e,
                        project = pid,
                        "tracked project rejected the issue fetch; skipping"
                    );
                    skipped += 1;
                }
                Err(e) => return self.fail_search_fetch(gitlab, "issues", &e).await,
            }

            match gitlab
                .fetch_merge_requests_for_search(Some(pid), updated_after)
                .await
            {
                Ok(mrs) => {
                    mr_count += mrs.len();
                    log_store("merge requests", guard.upsert_mrs(&mrs));
                    if full {
                        let keep: HashSet<u64> = mrs
                            .iter()
                            .chain(assigned_mrs.iter().filter(|m| m.project_id == pid))
                            .map(|m| m.id as u64)
                            .collect();
                        log_retain("merge requests", guard.retain_mrs_in_project(pid, &keep));
                    }
                }
                Err(e @ Error::Gitlab(_)) => {
                    warn!(
                        error = %e,
                        project = pid,
                        "tracked project rejected the MR fetch; skipping"
                    );
                    skipped += 1;
                }
                Err(e) => return self.fail_search_fetch(gitlab, "merge requests", &e).await,
            }
        }

        info!(
            full,
            tracked = tracked.len(),
            issues = issue_count,
            merge_requests = mr_count,
            skipped,
            "tracked search sync complete"
        );
        Outcome::Ok
    }

    /// The eager populations: `global` fetches the instance-wide `scope=all`
    /// endpoints once, otherwise one fetch per membership project. Deletion
    /// reconciliation uses global keep-sets, which is only sound because the
    /// fetch covered the whole population.
    async fn sync_eager(
        &self,
        guard: &SyncGuard<'_>,
        gitlab: &Arc<dyn GitlabApi>,
        global: bool,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
        assigned_mrs: &[SearchMr],
        projects: &[SearchProject],
    ) -> Outcome {
        let full = updated_after.is_none();

        let issues = if global {
            match gitlab.fetch_issues_for_search(None, updated_after).await {
                Ok(i) => i,
                Err(e) => {
                    let rejected = matches!(e, Error::Gitlab(_));
                    self.fail_search_fetch(gitlab, "issues", &e).await;
                    return if rejected {
                        Outcome::GlobalRejected
                    } else {
                        Outcome::Failed
                    };
                }
            }
        } else {
            let mut acc = Vec::new();
            for p in projects {
                match gitlab
                    .fetch_issues_for_search(Some(p.id), updated_after)
                    .await
                {
                    Ok(i) => acc.extend(i),
                    Err(e) => return self.fail_search_fetch(gitlab, "issues", &e).await,
                }
            }
            acc
        };
        log_store("issues", guard.upsert_issues(&issues));
        if full {
            let keep: HashSet<u64> = issues.iter().map(|i| i.id as u64).collect();
            log_retain("issues", guard.retain_issues(&keep));
        }

        let mrs = if global {
            match gitlab
                .fetch_merge_requests_for_search(None, updated_after)
                .await
            {
                Ok(m) => m,
                Err(e) => {
                    let rejected = matches!(e, Error::Gitlab(_));
                    self.fail_search_fetch(gitlab, "merge requests", &e).await;
                    return if rejected {
                        Outcome::GlobalRejected
                    } else {
                        Outcome::Failed
                    };
                }
            }
        } else {
            let mut acc = Vec::new();
            for p in projects {
                match gitlab
                    .fetch_merge_requests_for_search(Some(p.id), updated_after)
                    .await
                {
                    Ok(m) => acc.extend(m),
                    Err(e) => {
                        return self.fail_search_fetch(gitlab, "merge requests", &e).await;
                    }
                }
            }
            acc
        };
        log_store("merge requests", guard.upsert_mrs(&mrs));
        if full {
            let keep: HashSet<u64> = mrs
                .iter()
                .chain(assigned_mrs.iter())
                .map(|m| m.id as u64)
                .collect();
            log_retain("merge requests", guard.retain_mrs(&keep));
        }

        info!(
            full,
            issues = issues.len(),
            merge_requests = mrs.len(),
            projects = projects.len(),
            "search sync complete"
        );
        if full && global && issues.len() > LARGE_CORPUS_WARN {
            warn!(
                count = issues.len(),
                "search corpus is very large; consider [search] population = \"tracked\""
            );
        }
        Outcome::Ok
    }

    /// Log a failed search fetch and route it into the session state (a
    /// transient error demotes and wakes the reconnect supervisor, see
    /// [`Handlers::note_gitlab_error`]). Always [`Outcome::Failed`], so fetch
    /// arms can `return self.fail_search_fetch(..).await`.
    async fn fail_search_fetch(
        &self,
        gitlab: &Arc<dyn GitlabApi>,
        kind: &str,
        e: &Error,
    ) -> Outcome {
        warn!(error = %e, kind, "search sync: GitLab fetch failed");
        self.note_gitlab_error(gitlab, e).await;
        Outcome::Failed
    }
}

fn log_store(kind: &str, result: crate::error::Result<()>) {
    if let Err(e) = result {
        warn!(error = %e, kind, "search cache write failed");
    }
}

fn log_retain(kind: &str, result: crate::error::Result<usize>) {
    match result {
        Ok(0) => {}
        Ok(n) => info!(removed = n, kind, "search cache dropped deleted entries"),
        Err(e) => warn!(error = %e, kind, "search cache prune failed"),
    }
}
