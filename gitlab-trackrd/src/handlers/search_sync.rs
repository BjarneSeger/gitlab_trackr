//! Background sync for the search cache (issues, MRs, projects, groups).
//!
//! Unlike the issue/history tiers in [`super::refresh`], this owns its own
//! schedule via the cache-global [`SyncStamps`]: an incremental sync
//! (`updated_after` deltas, upsert-only) runs at most once per
//! `search.partial_interval_secs`, and a full resync — which also reconciles
//! deletions by dropping entries GitLab no longer returns — once per
//! `search.full_interval_secs`. Both stamps persist, so a daemon restart
//! inside the partial window costs GitLab nothing, and the very first sync
//! (zeroed stamps) is automatically a full one.
//!
//! [`Handlers::sync_search_cache`] is entirely self-gating, so the warm-up,
//! the periodic loop, and `ClearCache` can all call it unconditionally.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::config::SearchPopulation;
use crate::error::Error;
use crate::gitlab::GitlabApi;
use crate::search::SyncStamps;

use super::{Handlers, now_secs};

/// Safety margin subtracted from the incremental cursor so entries updated
/// while the previous sync was in flight (or under modest clock skew against
/// GitLab) are fetched again instead of missed. Upserts are idempotent, so
/// the overlap only costs a little re-download.
const UPDATED_AFTER_OVERLAP: Duration = Duration::from_secs(300);

/// Issue count after a full `all`-population sync above which we warn that
/// `population = "member"` is probably the better fit. Near gitlab.com's
/// offset-pagination cap; a corpus this size works but strains the instance
/// and the initial sync.
const LARGE_CORPUS_WARN: usize = 50_000;

/// What `SearchPopulation` resolved to for one sync run — `Auto` is gone by
/// the time fetches happen.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Population {
    All,
    Member,
}

/// How one [`Handlers::sync_all_kinds`] run ended.
#[derive(PartialEq)]
enum Outcome {
    Ok,
    Failed,
    /// The *global* (`scope=all`) issues/MR fetch was rejected outright
    /// (permanent error, e.g. gitlab.com's 500 on unfiltered `scope=all`).
    /// Under `population = "auto"` the caller retries the sync with
    /// [`Population::Member`].
    GlobalRejected,
}

/// gitlab.com rejects the unfiltered global `scope=all` fetch outright, so
/// `population = "auto"` never attempts it there.
fn is_gitlab_com(host: &str) -> bool {
    host.eq_ignore_ascii_case("gitlab.com")
}

impl Handlers {
    /// Sync the search cache if a cadence is due. Self-gating: no-ops when
    /// throttled (neither cadence due — the restart-storm guard), dormant, or
    /// when another sync is already in flight. Errors are logged and not
    /// propagated; a failed sync leaves the stamps untouched so the next tick
    /// retries.
    pub async fn sync_search_cache(&self) {
        let (partial_secs, full_secs, population) = {
            let c = self.config.read().unwrap();
            (
                c.search.partial_interval().as_secs(),
                c.search.full_interval().as_secs(),
                c.search.population,
            )
        };
        let stamps = match self.search.stamps() {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "search sync: stamp read failed");
                return;
            }
        };
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
        let gitlab = session.gitlab;
        let Ok(_gate) = self.search_sync_gate.try_lock() else {
            debug!("search sync already in flight; skipping");
            return;
        };

        // Resolve `auto` against the host and the sticky degrade flag. The
        // flag is honored between full syncs only — a due full sync retries
        // the global fetch, so a recovered instance heals automatically.
        let auto = population == SearchPopulation::Auto;
        let mut degraded = false;
        let mut effective = match population {
            SearchPopulation::All => Population::All,
            SearchPopulation::Member => Population::Member,
            SearchPopulation::Auto => {
                if is_gitlab_com(&session.host) {
                    debug!("population=auto on gitlab.com; using member fetches");
                    Population::Member
                } else if !full_due && stamps.degraded_to_member {
                    debug!(
                        "population=auto is degraded to member fetches; \
                         the global fetch is retried at the next full sync"
                    );
                    degraded = true;
                    Population::Member
                } else {
                    Population::All
                }
            }
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

        let mut outcome = self.sync_all_kinds(&gitlab, effective, updated_after).await;
        if outcome == Outcome::GlobalRejected {
            if auto {
                warn!(
                    "instance rejected the global scope=all fetch; degrading to \
                     member-project fetches until the next full sync \
                     (set [search] population explicitly to silence this)"
                );
                degraded = true;
                effective = Population::Member;
                outcome = self.sync_all_kinds(&gitlab, effective, updated_after).await;
            } else {
                warn!(
                    "instance rejected the global scope=all fetch; \
                     consider [search] population = \"member\""
                );
            }
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
            degraded_to_member: degraded,
        };
        if let Err(e) = self.search.set_stamps(&fresh) {
            warn!(error = %e, "search sync: stamp write failed");
        }
    }

    /// Fetch and store every kind. `updated_after = None` is the full resync:
    /// issues and MRs are fetched in full and entries GitLab no longer returns
    /// are dropped. `Some(cursor)` is the incremental sync: issues and MRs are
    /// delta-fetched and upserted only. Projects and groups are always the
    /// full membership lists — they are small and have no reliable delta
    /// filter — so they stay exact on every sync.
    ///
    /// A permanent rejection of a *global* (`Population::All`) issue/MR fetch
    /// returns [`Outcome::GlobalRejected`] so the auto-population caller can
    /// retry with member fetches; any other fetch failure is
    /// [`Outcome::Failed`]. Already-landed upserts are idempotent and
    /// harmless, and each kind's deletion diff runs only after its own
    /// successful fetch.
    async fn sync_all_kinds(
        &self,
        gitlab: &Arc<dyn GitlabApi>,
        population: Population,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Outcome {
        let full = updated_after.is_none();

        let projects = match gitlab.fetch_member_projects().await {
            Ok(p) => p,
            Err(e) => return self.fail_search_fetch(gitlab, "projects", &e).await,
        };
        log_store("projects", self.search.upsert_projects(&projects));
        let keep: HashSet<u64> = projects.iter().map(|p| p.id as u64).collect();
        log_retain("projects", self.search.retain_projects(&keep));

        let groups = match gitlab.fetch_member_groups().await {
            Ok(g) => g,
            Err(e) => return self.fail_search_fetch(gitlab, "groups", &e).await,
        };
        log_store("groups", self.search.upsert_groups(&groups));
        let keep: HashSet<u64> = groups.iter().map(|g| g.id as u64).collect();
        log_retain("groups", self.search.retain_groups(&keep));

        let issues = match population {
            Population::All => match gitlab.fetch_issues_for_search(None, updated_after).await {
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
            },
            Population::Member => {
                let mut acc = Vec::new();
                for p in &projects {
                    match gitlab
                        .fetch_issues_for_search(Some(p.id), updated_after)
                        .await
                    {
                        Ok(i) => acc.extend(i),
                        Err(e) => return self.fail_search_fetch(gitlab, "issues", &e).await,
                    }
                }
                acc
            }
        };
        log_store("issues", self.search.upsert_issues(&issues));
        if full {
            let keep: HashSet<u64> = issues.iter().map(|i| i.id as u64).collect();
            log_retain("issues", self.search.retain_issues(&keep));
        }

        let mrs = match population {
            Population::All => {
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
            }
            Population::Member => {
                let mut acc = Vec::new();
                for p in &projects {
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
            }
        };
        log_store("merge requests", self.search.upsert_mrs(&mrs));
        if full {
            let keep: HashSet<u64> = mrs.iter().map(|m| m.id as u64).collect();
            log_retain("merge requests", self.search.retain_mrs(&keep));
        }

        info!(
            full,
            issues = issues.len(),
            merge_requests = mrs.len(),
            projects = projects.len(),
            groups = groups.len(),
            "search sync complete"
        );
        if full && population == Population::All && issues.len() > LARGE_CORPUS_WARN {
            warn!(
                count = issues.len(),
                "search corpus is very large; consider [search] population = \"member\""
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
