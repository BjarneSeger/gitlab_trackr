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
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping search sync");
                return;
            }
        };
        let Ok(_gate) = self.search_sync_gate.try_lock() else {
            debug!("search sync already in flight; skipping");
            return;
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

        if !self
            .sync_all_kinds(&gitlab, population, updated_after)
            .await
        {
            return;
        }

        let fresh = SyncStamps {
            last_partial_sync_secs: started,
            last_full_sync_secs: if full_due {
                started
            } else {
                stamps.last_full_sync_secs
            },
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
    /// Returns `false` on any fetch failure; already-landed upserts are
    /// idempotent and harmless, and each kind's deletion diff runs only after
    /// its own successful fetch.
    async fn sync_all_kinds(
        &self,
        gitlab: &Arc<dyn GitlabApi>,
        population: SearchPopulation,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> bool {
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
            SearchPopulation::All => {
                match gitlab.fetch_issues_for_search(None, updated_after).await {
                    Ok(i) => i,
                    Err(e) => return self.fail_search_fetch(gitlab, "issues", &e).await,
                }
            }
            SearchPopulation::Member => {
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
            SearchPopulation::All => {
                match gitlab
                    .fetch_merge_requests_for_search(None, updated_after)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => return self.fail_search_fetch(gitlab, "merge requests", &e).await,
                }
            }
            SearchPopulation::Member => {
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
        true
    }

    /// Log a failed search fetch and route it into the session state (a
    /// transient error demotes and wakes the reconnect supervisor, see
    /// [`Handlers::note_gitlab_error`]). Always `false`, so fetch arms can
    /// `return self.fail_search_fetch(..).await`.
    async fn fail_search_fetch(&self, gitlab: &Arc<dyn GitlabApi>, kind: &str, e: &Error) -> bool {
        warn!(error = %e, kind, "search sync: GitLab fetch failed");
        self.note_gitlab_error(gitlab, e).await;
        false
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
