//! Background cache and history refresh — the daemon's population paths.
//!
//! The public entry points ([`Handlers::refresh_cache`],
//! [`Handlers::refresh_history_daily`], [`Handlers::backfill_history`],
//! [`Handlers::warm_up`]) are driven by the background loops in `main.rs` and
//! the reconnect supervisor; the read handlers stay pure cache readers because
//! this owns freshness.
//!
//! Each tier is self-gating on the persisted [`RefreshStamps`] (the pattern
//! [`super::search_sync`] established): a stamp advances only after a
//! successful run, so callers invoke unconditionally, a restart inside an
//! interval costs GitLab nothing, and a failed or dormant run leaves the
//! stamp untouched for the next tick — or the post-reconnect warm-up — to
//! retry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use gitlab_trackr_api::Issue;

use crate::boards::BoardCache;
use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};
use crate::history::StoredTimelog;
use crate::refresh_meta::RefreshStamps;

use super::{Handlers, now_secs};

impl Handlers {
    /// Fetch issues and boards from GitLab and update both caches, if the
    /// quick cadence is due. Self-gating (see the module doc); errors are
    /// logged and not propagated.
    pub async fn refresh_cache(&self) {
        let quick_secs = self
            .config
            .read()
            .unwrap()
            .refresh
            .quick
            .interval()
            .as_secs();
        if !self.refresh_due("quick refresh", |s, now| {
            s.last_quick_sync_secs == 0 || now.saturating_sub(s.last_quick_sync_secs) >= quick_secs
        }) {
            return;
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping background cache refresh");
                return;
            }
        };

        // Captured before any fetch, so nothing updated during the run is
        // stamped over.
        let started = now_secs();
        match gitlab.fetch_assigned_issues(None).await {
            Ok(raw) => {
                let issues = enrich_graph_status(&*gitlab, &self.boards, raw).await;
                if let Err(e) = self.cache.put(&issues) {
                    warn!(error = %e, "background cache write failed");
                    return;
                }
                info!(count = issues.len(), "background cache refresh complete");
            }
            Err(e) => {
                warn!(error = %e, "background cache refresh: GitLab fetch failed");
                self.note_gitlab_error(&gitlab, &e).await;
                return;
            }
        }

        let quick_window = self.config.read().unwrap().refresh.quick.window();
        if self.refresh_history_window(&gitlab, quick_window).await {
            self.stamp(|s| s.last_quick_sync_secs = started);
        }
    }

    /// Slow-tier refresh of the bulk history window followed by a prune of
    /// anything past the retention horizon, if the slow cadence is due.
    /// Self-gating; no-op when dormant. Called by the daily background loop.
    pub async fn refresh_history_daily(&self) {
        let slow_secs = self
            .config
            .read()
            .unwrap()
            .refresh
            .slow
            .interval()
            .as_secs();
        if !self.refresh_due("daily history refresh", |s, now| {
            s.last_slow_sync_secs == 0 || now.saturating_sub(s.last_slow_sync_secs) >= slow_secs
        }) {
            return;
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping daily history refresh");
                return;
            }
        };
        let started = now_secs();
        let slow_window = self.config.read().unwrap().refresh.slow.window();
        if self.refresh_history_window(&gitlab, slow_window).await {
            self.stamp(|s| s.last_slow_sync_secs = started);
        }
        self.prune_history();
    }

    /// Backfill of the full retention window (up to 90d) so the older,
    /// never-refreshed history is populated. Shares the slow stamp with
    /// [`Self::refresh_history_daily`] — the windows overlap and upserts
    /// dedupe — but additionally runs when the configured retention outgrew
    /// the widest window ever backfilled. Self-gating; no-op when dormant.
    /// Called at startup and after a full cache clear.
    pub async fn backfill_history(&self) {
        let (slow_secs, retention_hours, retention) = {
            let c = self.config.read().unwrap();
            (
                c.refresh.slow.interval().as_secs(),
                c.history.retention_hours,
                c.history.retention(),
            )
        };
        if !self.refresh_due("history backfill", |s, now| {
            s.last_slow_sync_secs == 0
                || now.saturating_sub(s.last_slow_sync_secs) >= slow_secs
                || s.backfilled_retention_hours < retention_hours
        }) {
            return;
        }
        let gitlab = match self.gitlab().await {
            Ok(g) => g,
            Err(_) => {
                debug!("dormant; skipping history backfill");
                return;
            }
        };
        let started = now_secs();
        if self.refresh_history_window(&gitlab, retention).await {
            self.stamp(|s| {
                s.last_slow_sync_secs = started;
                s.backfilled_retention_hours = retention_hours;
            });
        }
        self.prune_history();
    }

    /// Warm the caches from scratch: refresh issues/boards, then backfill the
    /// full history window. Order matters — history enrichment reads project IDs
    /// from the issue cache, so issues must land first; when the stamp-gated
    /// refresh skips, enrichment reads the *persisted* issue cache, so the
    /// guarantee holds across restarts (a first boot has zero stamps and runs
    /// everything). All steps are no-ops while dormant and gate on their
    /// persisted stamps, so a warm-up shortly after the last run — a restart,
    /// a reconnect flap — does not re-poll GitLab. Shared by the startup
    /// warm-up, the post-reconnect recovery, and the full cache-clear refill
    /// (which zeroes the stamps first).
    pub async fn warm_up(&self) {
        self.refresh_cache().await;
        self.backfill_history().await;
        self.sync_search_cache().await;
    }

    /// The shared due-check prologue: read the persisted stamps and evaluate
    /// `due(stamps, now)`. A stamp-read failure or a fresh stamp means "don't
    /// run" — the caller returns without fetching or stamping.
    fn refresh_due(&self, what: &str, due: impl FnOnce(&RefreshStamps, u64) -> bool) -> bool {
        let stamps = match self.refresh_meta.stamps() {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, what, "stamp read failed");
                return false;
            }
        };
        if due(&stamps, now_secs()) {
            true
        } else {
            debug!("{what} throttled; last run is fresh");
            false
        }
    }

    /// Advance the persisted stamps after a successful run; a write failure
    /// only costs one redundant refetch, so it is logged and swallowed.
    fn stamp(&self, f: impl FnOnce(&mut RefreshStamps)) {
        if let Err(e) = self.refresh_meta.update(f) {
            warn!(error = %e, "refresh stamp write failed");
        }
    }

    /// Pull the user's GitLab timelogs spanning `window` back from now, enrich
    /// with cached issue data, and store. Best-effort — each step's failure is
    /// logged and swallowed; the `bool` reports success so the stamp-gated
    /// callers know whether to advance their stamp. Pruning is a separate step
    /// (see [`Self::prune_history`]) so the frequent quick refresh doesn't
    /// scan the whole table every time.
    #[must_use]
    pub(crate) async fn refresh_history_window(
        &self,
        gitlab: &Arc<dyn GitlabApi>,
        window: Duration,
    ) -> bool {
        let now = now_secs();
        let cutoff = now.saturating_sub(window.as_secs());
        let since = chrono::DateTime::<chrono::Utc>::from_timestamp(cutoff as i64, 0)
            .unwrap_or_else(chrono::Utc::now);

        let fetched = match gitlab.fetch_my_timelogs(since).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "timelog refresh: GitLab fetch failed");
                self.note_gitlab_error(gitlab, &e).await;
                return false;
            }
        };

        let cached_issues = self.cache.get().ok().flatten().unwrap_or_default();
        let by_url: HashMap<&str, &Issue> = cached_issues
            .iter()
            .map(|i| (i.web_url.as_str(), i))
            .collect();
        let by_iid: HashMap<i64, &Issue> = cached_issues.iter().map(|i| (i.iid, i)).collect();

        let stored: Vec<StoredTimelog> = fetched
            .into_iter()
            .map(|t| enrich_timelog(t, &by_url, &by_iid))
            .collect();

        if let Err(e) = self.history.upsert(&stored) {
            warn!(error = %e, "history upsert failed");
            false
        } else {
            info!(
                count = stored.len(),
                window_secs = window.as_secs(),
                "history refresh complete"
            );
            true
        }
    }

    /// Drop history entries that have aged past the retention horizon.
    pub(crate) fn prune_history(&self) {
        let retention_secs = self.config.read().unwrap().history.retention().as_secs();
        let cutoff = now_secs().saturating_sub(retention_secs);
        match self.history.prune(cutoff) {
            Ok(0) => {}
            Ok(n) => info!(removed = n, "pruned stale history entries"),
            Err(e) => warn!(error = %e, "history prune failed"),
        }
    }
}

/// Fill `graph_status` on each issue using cached or freshly-fetched board
/// list labels. Best-effort: a board fetch failure for a project leaves
/// that project's issues with an empty `graph_status`.
pub(crate) async fn enrich_graph_status(
    gitlab: &dyn GitlabApi,
    boards: &BoardCache,
    raw: Vec<IssueWithLabels>,
) -> Vec<Issue> {
    let mut by_project: HashMap<i64, Option<Vec<String>>> = HashMap::new();
    let mut out = Vec::with_capacity(raw.len());

    for IssueWithLabels { mut issue, labels } in raw {
        let project_id = issue.project_id;

        let board_labels = match by_project.get(&project_id) {
            Some(entry) => entry.clone(),
            None => {
                let resolved = match boards.get(project_id) {
                    Ok(Some(cached)) => Some(cached),
                    Ok(None) => match gitlab.fetch_board_list_labels(project_id).await {
                        Ok(fetched) => {
                            if let Err(e) = boards.put(project_id, fetched.clone()) {
                                warn!(error = %e, project_id, "failed to persist board labels");
                            }
                            Some(fetched)
                        }
                        Err(e) => {
                            warn!(error = %e, project_id, "board fetch failed; graph_status will be empty");
                            None
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, project_id, "board cache read failed");
                        None
                    }
                };
                by_project.insert(project_id, resolved.clone());
                resolved
            }
        };

        issue.graph_status = graph_status_from(board_labels.as_deref(), &labels, &issue.state);

        out.push(issue);
    }

    out
}

/// The board-derived `graph_status` for an issue: the first of its labels that
/// appears in the project's board lists, the issue's state when none matches,
/// or empty when the board labels are unknown. Shared by the assigned-issues
/// enrichment above and the `Search` reply mapping.
pub(crate) fn graph_status_from(
    board_labels: Option<&[String]>,
    labels: &[String],
    state: &str,
) -> String {
    match board_labels {
        Some(board) => labels
            .iter()
            .find(|l| board.iter().any(|b| b == *l))
            .cloned()
            .unwrap_or_else(|| state.to_string()),
        None => String::new(),
    }
}

/// Fill `project_id` on a fetched timelog from the issue cache.
///
/// GitLab's GraphQL `Timelog.issue` doesn't expose `project_id` directly, so
/// we match by `web_url` first (exact, robust) and fall back to `iid` (which
/// can collide across projects but is better than nothing). If neither hits,
/// `project_id` stays at `0` — the client can still display the entry.
pub(crate) fn enrich_timelog(
    t: FetchedTimelog,
    by_url: &HashMap<&str, &Issue>,
    by_iid: &HashMap<i64, &Issue>,
) -> StoredTimelog {
    let issue = by_url
        .get(t.web_url.as_str())
        .or_else(|| by_iid.get(&t.issue_iid));
    StoredTimelog {
        timelog_id: t.timelog_id,
        spent_at_secs: t.spent_at_secs,
        project_id: issue.map(|i| i.project_id).unwrap_or(0),
        issue_iid: t.issue_iid,
        issue_title: if t.issue_title.is_empty() {
            issue.map(|i| i.title.clone()).unwrap_or_default()
        } else {
            t.issue_title
        },
        web_url: if t.web_url.is_empty() {
            issue.map(|i| i.web_url.clone()).unwrap_or_default()
        } else {
            t.web_url
        },
        duration: t.duration,
        summary: t.summary,
    }
}
