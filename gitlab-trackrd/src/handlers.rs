//! Varlink method implementations — orchestration only.
//!
//! Each method is a short cascade: consult the cache, fall back to GitLab,
//! reply. GitLab errors become `GitlabError` varlink replies; cache failures
//! are logged and treated as a miss so the daemon stays available.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_ClearCache, Call_CloseIssue, Call_GetAssignedIssues, Call_GetHistory, Call_PostTime,
    HistoryEvent, Issue, VarlinkInterface,
};

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::error::Error;
use crate::gitlab::{FetchedTimelog, GitlabClient, IssueWithLabels};
use crate::history::{HISTORY_WINDOW, HistoryCache, StoredTimelog};
use crate::queue::RetryQueue;

pub struct Handlers {
    pub gitlab: Arc<GitlabClient>,
    pub cache: Arc<IssueCache>,
    pub boards: Arc<BoardCache>,
    pub history: Arc<HistoryCache>,
    pub queue: RetryQueue,
}

impl Handlers {
    /// Unconditionally fetch issues and boards from GitLab and update both caches.
    /// Called by the background refresh task; errors are logged and not propagated.
    pub async fn refresh_cache(&self) {
        match self.gitlab.fetch_assigned_issues(None).await {
            Ok(raw) => {
                let issues = self.enrich_graph_status(raw).await;
                if let Err(e) = self.cache.put(&issues) {
                    warn!(error = %e, "background cache write failed");
                } else {
                    info!(count = issues.len(), "background cache refresh complete");
                }
            }
            Err(e) => {
                warn!(error = %e, "background cache refresh: GitLab fetch failed");
            }
        }

        self.refresh_history().await;
    }

    /// Pull the user's recent GitLab timelogs, enrich with cached issue data,
    /// store, and prune anything past the 7-day window. Best-effort — each
    /// step's failure is logged and swallowed.
    async fn refresh_history(&self) {
        let now = now_secs();
        let cutoff = now.saturating_sub(HISTORY_WINDOW.as_secs());
        let since = chrono::DateTime::<chrono::Utc>::from_timestamp(cutoff as i64, 0)
            .unwrap_or_else(chrono::Utc::now);

        let fetched = match self.gitlab.fetch_my_timelogs(since).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "background timelog refresh: GitLab fetch failed");
                return;
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
        } else {
            info!(count = stored.len(), "history refresh complete");
        }
        match self.history.prune(cutoff) {
            Ok(0) => {}
            Ok(n) => info!(removed = n, "pruned stale history entries"),
            Err(e) => warn!(error = %e, "history prune failed"),
        }
    }

    /// Fill `graph_status` on each issue using cached or freshly-fetched board
    /// list labels. Best-effort: a board fetch failure for a project leaves
    /// that project's issues with an empty `graph_status`.
    async fn enrich_graph_status(&self, raw: Vec<IssueWithLabels>) -> Vec<Issue> {
        let mut by_project: HashMap<i64, Option<Vec<String>>> = HashMap::new();
        let mut out = Vec::with_capacity(raw.len());

        for IssueWithLabels { mut issue, labels } in raw {
            let project_id = issue.project_id;

            let board_labels = match by_project.get(&project_id) {
                Some(entry) => entry.clone(),
                None => {
                    let resolved = match self.boards.get(project_id) {
                        Ok(Some(cached)) => Some(cached),
                        Ok(None) => match self.gitlab.fetch_board_list_labels(project_id).await {
                            Ok(fetched) => {
                                if let Err(e) = self.boards.put(project_id, fetched.clone()) {
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

            issue.graph_status = match board_labels {
                Some(board) => labels
                    .iter()
                    .find(|l| board.iter().any(|b| b == *l))
                    .cloned()
                    .unwrap_or_else(|| issue.state.clone()),
                None => String::new(),
            };

            out.push(issue);
        }

        out
    }
}

#[async_trait::async_trait]
impl VarlinkInterface for Handlers {
    #[instrument(skip(self, call))]
    async fn get_assigned_issues(
        &self,
        call: &mut dyn Call_GetAssignedIssues,
        groups: Option<Vec<String>>,
    ) -> varlink::Result<()> {
        match self.cache.get() {
            Ok(Some(issues)) => {
                debug!(count = issues.len(), "cache hit");
                return call.reply(issues);
            }
            Ok(None) => debug!("cache miss, fetching from GitLab"),
            Err(e) => warn!("cache read failed, treating as miss: {e}"),
        }

        let fetched = if let Some(groups) = groups {
            self.gitlab.fetch_group_issues(groups).await
        } else {
            self.gitlab.fetch_assigned_issues(None).await
        };

        match fetched {
            Ok(raw) => {
                let issues = self.enrich_graph_status(raw).await;
                if let Err(e) = self.cache.put(&issues) {
                    warn!(error = %e, "cache write failed");
                }
                call.reply(issues)
            }
            Err(e) => {
                warn!(error = %e, "GitLab fetch failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn clear_cache(&self, call: &mut dyn Call_ClearCache) -> varlink::Result<()> {
        if let Err(e) = self.cache.clear() {
            warn!("issue cache clear failed: {e}");
        } else {
            info!("issue cache cleared");
        }
        if let Err(e) = self.boards.clear() {
            warn!("board cache clear failed: {e}");
        } else {
            info!("board cache cleared");
        }
        call.reply()
    }

    #[instrument(skip(self, call))]
    async fn post_time(
        &self,
        call: &mut dyn Call_PostTime,
        project_id: i64,
        issue_iid: i64,
        duration: String,
        summary: Option<String>,
    ) -> varlink::Result<()> {
        match self
            .gitlab
            .add_spent_time(project_id, issue_iid, &duration, summary.as_deref())
            .await
        {
            Ok(()) => {
                info!(project_id, issue_iid, duration, "posted time");
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "PostTime network error, queuing for retry");
                let issue_id = self.cache.get().ok().flatten().and_then(|issues| {
                    issues
                        .into_iter()
                        .find(|i| i.project_id == project_id && i.iid == issue_iid)
                        .map(|i| i.id)
                });
                self.queue
                    .post_time(project_id, issue_iid, duration, summary, issue_id)
                    .await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "PostTime rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    #[instrument(skip(self, call))]
    async fn get_history(&self, call: &mut dyn Call_GetHistory) -> varlink::Result<()> {
        let now = now_secs();
        let cutoff = now.saturating_sub(HISTORY_WINDOW.as_secs());

        let cached_issues = self.cache.get().ok().flatten().unwrap_or_default();
        let by_key: HashMap<(i64, i64), &Issue> = cached_issues
            .iter()
            .map(|i| ((i.project_id, i.iid), i))
            .collect();

        let mut events: Vec<HistoryEvent> = Vec::new();

        match self.queue.pending_post_time() {
            Ok(pending) => {
                for p in pending {
                    let issue = by_key.get(&(p.project_id, p.issue_iid));
                    events.push(HistoryEvent {
                        timestamp: p.queued_at_secs as i64,
                        source: "queued".to_string(),
                        project_id: p.project_id,
                        issue_iid: p.issue_iid,
                        issue_title: issue.map(|i| i.title.clone()).unwrap_or_default(),
                        web_url: issue.map(|i| i.web_url.clone()).unwrap_or_default(),
                        duration: p.duration,
                        summary: p.summary.unwrap_or_default(),
                    });
                }
            }
            Err(e) => warn!(error = %e, "queue scan failed; queued events omitted"),
        }

        match self.history.all_since(cutoff) {
            Ok(entries) => {
                for e in entries {
                    events.push(HistoryEvent {
                        timestamp: e.spent_at_secs as i64,
                        source: "gitlab".to_string(),
                        project_id: e.project_id,
                        issue_iid: e.issue_iid,
                        issue_title: e.issue_title,
                        web_url: e.web_url,
                        duration: e.duration,
                        summary: e.summary,
                    });
                }
            }
            Err(e) => warn!(error = %e, "history read failed; returning queued only"),
        }

        call.reply(events)
    }

    #[instrument(skip(self, call))]
    async fn close_issue(
        &self,
        call: &mut dyn Call_CloseIssue,
        project_id: i64,
        issue_iid: i64,
    ) -> varlink::Result<()> {
        match self.gitlab.close_issue(project_id, issue_iid).await {
            Ok(()) => {
                info!(project_id, issue_iid, "closed issue");
                call.reply()
            }
            Err(Error::Transient(ref e)) => {
                warn!(error = %e, project_id, issue_iid, "CloseIssue network error, queuing for retry");
                self.queue.close_issue(project_id, issue_iid).await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "CloseIssue rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }
}

/// Fill `project_id` on a fetched timelog from the issue cache.
///
/// GitLab's GraphQL `Timelog.issue` doesn't expose `project_id` directly, so
/// we match by `web_url` first (exact, robust) and fall back to `iid` (which
/// can collide across projects but is better than nothing). If neither hits,
/// `project_id` stays at `0` — the client can still display the entry.
fn enrich_timelog(
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
