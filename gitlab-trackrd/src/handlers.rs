//! Varlink method implementations — orchestration only.
//!
//! Each method is a short cascade: consult the cache, fall back to GitLab,
//! reply. GitLab errors become `GitlabError` varlink replies; cache failures
//! are logged and treated as a miss so the daemon stays available.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_ClearCache, Call_CloseIssue, Call_GetAssignedIssues, Call_PostTime, Issue,
    VarlinkInterface,
};

use crate::boards::BoardCache;
use crate::cache::IssueCache;
use crate::error::Error;
use crate::gitlab::{GitlabClient, IssueWithLabels};
use crate::queue::RetryQueue;

pub struct Handlers {
    pub gitlab: Arc<GitlabClient>,
    pub cache: Arc<IssueCache>,
    pub boards: Arc<BoardCache>,
    pub queue: RetryQueue,
}

impl Handlers {
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
