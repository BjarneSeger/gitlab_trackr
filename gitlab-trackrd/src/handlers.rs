//! Varlink method implementations — orchestration only.
//!
//! Each method is a short cascade: consult the cache, fall back to GitLab,
//! reply. GitLab errors become `GitlabError` varlink replies; cache failures
//! are logged and treated as a miss so the daemon stays available.

use std::sync::Arc;

use tracing::{debug, info, instrument, warn};

use gitlab_trackr_api::{
    Call_ClearCache, Call_GetAssignedIssues, Call_PostTime, VarlinkInterface,
};

use crate::cache::IssueCache;
use crate::error::Error;
use crate::gitlab::GitlabClient;
use crate::queue::RetryQueue;

pub struct Handlers {
    pub gitlab: Arc<GitlabClient>,
    pub cache: Arc<IssueCache>,
    pub queue: RetryQueue,
}

#[async_trait::async_trait]
impl VarlinkInterface for Handlers {
    #[instrument(skip(self, call))]
    async fn get_assigned_issues(
        &self,
        call: &mut dyn Call_GetAssignedIssues,
    ) -> varlink::Result<()> {
        match self.cache.get() {
            Ok(Some(issues)) => {
                debug!(count = issues.len(), "cache hit");
                return call.reply(issues);
            }
            Ok(None) => debug!("cache miss, fetching from GitLab"),
            Err(e) => warn!("cache read failed, treating as miss: {e}"),
        }

        match self.gitlab.fetch_assigned_issues().await {
            Ok(issues) => {
                if let Err(e) = self.cache.put(&issues) {
                    warn!("cache write failed: {e}");
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
            warn!("cache clear failed: {e}");
        } else {
            info!("cache cleared");
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
                self.queue.post_time(project_id, issue_iid, duration, summary).await;
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "PostTime rejected by GitLab");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }
}
