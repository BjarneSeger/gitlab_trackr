//! Runtime state and varlink method implementations for the daemon.

use gitlab::api::AsyncQuery;
use redb::{Database, ReadableDatabase};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument, warn};

use super::iface::{Call_ClearCache, Call_GetAssignedIssues, Call_PostTime, Issue, VarlinkInterface};
use super::{Error, ISSUES_TABLE, Result};
use crate::gl;
use crate::utils::{issue_from_value, now_secs};

/// Shared state injected into every varlink method call.
pub struct Daemon {
    /// Async GitLab API client, authenticated with the user's token.
    pub client: gitlab::AsyncGitlab,
    /// Handle to the redb cache database, shared across concurrent calls.
    pub db: Arc<Mutex<Database>>,
    /// Maximum age (in seconds) of a cached issue list before it is refetched.
    pub cache_ttl: u64,
}

/// On-disk representation of a cached issue list, stored as JSON in redb.
#[derive(Serialize, Deserialize)]
pub(crate) struct CachedData {
    /// Unix timestamp (seconds) when this entry was written.
    timestamp: u64,
    issues: Vec<Issue>,
}

impl Daemon {
    /// Fetch all open issues assigned to the authenticated user from GitLab and
    /// refresh the on-disk cache.  Called when the cache is absent or stale.
    #[instrument(skip(self))]
    pub async fn fetch_issues(&self) -> Result<Vec<Issue>> {
        use gitlab::api::issues::{IssueScope, IssueState, Issues};

        let raw: Vec<serde_json::Value> = Issues::builder()
            .scope(IssueScope::AssignedToMe)
            .state(IssueState::Opened)
            .build()
            .map_err(|e| Error::Gitlab(e.to_string()))?
            .query_async(&self.client)
            .await
            .map_err(|e| Error::Gitlab(e.to_string()))?;

        let res: Vec<Issue> = raw.iter().map(issue_from_value).collect();
        info!(count = res.len(), "fetched issues from GitLab");
        self.write_cache(&res);
        Ok(res)
    }

    /// POST `/projects/:project_id/issues/:issue_iid/add_spent_time` with `duration`
    /// (e.g. `"1h30m"`).  Uses the custom [`gl::AddSpentTime`] endpoint because
    /// the `gitlab` crate does not provide this API natively.
    #[instrument(skip(self))]
    async fn post_time(
        &self,
        project_id: i64,
        issue_iid: i64,
        duration: &str,
        summary: Option<&str>,
    ) -> Result<()> {
        use gitlab::api::ignore;

        ignore(gl::AddSpentTime {
            project_id,
            issue_iid,
            duration,
            summary,
        })
        .query_async(&self.client)
        .await
        .map_err(|e| Error::Gitlab(e.to_string()))?;
        Ok(())
    }

    /// Return a fresh-enough cached issue list, or `None` if the cache is
    /// absent or expired.  Cache errors are logged and treated as a miss so
    /// the daemon stays available.
    fn read_cache(&self) -> Option<Vec<Issue>> {
        self.try_read_cache()
            .map_err(|e| warn!("cache read failed: {e}"))
            .ok()
            .flatten()
    }

    /// Inner fallible implementation of [`Self::read_cache`].
    fn try_read_cache(&self) -> Result<Option<Vec<Issue>>> {
        let now = now_secs();
        let db = self.db.lock().map_err(|e| Error::Cache(e.to_string()))?;
        let txn = db.begin_read().map_err(|e| Error::Cache(e.to_string()))?;
        let table = txn
            .open_table(ISSUES_TABLE)
            .map_err(|e| Error::Cache(e.to_string()))?;
        let Some(guard) = table
            .get("assigned")
            .map_err(|e| Error::Cache(e.to_string()))?
        else {
            return Ok(None);
        };
        let data: CachedData = serde_json::from_slice(guard.value())?;
        Ok((now.saturating_sub(data.timestamp) < self.cache_ttl).then_some(data.issues))
    }

    /// Persist `issues` to the redb cache.  Write errors are logged but never
    /// propagated so a cache failure never breaks a successful API response.
    fn write_cache(&self, issues: &[Issue]) {
        if let Err(e) = self.try_write_cache(issues) {
            warn!("cache write failed: {e}");
        }
    }

    fn invalidate_cache(&self) {
        if let Err(e) = self.try_invalidate_cache() {
            warn!("cache clear failed: {e}");
        }
    }

    fn try_invalidate_cache(&self) -> Result<()> {
        let db = self.db.lock().map_err(|e| Error::Cache(e.to_string()))?;
        let txn = db.begin_write().map_err(|e| Error::Cache(e.to_string()))?;
        {
            let mut table = txn
                .open_table(ISSUES_TABLE)
                .map_err(|e| Error::Cache(e.to_string()))?;
            table
                .remove("assigned")
                .map_err(|e| Error::Cache(e.to_string()))?;
        }
        txn.commit().map_err(|e| Error::Cache(e.to_string()))?;
        Ok(())
    }

    /// Inner fallible implementation of [`Self::write_cache`].
    fn try_write_cache(&self, issues: &[Issue]) -> Result<()> {
        let bytes = serde_json::to_vec(&CachedData {
            timestamp: now_secs(),
            issues: issues.to_vec(),
        })?;
        let db = self.db.lock().map_err(|e| Error::Cache(e.to_string()))?;
        let txn = db.begin_write().map_err(|e| Error::Cache(e.to_string()))?;
        {
            let mut table = txn
                .open_table(ISSUES_TABLE)
                .map_err(|e| Error::Cache(e.to_string()))?;
            table
                .insert("assigned", bytes.as_slice())
                .map_err(|e| Error::Cache(e.to_string()))?;
        }
        txn.commit().map_err(|e| Error::Cache(e.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl VarlinkInterface for Daemon {
    /// Varlink handler for `GetAssignedIssues()`.
    ///
    /// Returns the cached issue list if it is still within [`Daemon::cache_ttl`];
    /// otherwise fetches fresh data from GitLab, updates the cache, and replies.
    /// On error, replies with a `GitlabError` varlink error instead of panicking.
    #[instrument(skip(self, call))]
    async fn get_assigned_issues(
        &self,
        call: &mut dyn Call_GetAssignedIssues,
    ) -> varlink::Result<()> {
        if let Some(issues) = self.read_cache() {
            debug!(count = issues.len(), "cache hit");
            return call.reply(issues);
        }

        debug!("cache miss, fetching from GitLab");
        match self.fetch_issues().await {
            Ok(res) => call.reply(res),
            Err(e) => {
                warn!(error = %e, "GitLab fetch failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }

    /// Varlink handler for `ClearCache()`.
    #[instrument(skip(self, call))]
    async fn clear_cache(&self, call: &mut dyn Call_ClearCache) -> varlink::Result<()> {
        self.invalidate_cache();
        info!("cache cleared");
        call.reply()
    }

    /// Varlink handler for `PostTime(project_id, issue_iid, duration)`.
    ///
    /// `duration` must be a GitLab time-tracking string such as `"1h30m"` or `"45m"`.
    /// On error, replies with a `GitlabError` varlink error.
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
            .post_time(project_id, issue_iid, &duration, summary.as_deref())
            .await
        {
            Ok(()) => {
                info!(project_id, issue_iid, duration, "posted time");
                call.reply()
            }
            Err(e) => {
                warn!(error = %e, "PostTime failed");
                call.reply_gitlab_error(e.to_string())
            }
        }
    }
}
