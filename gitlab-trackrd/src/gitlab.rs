//! GitLab API access — the only module that knows about the `gitlab` crate.
//!
//! Wraps `gitlab::AsyncGitlab` plus one custom endpoint that the crate doesn't
//! ship (`add_spent_time`). Returns plain [`Issue`] values so the rest of the
//! daemon never touches `serde_json::Value`.

use std::borrow::Cow;

use gitlab::api::AsyncQuery;
use tracing::{info, instrument};

use crate::error::{Error, Result};
use gitlab_trackr_api::Issue;

pub struct GitlabClient {
    inner: gitlab::AsyncGitlab,
}

impl GitlabClient {
    pub async fn connect(host: &str, token: &str) -> Result<Self> {
        let inner = gitlab::GitlabBuilder::new(host.to_string(), token.to_string())
            .build_async()
            .await
            .map_err(|e| Error::Gitlab(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Fetch all open issues assigned to the authenticated user.
    #[instrument(skip(self))]
    pub async fn fetch_assigned_issues(&self) -> Result<Vec<Issue>> {
        use gitlab::api::issues::{IssueScope, IssueState, Issues};

        let raw: Vec<serde_json::Value> = Issues::builder()
            .scope(IssueScope::AssignedToMe)
            .state(IssueState::Opened)
            .build()
            .map_err(|e| Error::Gitlab(e.to_string()))?
            .query_async(&self.inner)
            .await
            .map_err(|e| Error::Gitlab(e.to_string()))?;

        let issues: Vec<Issue> = raw.iter().map(issue_from_value).collect();
        info!(count = issues.len(), "fetched issues from GitLab");
        Ok(issues)
    }

    /// Record time spent on a GitLab issue.
    #[instrument(skip(self))]
    pub async fn add_spent_time(
        &self,
        project_id: i64,
        issue_iid: i64,
        duration: &str,
        summary: Option<&str>,
    ) -> Result<()> {
        use gitlab::api::ignore;

        ignore(AddSpentTime {
            project_id,
            issue_iid,
            duration,
            summary,
        })
        .query_async(&self.inner)
        .await
        .map_err(|e| Error::Gitlab(e.to_string()))?;
        Ok(())
    }
}

/// Convert a raw JSON value from the GitLab issues API into [`Issue`].
/// Missing or malformed fields fall back to zero / empty-string defaults so
/// a single bad response does not crash the whole list.
fn issue_from_value(v: &serde_json::Value) -> Issue {
    Issue {
        id: v["id"].as_i64().unwrap_or(0),
        iid: v["iid"].as_i64().unwrap_or(0),
        project_id: v["project_id"].as_i64().unwrap_or(0),
        title: v["title"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
        state: v["state"].as_str().unwrap_or("").to_string(),
    }
}

/// `POST /projects/:project_id/issues/:issue_iid/add_spent_time`
///
/// The `gitlab` crate (v0.18) does not include this endpoint, so it's
/// implemented manually via [`gitlab::api::Endpoint`].
struct AddSpentTime<'a> {
    project_id: i64,
    issue_iid: i64,
    duration: &'a str,
    summary: Option<&'a str>,
}

impl gitlab::api::Endpoint for AddSpentTime<'_> {
    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/issues/{}/add_spent_time",
            self.project_id, self.issue_iid
        )
        .into()
    }

    fn body(&self) -> std::result::Result<Option<(&'static str, Vec<u8>)>, gitlab::api::BodyError> {
        let mut body = serde_json::json!({"duration": self.duration});
        if let Some(summary) = self.summary {
            body["summary"] = serde_json::Value::String(summary.to_owned());
        }
        Ok(Some(("application/json", serde_json::to_vec(&body)?)))
    }
}
