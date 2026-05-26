//! GitLab API access — the only module that knows about the `gitlab` crate.
//!
//! Wraps `gitlab::AsyncGitlab` plus a handful of custom endpoints that the
//! crate doesn't ship (`add_spent_time`, board lookups, `close_issue`).
//! Returns plain [`Issue`] values so the rest of the daemon never touches
//! `serde_json::Value`.

use std::borrow::Cow;
use std::time::Duration;

use gitlab::api::{AsyncQuery, UrlBase};
use tracing::{info, instrument, warn};

use crate::error::{Error, Result};
use gitlab_trackr_api::Issue;

pub struct GitlabClient {
    inner: gitlab::AsyncGitlab,
}

/// Issue plus the raw data we still need after the GitLab fetch — labels for
/// matching against the project's board lists. Dropped after `graph_status`
/// is filled in.
pub struct IssueWithLabels {
    pub issue: Issue,
    pub labels: Vec<String>,
}

/// A timelog entry as returned by GraphQL `currentUser.timelogs`. The handler
/// fills in `project_id` from the issue cache before persisting.
pub struct FetchedTimelog {
    pub timelog_id: u64,
    pub spent_at_secs: u64,
    pub issue_iid: i64,
    pub issue_title: String,
    pub web_url: String,
    pub duration: String,
    pub summary: String,
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
    ///
    /// Retries up to three times on transient network errors with exponential backoff.
    #[instrument(skip(self))]
    pub async fn fetch_assigned_issues(
        &self,
        group: Option<String>,
    ) -> Result<Vec<IssueWithLabels>> {
        use gitlab::api::issues::{GroupIssues, IssueScope, IssueState};

        let query = if let Some(group) = group {
            GroupIssues::builder()
                .scope(IssueScope::AssignedToMe)
                .state(IssueState::Opened)
                .group(group)
                .build()
                .map_err(|e| Error::Gitlab(e.to_string()))?
        } else {
            GroupIssues::builder()
                .scope(IssueScope::AssignedToMe)
                .state(IssueState::Opened)
                .build()
                .map_err(|e| Error::Gitlab(e.to_string()))?
        };

        let mut delay = Duration::from_secs(1);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match query.query_async(&self.inner).await.map_err(classify) {
                Ok(raw) => {
                    let raw: Vec<serde_json::Value> = raw;
                    let issues: Vec<IssueWithLabels> = raw.iter().map(issue_with_labels).collect();
                    info!(count = issues.len(), "fetched issues from GitLab");
                    return Ok(issues);
                }
                Err(e @ Error::Transient(_)) if attempt < 4 => {
                    warn!(attempt, error = %e, delay_secs = delay.as_secs(), "fetch failed, retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(4));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fetch all assigned issues from the provided groups
    #[instrument(skip(self))]
    pub async fn fetch_group_issues(&self, groups: Vec<String>) -> Result<Vec<IssueWithLabels>> {
        let mut issues = Vec::new();

        for group in groups {
            issues.extend(self.fetch_assigned_issues(Some(group)).await?);
        }

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
        .map_err(classify)?;
        Ok(())
    }

    /// Record time spent on a GitLab issue via the GraphQL `timelogCreate` mutation,
    /// stamping it at `spent_at` instead of "now". Used by the retry queue so a
    /// task that was queued during an outage appears in GitLab at the time the
    /// user actually logged it, not the time we reconnected.
    #[instrument(skip(self))]
    pub async fn create_timelog(
        &self,
        issue_id: i64,
        duration: &str,
        summary: &str,
        spent_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let endpoint = TimelogCreate {
            issuable_id: format!("gid://gitlab/Issue/{issue_id}"),
            time_spent: duration,
            summary,
            spent_at: spent_at.to_rfc3339(),
        };

        let raw: serde_json::Value = endpoint.query_async(&self.inner).await.map_err(classify)?;

        if let Some(errs) = raw["data"]["timelogCreate"]["errors"].as_array()
            && !errs.is_empty()
        {
            let msg = errs
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::Gitlab(format!("timelogCreate: {msg}")));
        }

        Ok(())
    }

    /// Fetch the authenticated user's recent timelogs via GraphQL.
    ///
    /// Returns entries with `spent_at >= since`, newest first. Used by the
    /// history refresh cycle to catch time logged via the web UI or other
    /// clients. `time_spent` is converted from seconds into the same
    /// "1h 30m"-style string GitLab returns elsewhere, so stored values look
    /// like what users typed.
    #[instrument(skip(self))]
    pub async fn fetch_my_timelogs(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<FetchedTimelog>> {
        let endpoint = MyTimelogs {
            start_time: since.to_rfc3339(),
        };

        let raw: serde_json::Value = endpoint.query_async(&self.inner).await.map_err(classify)?;

        let nodes = raw["data"]["currentUser"]["timelogs"]["nodes"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut out = Vec::with_capacity(nodes.len());
        for n in &nodes {
            let Some(timelog_id) = parse_gid(n["id"].as_str().unwrap_or("")) else {
                continue;
            };
            let spent_at = n["spentAt"].as_str().unwrap_or("");
            let spent_at_secs = chrono::DateTime::parse_from_rfc3339(spent_at)
                .map(|d| d.timestamp().max(0) as u64)
                .unwrap_or(0);
            let time_spent_secs = n["timeSpent"].as_i64().unwrap_or(0).max(0) as u64;

            out.push(FetchedTimelog {
                timelog_id,
                spent_at_secs,
                issue_iid: n["issue"]["iid"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| n["issue"]["iid"].as_i64())
                    .unwrap_or(0),
                issue_title: n["issue"]["title"].as_str().unwrap_or("").to_string(),
                web_url: n["issue"]["webUrl"].as_str().unwrap_or("").to_string(),
                duration: format_duration(time_spent_secs),
                summary: n["summary"].as_str().unwrap_or("").to_string(),
            });
        }

        out.sort_by_key(|t| std::cmp::Reverse(t.spent_at_secs));
        info!(count = out.len(), "fetched timelogs from GitLab");
        Ok(out)
    }

    /// Close a GitLab issue (`PUT /projects/:id/issues/:iid` with `state_event=close`).
    #[instrument(skip(self))]
    pub async fn close_issue(&self, project_id: i64, issue_iid: i64) -> Result<()> {
        use gitlab::api::ignore;

        ignore(CloseIssueEndpoint {
            project_id,
            issue_iid,
        })
        .query_async(&self.inner)
        .await
        .map_err(classify)?;
        Ok(())
    }

    /// Collect the label names of every list across every board in `project_id`.
    ///
    /// Used to drive `Issue::graph_status` — an issue's `graph_status` is set
    /// to the first of its labels that appears in this list. Lists without a
    /// label (e.g. backlog/closed) are skipped.
    #[instrument(skip(self))]
    pub async fn fetch_board_list_labels(&self, project_id: i64) -> Result<Vec<String>> {
        let boards: Vec<serde_json::Value> = ListProjectBoards { project_id }
            .query_async(&self.inner)
            .await
            .map_err(classify)?;

        let mut labels = Vec::new();
        for board in &boards {
            let Some(board_id) = board["id"].as_i64() else {
                continue;
            };
            let lists: Vec<serde_json::Value> = ListBoardLists {
                project_id,
                board_id,
            }
            .query_async(&self.inner)
            .await
            .map_err(classify)?;
            for list in &lists {
                if let Some(name) = list["label"]["name"].as_str() {
                    labels.push(name.to_string());
                }
            }
        }
        Ok(labels)
    }
}

/// Map a GitLab API error to [`Error::Transient`] for network failures and
/// [`Error::Gitlab`] for permanent rejections (auth, 4xx, bad JSON, …).
fn classify<E>(e: gitlab::api::ApiError<E>) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    let is_network = matches!(e, gitlab::api::ApiError::Client { .. });
    let msg = e.to_string();
    if is_network {
        Error::Transient(msg)
    } else {
        Error::Gitlab(msg)
    }
}

/// Convert a raw JSON value from the GitLab issues API into an [`Issue`] plus
/// the labels needed to compute `graph_status` later. Missing or malformed
/// fields fall back to zero / empty-string defaults so a single bad response
/// does not crash the whole list. `graph_status` is left empty here — the
/// handler fills it after consulting the board cache.
fn issue_with_labels(v: &serde_json::Value) -> IssueWithLabels {
    let labels = v["labels"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|l| l.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    IssueWithLabels {
        issue: Issue {
            id: v["id"].as_i64().unwrap_or(0),
            iid: v["iid"].as_i64().unwrap_or(0),
            project_id: v["project_id"].as_i64().unwrap_or(0),
            title: v["title"].as_str().unwrap_or("").to_string(),
            web_url: v["web_url"].as_str().unwrap_or("").to_string(),
            state: v["state"].as_str().unwrap_or("").to_string(),
            parent: v["epic"]["url"].as_str().unwrap_or("").to_string(),
            total_time: v["time_stats"]["human_total_time_spent"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            graph_status: String::new(),
        },
        labels,
    }
}

/// `POST /projects/:project_id/issues/:issue_iid/add_spent_time`
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

/// `POST /api/graphql` for `Mutation.timelogCreate`.
///
/// Hits the GraphQL endpoint instead of the REST `add_spent_time` because the
/// latter has no `spent_at` parameter — GitLab stamps it as "now" on receipt,
/// which is wrong for tasks the retry queue has been sitting on.
struct TimelogCreate<'a> {
    issuable_id: String,
    time_spent: &'a str,
    summary: &'a str,
    spent_at: String,
}

impl gitlab::api::Endpoint for TimelogCreate<'_> {
    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn endpoint(&self) -> Cow<'static, str> {
        "api/graphql".into()
    }

    fn url_base(&self) -> UrlBase {
        UrlBase::Instance
    }

    fn body(&self) -> std::result::Result<Option<(&'static str, Vec<u8>)>, gitlab::api::BodyError> {
        let body = serde_json::json!({
            "query": "mutation($id: IssuableID!, $time: String!, $summary: String!, $spent: Time) {\
                timelogCreate(input: { issuableId: $id, timeSpent: $time, summary: $summary, spentAt: $spent }) {\
                    errors\
                }\
            }",
            "variables": {
                "id": self.issuable_id,
                "time": self.time_spent,
                "summary": self.summary,
                "spent": self.spent_at,
            },
        });
        Ok(Some(("application/json", serde_json::to_vec(&body)?)))
    }
}

/// `PUT /projects/:project_id/issues/:issue_iid` with `state_event=close`.
struct CloseIssueEndpoint {
    project_id: i64,
    issue_iid: i64,
}

impl gitlab::api::Endpoint for CloseIssueEndpoint {
    fn method(&self) -> http::Method {
        http::Method::PUT
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!("projects/{}/issues/{}", self.project_id, self.issue_iid).into()
    }

    fn body(&self) -> std::result::Result<Option<(&'static str, Vec<u8>)>, gitlab::api::BodyError> {
        let body = serde_json::json!({"state_event": "close"});
        Ok(Some(("application/json", serde_json::to_vec(&body)?)))
    }
}

/// `GET /projects/:project_id/boards`
struct ListProjectBoards {
    project_id: i64,
}

impl gitlab::api::Endpoint for ListProjectBoards {
    fn method(&self) -> http::Method {
        http::Method::GET
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!("projects/{}/boards", self.project_id).into()
    }
}

/// `POST /api/graphql` for `currentUser.timelogs`.
///
/// Pulls the authenticated user's timelogs since `start_time`. Used by the
/// history refresh cycle so entries logged outside the daemon (web UI, other
/// clients) still show up.
struct MyTimelogs {
    start_time: String,
}

impl gitlab::api::Endpoint for MyTimelogs {
    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn endpoint(&self) -> Cow<'static, str> {
        "api/graphql".into()
    }

    fn url_base(&self) -> UrlBase {
        UrlBase::Instance
    }

    fn body(&self) -> std::result::Result<Option<(&'static str, Vec<u8>)>, gitlab::api::BodyError> {
        let body = serde_json::json!({
            "query": "query($start: Time!) {\
                currentUser {\
                    timelogs(startTime: $start) {\
                        nodes {\
                            id\
                            timeSpent\
                            spentAt\
                            summary\
                            issue { iid title webUrl }\
                        }\
                    }\
                }\
            }",
            "variables": { "start": self.start_time },
        });
        Ok(Some(("application/json", serde_json::to_vec(&body)?)))
    }
}

/// Pull the trailing integer out of a `gid://gitlab/Timelog/<id>` global ID.
fn parse_gid(gid: &str) -> Option<u64> {
    gid.rsplit('/').next().and_then(|s| s.parse().ok())
}

/// Format a duration in seconds as `"1h 30m"` (or `"45s"` when sub-minute).
/// Matches the style GitLab itself uses for `human_total_time_spent`.
fn format_duration(secs: u64) -> String {
    if secs == 0 {
        return "0m".to_string();
    }
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let rem = secs % 60;

    let mut parts = Vec::new();
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if mins > 0 {
        parts.push(format!("{mins}m"));
    }
    if hours == 0 && mins == 0 && rem > 0 {
        parts.push(format!("{rem}s"));
    }
    parts.join(" ")
}

/// `GET /projects/:project_id/boards/:board_id/lists`
struct ListBoardLists {
    project_id: i64,
    board_id: i64,
}

impl gitlab::api::Endpoint for ListBoardLists {
    fn method(&self) -> http::Method {
        http::Method::GET
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/boards/{}/lists",
            self.project_id, self.board_id
        )
        .into()
    }
}
