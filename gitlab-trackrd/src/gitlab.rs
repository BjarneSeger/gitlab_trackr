//! GitLab API access — the only module that knows about the `gitlab` crate.
//!
//! Wraps `gitlab::AsyncGitlab` plus a handful of custom endpoints that the
//! crate doesn't ship (`add_spent_time`, board lookups, `close`).
//! Returns plain [`Issue`] values so the rest of the daemon never touches
//! `serde_json::Value`.

use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;

use gitlab::api::{AsyncQuery, UrlBase};
use tracing::{info, instrument, warn};

use crate::error::{Error, Result};
use crate::search::{MrAssignee, SearchGroup, SearchIssue, SearchMr, SearchProject};
use gitlab_trackr_api::Issue;

/// Which GitLab issuable an operation targets. Internal counterpart of the
/// wire `IssuableKind`, kept separate so persisted queue/history records
/// don't couple the on-disk format to the api crate; `Default = Issue`
/// because every record written before MR support was an issue, which lets
/// them deserialize via `#[serde(default)]`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Issuable {
    #[default]
    Issue,
    MergeRequest,
}

impl Issuable {
    /// REST URL path segment: `projects/{id}/<segment>/{iid}/…`.
    pub fn path_segment(self) -> &'static str {
        match self {
            Issuable::Issue => "issues",
            Issuable::MergeRequest => "merge_requests",
        }
    }

    /// GraphQL global-ID type: `gid://gitlab/<type>/{id}`.
    pub fn gid_type(self) -> &'static str {
        match self {
            Issuable::Issue => "Issue",
            Issuable::MergeRequest => "MergeRequest",
        }
    }
}

pub struct GitlabClient {
    inner: gitlab::AsyncGitlab,
    /// GitLab host (e.g. `"gitlab.com"`) used to build `inner`. Exposed so
    /// `WhoAmI` can return it.
    host: String,
    /// Numeric ID of the authenticated user, fetched once at `connect()`.
    /// Required so `assign_self`/`unassign_self` can mutate the issuable's
    /// `assignee_ids` list without an extra round-trip per call.
    current_user_id: i64,
}

impl GitlabClient {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn current_user_id(&self) -> i64 {
        self.current_user_id
    }
}

/// Issue plus the raw data we still need after the GitLab fetch — labels for
/// matching against the project's board lists. Dropped after `graph_status`
/// is filled in.
#[derive(Clone)]
pub struct IssueWithLabels {
    pub issue: Issue,
    pub labels: Vec<String>,
}

/// A timelog entry as returned by GraphQL `currentUser.timelogs`. The handler
/// fills in `project_id` from the issue cache before persisting.
#[derive(Clone)]
pub struct FetchedTimelog {
    pub timelog_id: u64,
    pub spent_at_secs: u64,
    pub issue_iid: i64,
    pub issue_title: String,
    pub web_url: String,
    pub duration: String,
    pub summary: String,
}

/// Daemon-facing GitLab surface. Lets tests substitute a fake without touching
/// the real `gitlab` crate. Production code path goes through the impl on
/// [`GitlabClient`].
#[async_trait::async_trait]
pub trait GitlabApi: Send + Sync {
    async fn fetch_assigned_issues(&self, group: Option<String>) -> Result<Vec<IssueWithLabels>>;

    async fn add_spent_time(
        &self,
        kind: Issuable,
        project_id: i64,
        iid: i64,
        duration: &str,
        summary: Option<&str>,
    ) -> Result<()>;

    async fn create_timelog(
        &self,
        kind: Issuable,
        issuable_id: i64,
        duration: &str,
        summary: &str,
        spent_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()>;

    async fn fetch_my_timelogs(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<FetchedTimelog>>;

    async fn close(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()>;

    async fn assign_self(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()>;

    async fn unassign_self(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()>;

    async fn fetch_board_list_labels(&self, project_id: i64) -> Result<Vec<String>>;

    /// Issues for the search cache. `project = None` hits the global endpoint
    /// with `scope=all`; `Some(id)` hits `/projects/:id/issues` (member
    /// population). No state filter — closed issues stay searchable, and the
    /// incremental sync sees close transitions as ordinary updates.
    async fn fetch_issues_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<SearchIssue>>;

    /// Merge requests for the search cache; same shape as
    /// [`GitlabApi::fetch_issues_for_search`].
    async fn fetch_merge_requests_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<SearchMr>>;

    /// All projects the user is a member of (`membership=true`), always the
    /// full list — the membership set is small and has no reliable delta
    /// filter (renames don't bump `last_activity_at`).
    async fn fetch_member_projects(&self) -> Result<Vec<SearchProject>>;

    /// All groups the user is a member of (`min_access_level=guest`; a bare
    /// `GET /groups` would also include public non-member groups).
    async fn fetch_member_groups(&self) -> Result<Vec<SearchGroup>>;
}

impl GitlabClient {
    /// Read the issuable's current `assignee_ids`, apply the add/remove, and
    /// PUT the new list back. Skips the PUT when the issuable is already in
    /// the target state (`add && already assigned` or `!add && not assigned`).
    async fn mutate_self_assignment(
        &self,
        kind: Issuable,
        project_id: i64,
        iid: i64,
        add: bool,
    ) -> Result<()> {
        let raw: serde_json::Value = GetIssuableEndpoint {
            kind,
            project_id,
            iid,
        }
        .query_async(&self.inner)
        .await
        .map_err(classify)?;

        let current: Vec<i64> = raw["assignees"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|a| a["id"].as_i64()).collect())
            .unwrap_or_default();

        let Some(new_ids) = compute_new_assignees(&current, self.current_user_id, add) else {
            return Ok(());
        };

        use gitlab::api::ignore;
        ignore(UpdateAssigneesEndpoint {
            kind,
            project_id,
            iid,
            assignee_ids: new_ids,
        })
        .query_async(&self.inner)
        .await
        .map_err(classify)?;
        Ok(())
    }

    pub async fn connect(host: &str, token: &str) -> Result<Self> {
        let inner = gitlab::GitlabBuilder::new(host.to_string(), token.to_string())
            .build_async()
            .await
            .map_err(classify_build)?;

        let user: serde_json::Value = CurrentUserEndpoint
            .query_async(&inner)
            .await
            .map_err(classify)?;
        let current_user_id = user["id"].as_i64().ok_or_else(|| {
            Error::Gitlab(format!("GET /user response missing numeric id: {user}"))
        })?;
        info!(current_user_id, "resolved authenticated GitLab user");

        Ok(Self {
            inner,
            host: host.to_string(),
            current_user_id,
        })
    }

    /// Like [`GitlabClient::connect`], but retries transient (network) failures
    /// with bounded exponential back-off (see [`retry_transient`]). A permanent
    /// rejection (a bad token) fails immediately without retrying. Used by the
    /// interactive `Login` handler so a momentary blip doesn't fail the command.
    pub async fn connect_with_retry(host: &str, token: &str) -> Result<Self> {
        retry_transient("login connect", || Self::connect(host, token)).await
    }
}

/// Run `op` with bounded exponential back-off on transient (network) failures:
/// up to four attempts, sleeping 1 s → 2 s → 4 s between them. A permanent error
/// returns immediately. Shared by `connect_with_retry` and `run_issues_query`.
///
/// Takes a future *factory* (`FnMut() -> Future`) rather than an async closure so
/// the produced future has a single concrete type — an `impl AsyncFnMut` here
/// yields a per-call future whose `Send`-ness isn't general enough for the
/// `#[instrument]` callers.
pub(crate) async fn retry_transient<T, Fut>(op: &str, mut f: impl FnMut() -> Fut) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let mut delay = Duration::from_secs(1);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match f().await {
            Ok(v) => return Ok(v),
            Err(e @ Error::Transient(_)) if attempt < 4 => {
                warn!(attempt, error = %e, delay_secs = delay.as_secs(), op, "transient failure, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(4));
            }
            Err(e) => return Err(e),
        }
    }
}

#[async_trait::async_trait]
impl GitlabApi for GitlabClient {
    /// Fetch all open issues assigned to the authenticated user.
    ///
    /// Retries up to three times on transient network errors with exponential backoff.
    #[instrument(skip(self))]
    async fn fetch_assigned_issues(&self, group: Option<String>) -> Result<Vec<IssueWithLabels>> {
        use gitlab::api::issues::{GroupIssues, IssueScope, IssueState, Issues};
        use gitlab::api::{Pagination, paged};

        if let Some(group) = group {
            let query = GroupIssues::builder()
                .scope(IssueScope::AssignedToMe)
                .state(IssueState::Opened)
                .group(group)
                .build()
                .map_err(|e| Error::Gitlab(e.to_string()))?;
            run_issues_query(&self.inner, paged(query, Pagination::All)).await
        } else {
            let query = Issues::builder()
                .scope(IssueScope::AssignedToMe)
                .state(IssueState::Opened)
                .build()
                .map_err(|e| Error::Gitlab(e.to_string()))?;
            run_issues_query(&self.inner, paged(query, Pagination::All)).await
        }
    }

    /// Record time spent on a GitLab issue or merge request.
    #[instrument(skip(self))]
    async fn add_spent_time(
        &self,
        kind: Issuable,
        project_id: i64,
        iid: i64,
        duration: &str,
        summary: Option<&str>,
    ) -> Result<()> {
        use gitlab::api::ignore;

        ignore(AddSpentTime {
            kind,
            project_id,
            iid,
            duration,
            summary,
        })
        .query_async(&self.inner)
        .await
        .map_err(classify)?;
        Ok(())
    }

    /// Record time spent on an issuable via the GraphQL `timelogCreate` mutation,
    /// stamping it at `spent_at` instead of "now". Used by the retry queue so a
    /// task that was queued during an outage appears in GitLab at the time the
    /// user actually logged it, not the time we reconnected.
    #[instrument(skip(self))]
    async fn create_timelog(
        &self,
        kind: Issuable,
        issuable_id: i64,
        duration: &str,
        summary: &str,
        spent_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let endpoint = TimelogCreate {
            issuable_id: format!("gid://gitlab/{}/{issuable_id}", kind.gid_type()),
            time_spent: duration,
            summary,
            spent_at: spent_at.to_rfc3339(),
        };

        let raw: serde_json::Value = endpoint.query_async(&self.inner).await.map_err(classify)?;

        if let Some(errs) = raw["errors"].as_array()
            && !errs.is_empty()
        {
            let msg = errs
                .iter()
                .filter_map(|e| e["message"].as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::Gitlab(format!("timelogCreate: {msg}")));
        }

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
    async fn fetch_my_timelogs(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<FetchedTimelog>> {
        let endpoint = MyTimelogs {
            start_time: since.to_rfc3339(),
        };

        // Retry transient (network) failures like the issues fetch does: a
        // read is idempotent, so a momentary blip is absorbed here instead of
        // demoting the whole session (see `handlers::note_gitlab_error`).
        let raw: serde_json::Value = retry_transient("fetch timelogs", || async {
            endpoint.query_async(&self.inner).await.map_err(classify)
        })
        .await?;

        if let Some(errs) = raw["errors"].as_array()
            && !errs.is_empty()
        {
            let msg = errs
                .iter()
                .filter_map(|e| e["message"].as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::Gitlab(format!("currentUser.timelogs: {msg}")));
        }

        let nodes = raw["data"]["currentUser"]["timelogs"]["nodes"]
            .as_array()
            .cloned()
            .ok_or_else(|| {
                Error::Gitlab(format!(
                    "currentUser.timelogs returned unexpected shape: {raw}"
                ))
            })?;

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

    /// Close a GitLab issuable (`PUT /projects/:id/<kind>/:iid` with
    /// `state_event=close`).
    #[instrument(skip(self))]
    async fn close(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()> {
        use gitlab::api::ignore;

        ignore(CloseEndpoint {
            kind,
            project_id,
            iid,
        })
        .query_async(&self.inner)
        .await
        .map_err(classify)?;
        Ok(())
    }

    /// Add the authenticated user to the issuable's `assignee_ids` list.
    #[instrument(skip(self))]
    async fn assign_self(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()> {
        self.mutate_self_assignment(kind, project_id, iid, true)
            .await
    }

    /// Remove the authenticated user from the issuable's `assignee_ids` list.
    #[instrument(skip(self))]
    async fn unassign_self(&self, kind: Issuable, project_id: i64, iid: i64) -> Result<()> {
        self.mutate_self_assignment(kind, project_id, iid, false)
            .await
    }

    /// Collect the label names of every list across every board in `project_id`.
    ///
    /// Used to drive `Issue::graph_status` — an issue's `graph_status` is set
    /// to the first of its labels that appears in this list. Lists without a
    /// label (e.g. backlog/closed) are skipped.
    #[instrument(skip(self))]
    async fn fetch_board_list_labels(&self, project_id: i64) -> Result<Vec<String>> {
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

    #[instrument(skip(self))]
    async fn fetch_issues_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<SearchIssue>> {
        use gitlab::api::issues::{IssueScope, Issues, ProjectIssues};
        use gitlab::api::{Pagination, paged};

        let raw = if let Some(project_id) = project {
            let mut builder = ProjectIssues::builder();
            builder.project(project_id as u64);
            if let Some(after) = updated_after {
                builder.updated_after(after);
            }
            let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
            run_paged_query(
                &self.inner,
                "fetch search issues",
                paged(query, Pagination::All),
            )
            .await?
        } else {
            let mut builder = Issues::builder();
            builder.scope(IssueScope::All);
            if let Some(after) = updated_after {
                builder.updated_after(after);
            }
            let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
            run_paged_query(
                &self.inner,
                "fetch search issues",
                paged(query, Pagination::All),
            )
            .await?
        };

        let issues: Vec<SearchIssue> = raw
            .iter()
            .map(search_issue_from_json)
            .filter(|i| i.id > 0)
            .collect();
        info!(count = issues.len(), "fetched search issues from GitLab");
        Ok(issues)
    }

    #[instrument(skip(self))]
    async fn fetch_merge_requests_for_search(
        &self,
        project: Option<i64>,
        updated_after: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<SearchMr>> {
        use gitlab::api::merge_requests::{MergeRequestScope, MergeRequests};
        use gitlab::api::projects::merge_requests::MergeRequests as ProjectMergeRequests;
        use gitlab::api::{Pagination, paged};

        let raw = if let Some(project_id) = project {
            let mut builder = ProjectMergeRequests::builder();
            builder.project(project_id as u64);
            if let Some(after) = updated_after {
                builder.updated_after(after);
            }
            let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
            run_paged_query(
                &self.inner,
                "fetch search MRs",
                paged(query, Pagination::All),
            )
            .await?
        } else {
            let mut builder = MergeRequests::builder();
            builder.scope(MergeRequestScope::All);
            if let Some(after) = updated_after {
                builder.updated_after(after);
            }
            let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
            run_paged_query(
                &self.inner,
                "fetch search MRs",
                paged(query, Pagination::All),
            )
            .await?
        };

        let mrs: Vec<SearchMr> = raw
            .iter()
            .map(search_mr_from_json)
            .filter(|m| m.id > 0)
            .collect();
        info!(
            count = mrs.len(),
            "fetched search merge requests from GitLab"
        );
        Ok(mrs)
    }

    #[instrument(skip(self))]
    async fn fetch_member_projects(&self) -> Result<Vec<SearchProject>> {
        use gitlab::api::projects::Projects;
        use gitlab::api::{Pagination, paged};

        let mut builder = Projects::builder();
        builder.membership(true).simple(true);
        let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
        let raw = run_paged_query(
            &self.inner,
            "fetch member projects",
            paged(query, Pagination::All),
        )
        .await?;

        let projects: Vec<SearchProject> = raw
            .iter()
            .map(search_project_from_json)
            .filter(|p| p.id > 0)
            .collect();
        info!(
            count = projects.len(),
            "fetched member projects from GitLab"
        );
        Ok(projects)
    }

    #[instrument(skip(self))]
    async fn fetch_member_groups(&self) -> Result<Vec<SearchGroup>> {
        use gitlab::api::common::AccessLevel;
        use gitlab::api::groups::Groups;
        use gitlab::api::{Pagination, paged};

        let mut builder = Groups::builder();
        builder.min_access_level(AccessLevel::Guest);
        let query = builder.build().map_err(|e| Error::Gitlab(e.to_string()))?;
        let raw = run_paged_query(
            &self.inner,
            "fetch member groups",
            paged(query, Pagination::All),
        )
        .await?;

        let groups: Vec<SearchGroup> = raw
            .iter()
            .map(search_group_from_json)
            .filter(|g| g.id > 0)
            .collect();
        info!(count = groups.len(), "fetched member groups from GitLab");
        Ok(groups)
    }
}

/// Run `query` against `client`, retrying transient errors with exponential
/// back-off (see [`retry_transient`]).
async fn run_issues_query<Q>(client: &gitlab::AsyncGitlab, query: Q) -> Result<Vec<IssueWithLabels>>
where
    Q: gitlab::api::AsyncQuery<Vec<serde_json::Value>, gitlab::AsyncGitlab> + Sync,
{
    let raw = run_paged_query(client, "fetch issues", query).await?;
    let issues: Vec<IssueWithLabels> = raw.iter().map(issue_with_labels).collect();
    info!(count = issues.len(), "fetched issues from GitLab");
    Ok(issues)
}

/// Run a paged list `query` against `client` into raw JSON, retrying transient
/// errors with exponential back-off (see [`retry_transient`]).
async fn run_paged_query<Q>(
    client: &gitlab::AsyncGitlab,
    op: &str,
    query: Q,
) -> Result<Vec<serde_json::Value>>
where
    Q: gitlab::api::AsyncQuery<Vec<serde_json::Value>, gitlab::AsyncGitlab> + Sync,
{
    retry_transient(op, || async {
        query.query_async(client).await.map_err(classify)
    })
    .await
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

/// Same split as [`classify`], but for the [`gitlab::GitlabError`] returned by
/// `GitlabBuilder::build_async`. The builder runs an initial connection check,
/// so an unreachable host must surface as [`Error::Transient`] (retryable) and
/// not a permanent [`Error::Gitlab`] — otherwise `connect` reports a network
/// outage as a rejected token.
fn classify_build(e: gitlab::GitlabError) -> Error {
    use gitlab::GitlabError;
    match e {
        // The connection check goes through the REST client, so route its
        // ApiError through the same classifier as every other call.
        GitlabError::Api { source } => classify(source),
        // Transport failure or an empty reply — network-level, safe to retry.
        e @ (GitlabError::Communication { .. } | GitlabError::NoResponse { .. }) => {
            Error::Transient(e.to_string())
        }
        // URL/auth-header/HTTP-status/GraphQL/JSON failures are permanent.
        other => Error::Gitlab(other.to_string()),
    }
}

/// Convert a raw JSON value from the GitLab issues API into an [`Issue`] plus
/// the labels needed to compute `graph_status` later. Missing or malformed
/// fields fall back to zero / empty-string defaults so a single bad response
/// does not crash the whole list. `graph_status` is left empty here — the
/// handler fills it after consulting the board cache.
fn issue_with_labels(v: &serde_json::Value) -> IssueWithLabels {
    let labels = labels_from(v);

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

/// Extract the `labels` string array; missing or malformed → empty.
fn labels_from(v: &serde_json::Value) -> Vec<String> {
    v["labels"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|l| l.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse an RFC 3339 timestamp value into unix seconds; missing or malformed
/// → 0 (sorts oldest, never blocks storing the entry).
fn rfc3339_secs(v: &serde_json::Value) -> u64 {
    v.as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp().max(0) as u64)
        .unwrap_or(0)
}

/// Convert a raw issues-API value into a [`SearchIssue`], in the same
/// defensive style as [`issue_with_labels`]. Callers drop entries whose `id`
/// parsed to 0.
fn search_issue_from_json(v: &serde_json::Value) -> SearchIssue {
    SearchIssue {
        id: v["id"].as_i64().unwrap_or(0),
        iid: v["iid"].as_i64().unwrap_or(0),
        project_id: v["project_id"].as_i64().unwrap_or(0),
        title: v["title"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
        state: v["state"].as_str().unwrap_or("").to_string(),
        labels: labels_from(v),
        parent: v["epic"]["url"].as_str().unwrap_or("").to_string(),
        total_time: v["time_stats"]["human_total_time_spent"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        updated_at_secs: rfc3339_secs(&v["updated_at"]),
    }
}

/// Convert a raw merge-requests-API value into a [`SearchMr`].
fn search_mr_from_json(v: &serde_json::Value) -> SearchMr {
    SearchMr {
        id: v["id"].as_i64().unwrap_or(0),
        iid: v["iid"].as_i64().unwrap_or(0),
        project_id: v["project_id"].as_i64().unwrap_or(0),
        title: v["title"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
        state: v["state"].as_str().unwrap_or("").to_string(),
        labels: labels_from(v),
        assignees: mr_assignees_from(v),
        updated_at_secs: rfc3339_secs(&v["updated_at"]),
    }
}

/// Extract `assignees[].{id, username}`; entries without a numeric id are
/// dropped (they couldn't drive the assigned-to-me filter anyway).
fn mr_assignees_from(v: &serde_json::Value) -> Vec<MrAssignee> {
    v["assignees"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|a| {
                    Some(MrAssignee {
                        id: a["id"].as_i64()?,
                        username: a["username"].as_str().unwrap_or("").to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Convert a raw projects-API value (`simple=true` fields) into a
/// [`SearchProject`].
fn search_project_from_json(v: &serde_json::Value) -> SearchProject {
    SearchProject {
        id: v["id"].as_i64().unwrap_or(0),
        name: v["name"].as_str().unwrap_or("").to_string(),
        path: v["path_with_namespace"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
    }
}

/// Convert a raw groups-API value into a [`SearchGroup`].
fn search_group_from_json(v: &serde_json::Value) -> SearchGroup {
    SearchGroup {
        id: v["id"].as_i64().unwrap_or(0),
        name: v["name"].as_str().unwrap_or("").to_string(),
        path: v["full_path"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
    }
}

/// `POST /projects/:project_id/<kind>/:iid/add_spent_time`
struct AddSpentTime<'a> {
    kind: Issuable,
    project_id: i64,
    iid: i64,
    duration: &'a str,
    summary: Option<&'a str>,
}

impl gitlab::api::Endpoint for AddSpentTime<'_> {
    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/{}/{}/add_spent_time",
            self.project_id,
            self.kind.path_segment(),
            self.iid
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
            "query": r#"
                mutation($id: IssuableID!, $time: String!, $summary: String!, $spent: Time) {
                    timelogCreate(input: { issuableId: $id, timeSpent: $time, summary: $summary, spentAt: $spent }) {
                        errors
                    }
                }
            "#,
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

/// `GET /user` — returns the authenticated user's profile. Only `id` is used.
struct CurrentUserEndpoint;

impl gitlab::api::Endpoint for CurrentUserEndpoint {
    fn method(&self) -> http::Method {
        http::Method::GET
    }

    fn endpoint(&self) -> Cow<'static, str> {
        "user".into()
    }
}

/// `GET /projects/:project_id/<kind>/:iid`. Used to read the existing
/// assignee list before mutating it.
struct GetIssuableEndpoint {
    kind: Issuable,
    project_id: i64,
    iid: i64,
}

impl gitlab::api::Endpoint for GetIssuableEndpoint {
    fn method(&self) -> http::Method {
        http::Method::GET
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/{}/{}",
            self.project_id,
            self.kind.path_segment(),
            self.iid
        )
        .into()
    }
}

/// `PUT /projects/:project_id/<kind>/:iid` with `assignee_ids=[...]`.
struct UpdateAssigneesEndpoint {
    kind: Issuable,
    project_id: i64,
    iid: i64,
    assignee_ids: Vec<i64>,
}

impl gitlab::api::Endpoint for UpdateAssigneesEndpoint {
    fn method(&self) -> http::Method {
        http::Method::PUT
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/{}/{}",
            self.project_id,
            self.kind.path_segment(),
            self.iid
        )
        .into()
    }

    fn body(&self) -> std::result::Result<Option<(&'static str, Vec<u8>)>, gitlab::api::BodyError> {
        let body = serde_json::json!({"assignee_ids": self.assignee_ids});
        Ok(Some(("application/json", serde_json::to_vec(&body)?)))
    }
}

/// Compute the new assignee list when adding (`add=true`) or removing
/// (`add=false`) `self_id`. Returns `None` when no change is needed — the
/// caller can skip the PUT entirely.
fn compute_new_assignees(current: &[i64], self_id: i64, add: bool) -> Option<Vec<i64>> {
    let already = current.contains(&self_id);
    if add == already {
        return None;
    }
    if add {
        let mut out = current.to_vec();
        out.push(self_id);
        Some(out)
    } else {
        Some(
            current
                .iter()
                .copied()
                .filter(|id| *id != self_id)
                .collect(),
        )
    }
}

/// `PUT /projects/:project_id/<kind>/:iid` with `state_event=close`.
struct CloseEndpoint {
    kind: Issuable,
    project_id: i64,
    iid: i64,
}

impl gitlab::api::Endpoint for CloseEndpoint {
    fn method(&self) -> http::Method {
        http::Method::PUT
    }

    fn endpoint(&self) -> Cow<'static, str> {
        format!(
            "projects/{}/{}/{}",
            self.project_id,
            self.kind.path_segment(),
            self.iid
        )
        .into()
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
            "query": r#"
                query($start: Time!) {
                    currentUser {
                        timelogs(startTime: $start) {
                            nodes {
                                id
                                timeSpent
                                spentAt
                                summary
                                issue { iid title webUrl }
                            }
                        }
                    }
                }
            "#,
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Anchors the exact output grammar; the shape over all inputs is covered
    /// by `format_duration_renders_whole_minutes_of_any_input`.
    #[test]
    fn format_duration_pins_the_gitlab_style_grammar() {
        assert_eq!(format_duration(5400), "1h 30m");
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(0), "0m");
    }

    /// Split a `"2h 5m"`-style rendering back into (hours, minutes).
    fn parse_h_m(s: &str) -> (u64, u64) {
        let (mut hours, mut mins) = (0, 0);
        for part in s.split(' ') {
            if let Some(h) = part.strip_suffix('h') {
                hours = h.parse().unwrap();
            } else if let Some(m) = part.strip_suffix('m') {
                mins = m.parse().unwrap();
            } else {
                panic!("unexpected part {part:?} in {s:?}");
            }
        }
        (hours, mins)
    }

    proptest! {
        #[test]
        fn format_duration_renders_whole_minutes_of_any_input(secs in any::<u64>()) {
            let out = format_duration(secs);
            prop_assert!(!out.is_empty());
            if secs == 0 {
                prop_assert_eq!(out, "0m");
            } else if secs < 60 {
                prop_assert_eq!(out, format!("{secs}s"));
            } else {
                // Past a minute the seconds remainder is dropped, never shown.
                let (hours, mins) = parse_h_m(&out);
                prop_assert!(mins < 60);
                prop_assert_eq!(hours * 3600 + mins * 60, secs - secs % 60);
            }
        }

        #[test]
        fn parse_gid_roundtrips_any_id(n in any::<u64>()) {
            prop_assert_eq!(parse_gid(&format!("gid://gitlab/Timelog/{n}")), Some(n));
            prop_assert_eq!(parse_gid(&n.to_string()), Some(n));
        }

        #[test]
        fn parse_gid_rejects_non_numeric_tails(tail in "[a-zA-Z ]{0,6}") {
            // The empty tail also covers the trailing-slash and empty-input
            // forms.
            prop_assert_eq!(parse_gid(&format!("gid://gitlab/Timelog/{tail}")), None);
            prop_assert_eq!(parse_gid(&tail), None);
        }
    }

    #[test]
    fn issuable_maps_to_rest_segment_and_gid_type() {
        assert_eq!(Issuable::Issue.path_segment(), "issues");
        assert_eq!(Issuable::MergeRequest.path_segment(), "merge_requests");
        assert_eq!(Issuable::Issue.gid_type(), "Issue");
        assert_eq!(Issuable::MergeRequest.gid_type(), "MergeRequest");
    }

    /// The write endpoints render the same URL shape per kind — a wrong
    /// segment here would silently hit the wrong resource class.
    #[test]
    fn write_endpoints_render_kind_specific_paths() {
        use gitlab::api::Endpoint;

        for (kind, seg) in [
            (Issuable::Issue, "issues"),
            (Issuable::MergeRequest, "merge_requests"),
        ] {
            let close = CloseEndpoint {
                kind,
                project_id: 7,
                iid: 42,
            };
            assert_eq!(close.endpoint(), format!("projects/7/{seg}/42"));

            let spend = AddSpentTime {
                kind,
                project_id: 7,
                iid: 42,
                duration: "1h",
                summary: None,
            };
            assert_eq!(
                spend.endpoint(),
                format!("projects/7/{seg}/42/add_spent_time")
            );

            let get = GetIssuableEndpoint {
                kind,
                project_id: 7,
                iid: 42,
            };
            assert_eq!(get.endpoint(), format!("projects/7/{seg}/42"));

            let update = UpdateAssigneesEndpoint {
                kind,
                project_id: 7,
                iid: 42,
                assignee_ids: vec![1],
            };
            assert_eq!(update.endpoint(), format!("projects/7/{seg}/42"));
        }
    }

    #[test]
    fn issue_with_labels_complete() {
        let v = serde_json::json!({
            "id": 123,
            "iid": 7,
            "project_id": 9,
            "title": "Fix it",
            "web_url": "https://example.com/issues/7",
            "state": "opened",
            "epic": { "url": "https://example.com/epics/1" },
            "time_stats": { "human_total_time_spent": "2h" },
            "labels": ["bug", "high"],
        });
        let r = issue_with_labels(&v);
        assert_eq!(r.issue.id, 123);
        assert_eq!(r.issue.iid, 7);
        assert_eq!(r.issue.project_id, 9);
        assert_eq!(r.issue.title, "Fix it");
        assert_eq!(r.issue.web_url, "https://example.com/issues/7");
        assert_eq!(r.issue.state, "opened");
        assert_eq!(r.issue.parent, "https://example.com/epics/1");
        assert_eq!(r.issue.total_time, "2h");
        assert!(
            r.issue.graph_status.is_empty(),
            "graph_status is filled later"
        );
        assert_eq!(r.labels, vec!["bug".to_string(), "high".to_string()]);
    }

    #[test]
    fn issue_with_labels_missing_fields_default() {
        let v = serde_json::json!({});
        let r = issue_with_labels(&v);
        assert_eq!(r.issue.id, 0);
        assert_eq!(r.issue.iid, 0);
        assert_eq!(r.issue.project_id, 0);
        assert!(r.issue.title.is_empty());
        assert!(r.issue.web_url.is_empty());
        assert!(r.issue.state.is_empty());
        assert!(r.issue.parent.is_empty());
        assert!(r.issue.total_time.is_empty());
        assert!(r.labels.is_empty());
    }

    proptest! {
        #[test]
        fn compute_new_assignees_add_appends_exactly_when_absent(
            current in proptest::collection::vec(0i64..20, 0..8),
            self_id in 0i64..20,
        ) {
            match compute_new_assignees(&current, self_id, true) {
                None => prop_assert!(current.contains(&self_id), "no-op only when already assigned"),
                Some(new) => {
                    prop_assert!(!current.contains(&self_id));
                    prop_assert_eq!(*new.last().unwrap(), self_id);
                    prop_assert_eq!(new[..new.len() - 1].to_vec(), current);
                }
            }
        }

        #[test]
        fn compute_new_assignees_remove_drops_exactly_the_self_id(
            current in proptest::collection::vec(0i64..20, 0..8),
            self_id in 0i64..20,
        ) {
            match compute_new_assignees(&current, self_id, false) {
                None => prop_assert!(!current.contains(&self_id), "no-op only when not assigned"),
                Some(new) => {
                    prop_assert!(current.contains(&self_id));
                    let expected: Vec<i64> =
                        current.iter().copied().filter(|id| *id != self_id).collect();
                    prop_assert_eq!(new, expected);
                }
            }
        }

        #[test]
        fn compute_new_assignees_add_then_remove_restores_the_original(
            current in proptest::collection::vec(0i64..20, 0..8),
            self_id in 20i64..40, // guaranteed absent from `current`
        ) {
            let added = compute_new_assignees(&current, self_id, true).expect("absent → change");
            let removed = compute_new_assignees(&added, self_id, false).expect("present → change");
            prop_assert_eq!(removed, current);
        }
    }

    #[test]
    fn search_mr_from_json_captures_assignees() {
        let v = serde_json::json!({
            "id": 5, "iid": 2, "project_id": 9,
            "title": "t", "web_url": "u", "state": "opened",
            "assignees": [
                { "id": 42, "username": "me" },
                { "id": 43 },                       // missing username → kept, empty name
                { "username": "ghost" },            // missing id → dropped
            ],
        });
        let m = search_mr_from_json(&v);
        assert_eq!(
            m.assignees,
            vec![
                MrAssignee {
                    id: 42,
                    username: "me".into()
                },
                MrAssignee {
                    id: 43,
                    username: String::new()
                },
            ]
        );

        let bare = search_mr_from_json(&serde_json::json!({"id": 1}));
        assert!(bare.assignees.is_empty(), "missing assignees array → empty");
    }

    #[test]
    fn issue_with_labels_filters_non_string_labels() {
        let v = serde_json::json!({
            "labels": ["ok", 42, null, "good"],
        });
        let r = issue_with_labels(&v);
        assert_eq!(r.labels, vec!["ok".to_string(), "good".to_string()]);
    }
}
