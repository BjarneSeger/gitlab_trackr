//! Custom GitLab API endpoints not provided by the `gitlab` crate.

use std::borrow::Cow;

/// `POST /projects/:project_id/issues/:issue_iid/add_spent_time`
///
/// Records time spent on a GitLab issue.  `duration` must be a GitLab
/// time-tracking string, e.g. `"1h30m"`, `"45m"`, `"2h"`.
///
/// The `gitlab` crate (v0.1811) does not include this endpoint, so it is
/// implemented manually via [`gitlab::api::Endpoint`].
pub struct AddSpentTime<'a> {
    /// Numeric project ID (not the path).
    pub project_id: i64,
    /// Issue IID (the per-project issue number shown in the GitLab UI).
    pub issue_iid: i64,
    /// Time string accepted by the GitLab time-tracking API (e.g. `"1h30m"`).
    pub duration: &'a str,
    /// Optional summary of how the time was spent.
    pub summary: Option<&'a str>,
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
