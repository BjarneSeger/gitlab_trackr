//! Miscellaneous helper utilities.

use super::Issue;

/// Convert a raw JSON value returned by the GitLab issues API into the
/// varlink [`Issue`] type.  Missing or malformed fields fall back to
/// zero / empty-string defaults so a single bad response does not crash
/// the whole list.
pub fn issue_from_value(v: &serde_json::Value) -> Issue {
    Issue {
        id: v["id"].as_i64().unwrap_or(0),
        iid: v["iid"].as_i64().unwrap_or(0),
        project_id: v["project_id"].as_i64().unwrap_or(0),
        title: v["title"].as_str().unwrap_or("").to_string(),
        web_url: v["web_url"].as_str().unwrap_or("").to_string(),
        state: v["state"].as_str().unwrap_or("").to_string(),
    }
}

/// Current Unix timestamp in whole seconds.  Used for cache TTL comparisons.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
