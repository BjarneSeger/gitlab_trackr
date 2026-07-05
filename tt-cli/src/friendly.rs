//! Turn the daemon's `NotAuthenticated` varlink error into a friendly,
//! actionable message.
//!
//! The daemon ships a stable machine `reason` code (plus an optional `detail`)
//! on the error; the CLI owns the phrasing here. That split means wording can
//! change without a protocol bump, and an older daemon — which sends no reason
//! — still gets a sensible generic line.

use gitlab_trackr_api::{Error as ApiError, ErrorKind};

/// Map a failed varlink call to an `anyhow::Error` with a user-facing message.
/// `op` is the method name used in the generic (non-auth) fallback, preserving
/// the previous `"<Method> failed: <error>"` output for every other error.
pub fn friendly(op: &str, e: ApiError) -> anyhow::Error {
    if let ErrorKind::NotAuthenticated(args) = e.kind() {
        let reason = args.as_ref().and_then(|a| a.reason.as_deref());
        let detail = args.as_ref().and_then(|a| a.detail.as_deref());
        return anyhow::anyhow!("{}", message_for(reason, detail));
    }
    anyhow::anyhow!("{op} failed: {e}")
}

/// The message for a dormancy `reason` code, with `detail` appended in
/// parentheses when present. Unknown codes and a missing reason (older daemon)
/// fall back to the generic "run `tt login`" line.
fn message_for(reason: Option<&str>, detail: Option<&str>) -> String {
    let base = match reason {
        Some("no-credentials") => "Not connected to GitLab. Run `tt login` to authenticate.",
        Some("token-rejected") => {
            "GitLab rejected the stored token. Run `tt login` to re-authenticate."
        }
        Some("unreachable") => {
            "Can't reach GitLab — the daemon is not connected. \
             Run `tt login` once GitLab is reachable (or restart the daemon)."
        }
        Some("keychain-error") => {
            "Couldn't read your saved credentials from the keychain. \
             Run `tt login` to store them again."
        }
        Some("logged-out") => "Logged out. Run `tt login` to authenticate.",
        _ => "Not connected to GitLab. Run `tt login` to authenticate.",
    };
    match detail {
        Some(d) if !d.is_empty() => format!("{base} ({d})"),
        _ => base.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::message_for;

    #[test]
    fn maps_each_known_reason() {
        assert!(message_for(Some("no-credentials"), None).contains("tt login"));
        assert!(message_for(Some("token-rejected"), None).contains("rejected"));
        assert!(message_for(Some("unreachable"), None).contains("reach GitLab"));
        assert!(message_for(Some("keychain-error"), None).contains("keychain"));
        assert!(message_for(Some("logged-out"), None).contains("Logged out"));
    }

    #[test]
    fn unknown_and_missing_reason_fall_back() {
        let fallback = "Not connected to GitLab. Run `tt login` to authenticate.";
        assert_eq!(message_for(None, None), fallback);
        assert_eq!(message_for(Some("something-new"), None), fallback);
    }

    #[test]
    fn detail_is_appended_when_present() {
        let m = message_for(Some("unreachable"), Some("gitlab.example.com: connection refused"));
        assert!(m.ends_with("(gitlab.example.com: connection refused)"));
        // An empty detail is ignored rather than rendered as "()".
        assert!(!message_for(Some("unreachable"), Some("")).ends_with("()"));
    }
}
