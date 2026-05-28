//! `tt prompt` — interactive issue picker + time logger.
//!
//! Shared by [`crate::cmd::tick`], which feeds in an elapsed-time-based
//! duration suggestion.
//!
//! Cancellation contract: Esc / Ctrl-C at any of the three prompts (issue,
//! duration, summary) is a silent skip, not an error — the caller updates the
//! last-prompt timestamp regardless, so a user dismissing a tick prompt isn't
//! re-prompted immediately.

use std::fmt;

use anyhow::{Context, Result};
use gitlab_trackr_api::{Issue, VarlinkClientInterface};
use inquire::{InquireError, Select, Text};

use crate::{client, config, state};

/// Newtype so we can render `Issue` in the `Select` list without touching the
/// generated struct (which we don't own and which doesn't implement `Display`).
struct IssueChoice(Issue);

impl fmt::Display for IssueChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:<5} {}", self.0.iid, self.0.title)
    }
}

struct PromptAnswers {
    issue: Issue,
    duration: String,
    summary: Option<String>,
}

pub async fn run() -> Result<()> {
    run_with_default_duration(None).await
}

/// Run the interactive flow. `suggested_duration` pre-fills the duration
/// input (so the user just hits Enter to accept the elapsed-time suggestion);
/// `None` falls back to the configured `default_duration`.
pub async fn run_with_default_duration(suggested_duration: Option<String>) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.clone().unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let reply = client
        .get_assigned_issues(None)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("GetAssignedIssues failed: {e}"))?;

    if reply.issues.is_empty() {
        println!("no assigned issues");
        return Ok(());
    }

    let issues = reply.issues;
    let suggested = suggested_duration.unwrap_or(cfg.default_duration);

    // Run all inquire prompts in a dedicated blocking thread. Calling
    // blocking terminal I/O directly on the tokio executor thread causes
    // crossterm's raw-mode cleanup to race with the async runtime, leaving
    // the terminal in raw mode after the process exits.
    let answers = tokio::task::spawn_blocking(move || -> Result<Option<PromptAnswers>> {
        let choices: Vec<IssueChoice> = issues.into_iter().map(IssueChoice).collect();

        let picked = match Select::new("What are you working on?", choices).prompt() {
            Ok(p) => p,
            Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => {
                println!("(skipped)");
                return Ok(None);
            }
            Err(e) => return Err(e).context("issue picker"),
        };

        let duration = match Text::new("Duration:").with_initial_value(&suggested).prompt() {
            Ok(d) => d,
            Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => {
                println!("(skipped)");
                return Ok(None);
            }
            Err(e) => return Err(e).context("duration prompt"),
        };

        let summary = match Text::new("Summary (optional):").prompt() {
            Ok(s) if s.trim().is_empty() => None,
            Ok(s) => Some(s),
            Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => None,
            Err(e) => return Err(e).context("summary prompt"),
        };

        Ok(Some(PromptAnswers { issue: picked.0, duration, summary }))
    })
    .await??;

    let Some(PromptAnswers { issue, duration, summary }) = answers else {
        return Ok(());
    };

    client
        .post_time(issue.project_id, issue.iid, duration.clone(), summary)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("PostTime failed: {e}"))?;

    let mut st = state::load().unwrap_or_default();
    st.last_issue = Some(state::LastIssue {
        project_id: issue.project_id,
        issue_iid: issue.iid,
    });
    state::save(&st).context("saving state")?;

    println!("logged {duration} on !{} ({})", issue.iid, issue.title);
    Ok(())
}
