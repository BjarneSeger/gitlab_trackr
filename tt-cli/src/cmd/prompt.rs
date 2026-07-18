//! `tt prompt` — interactive issue/MR picker + time logger.
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
use gitlab_trackr_api::{Issue, MergeRequest, VarlinkClientInterface};
use inquire::{InquireError, Select, Text};

use crate::refspec::{self, RefKind};
use crate::{client, config, state};

/// One pickable issuable, wrapping the generated structs (which we don't own
/// and which don't implement `Display`) so `Select` can render them with the
/// GitLab sigil: `#42` for issues, `!7` for MRs.
enum Choice {
    Issue(Issue),
    Mr(MergeRequest),
}

impl Choice {
    fn kind(&self) -> RefKind {
        match self {
            Choice::Issue(_) => RefKind::Issue,
            Choice::Mr(_) => RefKind::Mr,
        }
    }

    fn project_id(&self) -> i64 {
        match self {
            Choice::Issue(i) => i.project_id,
            Choice::Mr(m) => m.project_id,
        }
    }

    fn iid(&self) -> i64 {
        match self {
            Choice::Issue(i) => i.iid,
            Choice::Mr(m) => m.iid,
        }
    }

    fn title(&self) -> &str {
        match self {
            Choice::Issue(i) => &i.title,
            Choice::Mr(m) => &m.title,
        }
    }
}

impl fmt::Display for Choice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{:<5} {}",
            refspec::sigil(self.kind()),
            self.iid(),
            self.title()
        )
    }
}

struct PromptAnswers {
    picked: Choice,
    duration: String,
    summary: Option<String>,
}

pub async fn run() -> Result<()> {
    let suggested = state::load().unwrap_or_default().elapsed_suggestion();
    if run_with_default_duration(suggested).await? {
        // A successful log resets the interval — so nushell's `tt tick
        // --mode remind` nudge stops and the next elapsed-time suggestion is
        // measured from now.
        let mut st = state::load().unwrap_or_default();
        st.last_prompt = state::now_secs();
        state::save(&st).context("saving state")?;
    }
    Ok(())
}

/// Run the interactive flow. `suggested_duration` pre-fills the duration
/// input (so the user just hits Enter to accept the elapsed-time suggestion);
/// `None` falls back to the configured `default_duration`. Returns `true` if a
/// time entry was logged, `false` if the user skipped or had no assigned issues.
pub async fn run_with_default_duration(suggested_duration: Option<String>) -> Result<bool> {
    let cfg = config::load()?;
    let socket = cfg.socket.clone().unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let issues = client
        .get_assigned_issues(None)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("GetAssignedIssues", e))?
        .issues;
    let mrs = client
        .get_assigned_merge_requests(None)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("GetAssignedMergeRequests", e))?
        .merge_requests;

    if issues.is_empty() && mrs.is_empty() {
        println!("no assigned issues or merge requests");
        return Ok(false);
    }

    let suggested = suggested_duration.unwrap_or(cfg.default_duration);

    // inquire's prompts are synchronous, blocking terminal I/O. Run them off
    // the async executor thread so the single-threaded runtime isn't blocked
    // for the (potentially long) duration of user input.
    let answers = tokio::task::spawn_blocking(move || -> Result<Option<PromptAnswers>> {
        // Issues first (the primary tracking objects), MRs after — the daemon
        // pre-sorts MRs newest-updated first.
        let choices: Vec<Choice> = issues
            .into_iter()
            .map(Choice::Issue)
            .chain(mrs.into_iter().map(Choice::Mr))
            .collect();

        let picked = match Select::new("What are you working on?", choices).prompt() {
            Ok(p) => p,
            Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => {
                println!("(skipped)");
                return Ok(None);
            }
            Err(e) => return Err(e).context("issue picker"),
        };

        let duration = match Text::new("Duration:")
            .with_initial_value(&suggested)
            .prompt()
        {
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

        Ok(Some(PromptAnswers {
            picked,
            duration,
            summary,
        }))
    })
    .await??;

    let Some(PromptAnswers {
        picked,
        duration,
        summary,
    }) = answers
    else {
        return Ok(false);
    };

    let kind = picked.kind();
    client
        .post_time(
            picked.project_id(),
            picked.iid(),
            refspec::wire(kind),
            duration.clone(),
            summary,
        )
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("PostTime", e))?;

    let mut st = state::load().unwrap_or_default();
    st.last_issue = Some(state::LastIssue {
        project_id: picked.project_id(),
        issue_iid: picked.iid(),
        kind,
    });
    state::save(&st).context("saving state")?;

    println!(
        "logged {duration} on {}{} ({})",
        refspec::sigil(kind),
        picked.iid(),
        picked.title()
    );
    Ok(true)
}
