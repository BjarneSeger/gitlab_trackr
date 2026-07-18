//! `tt log` — non-interactive time logging.
//!
//! Designed for scripting and one-off use ("I forgot to start the prompt,
//! just log 45m on #42" — or `'!42'` for a merge request). Tries hard to
//! avoid making the user supply `--project-id` — see [`crate::cmd::project`]
//! for the precedence rules.

use anyhow::{Context, Result};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cmd::project;
use crate::{client, config, refspec, state};

pub async fn run(
    issuable: &str,
    duration: String,
    mr: bool,
    project_id: Option<i64>,
    summary: Option<String>,
) -> Result<()> {
    let reference = refspec::parse(issuable)?;
    let kind = refspec::resolve_kind(reference, mr)?;
    let iid = reference.iid;
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);

    let project_id = match project_id {
        Some(p) => p,
        None => project::resolve(iid, kind, &socket).await?,
    };

    let client = client::connect(&socket).await?;
    client
        .post_time(
            project_id,
            iid,
            refspec::wire(kind),
            duration.clone(),
            summary.clone(),
        )
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("PostTime", e))?;

    let mut st = state::load().unwrap_or_default();
    st.last_issue = Some(state::LastIssue {
        project_id,
        issue_iid: iid,
        kind,
    });
    state::save(&st).context("saving state")?;

    println!(
        "logged {duration} on {}{iid} (project {project_id})",
        refspec::sigil(kind)
    );
    Ok(())
}
