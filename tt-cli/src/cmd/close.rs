//! `tt close <iid>` — close an issue. Project ID resolves through
//! [`crate::cmd::project`] when not explicitly supplied.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cmd::project;
use crate::{client, config};

pub async fn run(iid: i64, project_id: Option<i64>) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);

    let project_id = match project_id {
        Some(p) => p,
        None => project::resolve(iid, &socket).await?,
    };

    let client = client::connect(&socket).await?;
    client
        .close_issue(project_id, iid)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("CloseIssue", e))?;

    println!("closed !{iid} (project {project_id})");
    Ok(())
}
