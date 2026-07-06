//! `tt assign <iid>` — add yourself to an issue's assignees (without
//! displacing anyone already on it).

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
        .assign_self(project_id, iid)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("AssignSelf", e))?;

    println!("assigned to !{iid} (project {project_id})");
    Ok(())
}
