//! `tt unassign <iid>` — remove yourself from an issue's assignee list.
//! Other assignees stay in place.

use anyhow::Result;
use gitlab_trackr_api::{IssuableKind, VarlinkClientInterface};

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
        .unassign_self(project_id, iid, IssuableKind::issue)
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("UnassignSelf", e))?;

    println!("unassigned from !{iid} (project {project_id})");
    Ok(())
}
