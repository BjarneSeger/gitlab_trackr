//! `tt close <ref>` — close an issue (`42`, `#42`) or merge request (`!42`).
//! Project ID resolves through [`crate::cmd::project`] when not explicitly
//! supplied.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cmd::project;
use crate::{client, config, refspec};

pub async fn run(issuable: &str, mr: bool, project_id: Option<i64>) -> Result<()> {
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
        .close(project_id, iid, refspec::wire(kind))
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("Close", e))?;

    println!(
        "closed {}{iid} (project {project_id})",
        refspec::sigil(kind)
    );
    Ok(())
}
