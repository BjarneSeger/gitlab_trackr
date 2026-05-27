//! `tt logout` — clear stored credentials and drop the daemon's GitLab
//! connection.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::{client, config};

pub async fn run() -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    client
        .logout()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("Logout failed: {e}"))?;
    println!("Logged out.");
    Ok(())
}
