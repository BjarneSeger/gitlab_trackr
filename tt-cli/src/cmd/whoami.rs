//! `tt whoami` — show who the daemon is currently authenticated as.

use anyhow::Result;
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::OutputFormat;
use crate::{client, config};

pub async fn run(output: OutputFormat) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let me = client
        .who_am_i()
        .call()
        .await
        .map_err(|e| crate::friendly::friendly("WhoAmI", e))?;

    match output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "host": me.host,
                    "user_id": me.user_id,
                }))?
            );
        }
        OutputFormat::Text => {
            println!("Logged in to {} as user #{}.", me.host, me.user_id);
        }
    }
    Ok(())
}
