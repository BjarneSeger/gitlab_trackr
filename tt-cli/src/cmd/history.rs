//! `tt history` — recent time-tracking events from the daemon.
//!
//! Pulls the timelogs the daemon has stored for the last `days` days (default
//! 7, up to the 90-day retention). Events are already sorted newest-first.

use anyhow::Result;
use chrono::{DateTime, Utc};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::OutputFormat;
use crate::{client, config};

pub async fn run(output: OutputFormat, days: u32) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;
    let reply = client
        .get_history(Some(i64::from(days)))
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("GetHistory failed: {e}"))?;

    match output {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&reply.events)?);
        }
        OutputFormat::Text => {
            for e in &reply.events {
                let ts = DateTime::<Utc>::from_timestamp(e.timestamp, 0)
                    .map(|d| d.to_rfc3339())
                    .unwrap_or_else(|| e.timestamp.to_string());
                println!(
                    "{ts}  {:<8}  #{:<5}  {:<6}  {}",
                    e.source, e.iid, e.duration, e.title
                );
            }
        }
    }
    Ok(())
}
