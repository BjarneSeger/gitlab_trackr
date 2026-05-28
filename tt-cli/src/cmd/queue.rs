//! `tt queue` — inspect and manage failed queued actions.
//!
//! A write op that hit a network error is queued and retried by the daemon.
//! If GitLab later rejects it, or the retry window expires, the daemon moves it
//! to a dead-letter store. This command lists those failures and lets the user
//! retry or dismiss them.

use anyhow::Result;
use chrono::{DateTime, Utc};
use gitlab_trackr_api::{VarlinkClient, VarlinkClientInterface};

use crate::cli::{OutputFormat, QueueAction};
use crate::{client, config};

pub async fn run(action: Option<QueueAction>, output: OutputFormat) -> Result<()> {
    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;

    match action {
        None => list(&client, output).await,
        Some(QueueAction::Retry { id }) => {
            client
                .retry_failure(id as i64)
                .call()
                .await
                .map_err(|e| anyhow::anyhow!("RetryFailure failed: {e}"))?;
            println!("re-enqueued failed action {id}");
            Ok(())
        }
        Some(QueueAction::Dismiss { id }) => {
            client
                .dismiss_failure(id as i64)
                .call()
                .await
                .map_err(|e| anyhow::anyhow!("DismissFailure failed: {e}"))?;
            println!("dismissed failed action {id}");
            Ok(())
        }
        Some(QueueAction::Clear) => {
            client
                .clear_failures()
                .call()
                .await
                .map_err(|e| anyhow::anyhow!("ClearFailures failed: {e}"))?;
            println!("cleared all failed actions");
            Ok(())
        }
    }
}

async fn list(client: &VarlinkClient, output: OutputFormat) -> Result<()> {
    let reply = client
        .get_failures()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("GetFailures failed: {e}"))?;

    match output {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&reply.failures)?);
        }
        OutputFormat::Text => {
            if reply.failures.is_empty() {
                println!("no failed actions");
                return Ok(());
            }
            for f in &reply.failures {
                let when = DateTime::<Utc>::from_timestamp(f.failed_at, 0)
                    .map(|d| d.to_rfc3339())
                    .unwrap_or_else(|| f.failed_at.to_string());
                let detail = if f.detail.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", f.detail)
                };
                println!(
                    "[{}] {} #{}{}  —  {}  ({})",
                    f.id, f.op, f.issue_iid, detail, f.error, when
                );
            }
            println!(
                "\nretry with `tt queue retry <id>`, drop with `tt queue dismiss <id>`, \
                 or `tt queue clear`"
            );
        }
    }
    Ok(())
}
