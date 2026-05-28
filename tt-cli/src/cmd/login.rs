//! `tt login` — interactive GitLab authentication.
//!
//! Opens the GitLab PAT creation page with sensible scope hints, prompts for
//! the resulting token, and hands it to the daemon which validates it,
//! persists it to the OS keychain, and connects. The token never touches a
//! file on disk on the CLI side.

use anyhow::{Context, Result};
use gitlab_trackr_api::VarlinkClientInterface;
use inquire::Password;

use crate::{client, config};

pub async fn run(host: String) -> Result<()> {
    let url = format!(
        "https://{host}/-/user_settings/personal_access_tokens?name=gitlab-trackrd&scopes=api,read_user"
    );

    println!("Opening {url}");
    println!(
        "Generate a token with the `api` and `read_user` scopes, then paste it below."
    );
    if let Err(e) = open::that(&url) {
        eprintln!("(couldn't open browser automatically: {e})");
        eprintln!("Open the URL above manually.");
    }

    let token = tokio::task::spawn_blocking(|| {
        Password::new("Paste the personal access token:")
            .without_confirmation()
            .with_display_mode(inquire::PasswordDisplayMode::Masked)
            .prompt()
            .context("reading token from stdin")
    })
    .await??;
    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("no token entered; aborting");
    }

    let cfg = config::load()?;
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let client = client::connect(&socket).await?;

    client
        .login(host.clone(), token)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("Login failed: {e}"))?;

    let me = client
        .who_am_i()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("WhoAmI failed: {e}"))?;
    println!("Logged in to {} as user #{}.", me.host, me.user_id);
    Ok(())
}
