//! `tt` — interactive client for the [`gitlab-trackrd`] varlink daemon.
//!
//! See the per-subcommand modules under [`cmd`] for behaviour. The binary is
//! deliberately thin: all GitLab access goes through the daemon over a unix
//! socket, so `tt` only handles argument parsing, local state (last-prompt
//! timestamp, last-used issue) and the interactive UI.
//!
//! [`gitlab-trackrd`]: ../../gitlab-trackrd/README.md

use anyhow::Result;
use clap::Parser;

/// Clap-derived command-line surface.
///
/// GitLab terminology note: every issue has both a global `id` and a
/// per-project `iid` (the `#42` shown in the UI). The varlink API needs both
/// `project_id` and `issue_iid` to address an issue; users almost always know
/// the `iid` but rarely the `project_id`, so [`cli::Command::Log`] accepts
/// `iid` positionally and resolves the project lazily (see [`cmd::log`]).
mod cli;
mod client;
mod cmd;
mod config;
mod state;

use cli::{Cli, Command};

/// Single-thread tokio flavour: each invocation does at most one varlink
/// round-trip plus stdin/stdout work, so a multi-thread runtime would just add
/// startup overhead to the hot `tt tick` path (fires on every shell prompt).
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::List => cmd::list::run().await,
        Command::Log {
            iid,
            duration,
            project_id,
            summary,
        } => cmd::log::run(iid, duration, project_id, summary).await,
        Command::Prompt => cmd::prompt::run().await,
        Command::Tick => cmd::tick::run().await,
        Command::Hook { shell } => {
            cmd::hook::run(shell);
            Ok(())
        }
        Command::Refresh => cmd::refresh::run().await,
        Command::Config { action } => {
            cmd::config::run(action);
            Ok(())
        }
    }
}
