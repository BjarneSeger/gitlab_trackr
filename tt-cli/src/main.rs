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
/// GitLab terminology note: every issue/MR has both a global `id` and a
/// per-project `iid` (the `#42` / `!7` shown in the UI). The varlink API needs
/// `project_id`, `iid`, and the issuable kind to address one; users almost
/// always know the `iid` but rarely the `project_id`, so the issuable-acting
/// commands accept a `#42`/`!42`-style ref positionally (see [`refspec`]) and
/// resolve the project lazily (see [`cmd::project`]).
mod cli;
mod client;
mod cmd;
mod config;
mod friendly;
mod refspec;
mod state;

use cli::{Cli, Command};

/// Single-thread tokio flavour: each invocation does at most one varlink
/// round-trip plus stdin/stdout work, so a multi-thread runtime would just add
/// startup overhead to the hot `tt tick` path (fires on every shell prompt).
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let output = args.output;
    match args.command {
        Command::List { groups, mrs } => cmd::list::run(groups, mrs, output).await,
        Command::Search {
            query,
            kinds,
            limit,
        } => cmd::search::run(query, kinds, limit, output).await,
        Command::Log {
            issuable,
            duration,
            mr,
            project_id,
            summary,
        } => cmd::log::run(&issuable, duration, mr, project_id, summary).await,
        Command::Prompt => cmd::prompt::run().await,
        Command::Tick { mode } => cmd::tick::run(mode).await,
        Command::Hook { shell } => {
            cmd::hook::run(shell);
            Ok(())
        }
        Command::Refresh {
            quick,
            slow,
            stale,
            issues,
            search,
        } => cmd::refresh::run(quick, slow, stale, issues, search).await,
        Command::Config { action } => {
            cmd::config::run(action);
            Ok(())
        }
        Command::Login { host } => cmd::login::run(host).await,
        Command::Logout => cmd::logout::run().await,
        Command::Whoami => cmd::whoami::run(output).await,
        Command::Close {
            issuable,
            mr,
            project_id,
        } => cmd::close::run(&issuable, mr, project_id).await,
        Command::Assign {
            issuable,
            mr,
            project_id,
        } => cmd::assign::run(&issuable, mr, project_id).await,
        Command::Unassign {
            issuable,
            mr,
            project_id,
        } => cmd::unassign::run(&issuable, mr, project_id).await,
        Command::History { days } => cmd::history::run(output, days).await,
        Command::Queue { action } => cmd::queue::run(action, output).await,
    }
}
