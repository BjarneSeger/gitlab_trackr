//! Clap-derived command-line surface.
//!
//! GitLab terminology note: every issue has both a global `id` and a
//! per-project `iid` (the `#42` shown in the UI). The varlink API needs both
//! `project_id` and `issue_iid` to address an issue; users almost always know
//! the `iid` but rarely the `project_id`, so [`Command::Log`] accepts `iid`
//! positionally and resolves the project lazily (see [`crate::cmd::log`]).

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "tt", about = "GitLab time-tracking CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// List issues assigned to you.
    List,
    /// Log time on an issue non-interactively.
    Log {
        /// Issue IID (per-project number, shown as `#42` in the GitLab UI).
        iid: i64,
        /// Duration string accepted by GitLab (e.g. `30m`, `1h15m`).
        duration: String,
        /// Project ID. If omitted, resolved from the last-used issue cache or
        /// by scanning your assigned issues for one matching `iid`.
        #[arg(short = 'p', long)]
        project_id: Option<i64>,
        /// Optional summary note.
        #[arg(short = 's', long)]
        summary: Option<String>,
    },
    /// Interactively pick an issue and log time.
    Prompt,
    /// Hook entry: if enough time has elapsed, run the interactive prompt; otherwise exit silently.
    Tick,
    /// Print a shell snippet that wires `tt tick` into the pre-prompt hook.
    Hook {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Tell the daemon to drop its cached issue list.
    Refresh,
    /// Inspect or scaffold the user configuration file.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Print an annotated TOML template (with current defaults and doc
    /// comments) to stdout. Pipe into `$XDG_CONFIG_HOME/gitlab_trackr_cli/config.toml`.
    Template,
    /// Print the resolved path to the user config file.
    Path,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Shell {
    Fish,
    Zsh,
    Bash,
    Nu,
}
