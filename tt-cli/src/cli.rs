use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "tt",
    about = "GitLab time-tracking CLI",
    version,
    max_term_width = 100,
)]
pub struct Cli {
    /// Output format for data-returning commands (`list`, `history`, `whoami`).
    /// Mutation commands accept the flag but keep their plain status messages.
    #[arg(
        long = "output",
        short = 'o',
        value_enum,
        default_value_t = OutputFormat::Text,
        global = true,
    )]
    pub output: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// `tt tick` operating mode. Default `inline` is the single-shot path used by
/// bash/fish/zsh. The two-phase `defer` + `redeem` pair exists for nushell,
/// where `pre_execution` would collide with the next command's TUI and
/// `pre_prompt` re-fires during inquire redraws.
#[derive(Clone, Copy, Default, ValueEnum)]
pub enum TickMode {
    /// Check the interval and, if elapsed, run the interactive prompt
    /// directly. Used by bash's PROMPT_COMMAND, fish's fish_postexec, and
    /// zsh's precmd.
    #[default]
    Inline,
    /// Check the interval and, if elapsed, write a "prompt owed" marker into
    /// the state file. Never runs inquire. Used by nushell's `pre_execution`
    /// hook so the prompt doesn't collide with the command the user just
    /// launched.
    Defer,
    /// If a "prompt owed" marker exists, clear it and run the interactive
    /// prompt. Otherwise exit silently. Used by nushell's `pre_prompt` hook;
    /// the clear-before-prompt order means re-fires during inquire redraws
    /// are no-ops.
    Redeem,
}

#[derive(Subcommand)]
pub enum Command {
    /// List issues assigned to you.
    List {
        /// Restrict to issues in the given GitLab group. Repeat the flag to
        /// query several groups; the daemon merges their results.
        #[arg(long = "group", value_name = "GROUP")]
        groups: Vec<String>,
    },
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
    /// Hook entry: if enough time has elapsed, run the interactive prompt;
    /// otherwise exit silently. Most shell hooks use the default `inline`
    /// mode; the nushell hook splits the work in two — see `tt hook nu`.
    Tick {
        #[arg(long, value_enum, default_value_t = TickMode::Inline)]
        mode: TickMode,
    },
    /// Print a shell snippet that wires `tt tick` into the pre-prompt hook.
    Hook {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Drop the daemon's caches and re-fetch. With no flags it clears
    /// everything (issues, boards, and all history tiers); pass tier flags to
    /// target only those. Cleared history tiers are re-fetched immediately.
    Refresh {
        /// Clear the active history tier (the last 24h).
        #[arg(long)]
        active: bool,
        /// Clear the semi-active history tier (24h–30d).
        #[arg(long)]
        semi: bool,
        /// Clear the stale history tier (30d–90d).
        #[arg(long)]
        stale: bool,
        /// Clear the assigned-issue and board caches.
        #[arg(long)]
        issues: bool,
    },
    /// Inspect or scaffold the user configuration file.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Interactively authenticate against GitLab and store the token in the OS
    /// keychain (Keychain on macOS, secret-service on Linux).
    Login {
        /// GitLab host (e.g. `gitlab.com` or `gitlab.mycorp.com`).
        #[arg(long, default_value = "gitlab.com")]
        host: String,
    },
    /// Clear the stored credentials and disconnect the daemon from GitLab.
    Logout,
    /// Print the authenticated user (host + numeric user ID).
    Whoami,
    /// Close an issue.
    Close {
        /// Issue IID.
        iid: i64,
        /// Project ID. If omitted, resolved like `tt log`.
        #[arg(short = 'p', long)]
        project_id: Option<i64>,
    },
    /// Assign yourself to an issue (without removing existing assignees).
    Assign {
        /// Issue IID.
        iid: i64,
        /// Project ID. If omitted, resolved like `tt log`.
        #[arg(short = 'p', long)]
        project_id: Option<i64>,
    },
    /// Remove yourself from an issue's assignee list.
    Unassign {
        /// Issue IID.
        iid: i64,
        /// Project ID. If omitted, resolved like `tt log`.
        #[arg(short = 'p', long)]
        project_id: Option<i64>,
    },
    /// Show recent time-tracking history. Defaults to the last 7 days; widen
    /// up to the 90-day retention with `--days`.
    History {
        /// How many days back to show (the daemon retains up to 90).
        #[arg(long, default_value_t = 7)]
        days: u32,
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
