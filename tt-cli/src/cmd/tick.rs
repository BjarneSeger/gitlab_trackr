//! `tt tick` — the shell pre-prompt hook entry point.
//!
//! Fires on every shell prompt, so the no-op fast path must be cheap (one
//! TOML read, one JSON read, no network). When elapsed time exceeds the
//! configured interval, hands off to [`crate::cmd::prompt`] for the
//! interactive flow.
//!
//! The default `inline` mode does the elapsed check and the interactive
//! prompt in one go — bash/fish/zsh fire their hooks in a clean cooked
//! terminal, so there's no risk of colliding with a TUI. Nushell instead uses
//! `remind`: reedline keeps the terminal in raw mode across its hooks, so a
//! TUI launched from one corrupts the line editor — `remind` only prints a
//! one-line nudge and the user logs with `tt prompt`. See `hooks/nu.txt`.

use anyhow::{Context, Result};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::TickMode;
use crate::{client, cmd::prompt, config, state};

pub async fn run(mode: TickMode) -> Result<()> {
    match mode {
        TickMode::Inline => run_inline().await,
        TickMode::Remind => run_remind().await,
    }
}

async fn run_inline() -> Result<()> {
    let cfg = config::load()?;
    let mut st = state::load().unwrap_or_default();
    let now = state::now_secs();
    let elapsed_secs = now.saturating_sub(st.last_prompt);
    let interval_secs = cfg.interval_minutes.saturating_mul(60);

    if elapsed_secs < interval_secs {
        return Ok(());
    }

    notify_new_failures(&mut st).await;
    // Persist the failure high-water mark before the UI work (best-effort).
    let _ = state::save(&st);

    let suggested = st.elapsed_suggestion();
    let outcome = prompt::run_with_default_duration(suggested).await;

    // Always advance last_prompt — even if the user cancelled, or the daemon
    // was unreachable — so a refused prompt or a downed daemon doesn't re-fire
    // on every shell prompt for the next several seconds. The user gets at
    // most one error per `interval_minutes`, which is the right blast radius.
    // Reload first so we don't clobber a `last_issue` the prompt just wrote.
    let mut st = state::load().unwrap_or_default();
    st.last_prompt = state::now_secs();
    if let Err(e) = state::save(&st).context("saving state") {
        eprintln!("tt tick: state save failed: {e:#}");
    }

    outcome.map(|_| ())
}

/// Nushell path: if the interval has elapsed, print a one-line reminder to log
/// time (and surface any new queued-action failures). Never opens inquire —
/// reedline owns the terminal in raw mode during nushell's hooks, so a TUI
/// launched there corrupts the line editor. The user logs with `tt prompt`,
/// which runs as an ordinary foreground command with a clean terminal and
/// resets the interval. The nudge therefore repeats each command until logged.
async fn run_remind() -> Result<()> {
    let cfg = config::load()?;
    let mut st = state::load().unwrap_or_default();
    let elapsed_secs = state::now_secs().saturating_sub(st.last_prompt);
    let interval_secs = cfg.interval_minutes.saturating_mul(60);

    if elapsed_secs < interval_secs {
        return Ok(());
    }

    notify_new_failures(&mut st).await;
    // Persist the failure high-water mark (best-effort; harmless if it fails).
    let _ = state::save(&st);

    match st.elapsed_suggestion() {
        Some(elapsed) => eprintln!("tt: {elapsed} unlogged — run `tt prompt` to log time"),
        None => eprintln!("tt: run `tt prompt` to log time"),
    }
    Ok(())
}

/// Print a one-line notice for queued actions that have failed since the user
/// last saw one, and advance the high-water mark in `st` (the caller persists
/// it). Best-effort: a missing/unreachable daemon is swallowed silently so a
/// downed daemon never blocks or breaks the prompt. Gated behind the interval
/// check by its call sites, so the cheap no-network fast path is untouched.
async fn notify_new_failures(st: &mut state::State) {
    let Ok(cfg) = config::load() else { return };
    let socket = cfg.socket.unwrap_or_else(client::default_socket);
    let Ok(client) = client::connect(&socket).await else {
        return;
    };
    let Ok(reply) = client.get_failures().call().await else {
        return;
    };

    let mut high_water = st.last_seen_failure_id;
    for f in &reply.failures {
        let id = f.id.max(0) as u64;
        if id <= st.last_seen_failure_id {
            continue;
        }
        let detail = if f.detail.is_empty() {
            String::new()
        } else {
            format!(" ({})", f.detail)
        };
        eprintln!(
            "⚠ tt: queued {} #{}{} failed — {}",
            f.op, f.iid, detail, f.error
        );
        high_water = high_water.max(id);
    }
    st.last_seen_failure_id = high_water;
}
