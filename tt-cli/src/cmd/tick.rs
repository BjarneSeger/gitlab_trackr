//! `tt tick` — the shell pre-prompt hook entry point.
//!
//! Fires on every shell prompt, so the no-op fast path must be cheap (one
//! TOML read, one JSON read, no network). When elapsed time exceeds the
//! configured interval, hands off to [`crate::cmd::prompt`] for the
//! interactive flow.
//!
//! The default `inline` mode does the elapsed check and the interactive
//! prompt in one go — bash/fish/zsh fire their hooks after the previous
//! command finishes, so there's no risk of colliding with a TUI. Nushell
//! splits the same work across two hooks (`defer` from `pre_execution`,
//! `redeem` from `pre_prompt`); see `hooks/nu.txt` for the rationale.

use anyhow::{Context, Result};
use gitlab_trackr_api::VarlinkClientInterface;

use crate::cli::TickMode;
use crate::{client, cmd::prompt, config, state};

pub async fn run(mode: TickMode) -> Result<()> {
    match mode {
        TickMode::Inline => run_inline().await,
        TickMode::Defer => run_defer(),
        TickMode::Redeem => run_redeem().await,
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

    let suggested = suggestion_for(&st, elapsed_secs);

    let outcome = prompt::run_with_default_duration(suggested).await;

    // Always advance last_prompt — even if the user cancelled, or the daemon
    // was unreachable — so a refused prompt or a downed daemon doesn't re-fire
    // on every shell prompt for the next several seconds. The user gets at
    // most one error per `interval_minutes`, which is the right blast radius.
    st.last_prompt = state::now_secs();
    if let Err(e) = state::save(&st).context("saving state") {
        eprintln!("tt tick: state save failed: {e:#}");
    }

    outcome
}

/// Record a "prompt owed" marker if the interval has elapsed. Never opens
/// inquire. Idempotent: a second defer while one is already pending keeps
/// the original suggestion so the user sees the duration that was current
/// when the marker was first set.
fn run_defer() -> Result<()> {
    let cfg = config::load()?;
    let mut st = state::load().unwrap_or_default();
    let elapsed_secs = state::now_secs().saturating_sub(st.last_prompt);
    let interval_secs = cfg.interval_minutes.saturating_mul(60);

    if elapsed_secs < interval_secs || st.pending_prompt.is_some() {
        return Ok(());
    }

    let suggested = suggestion_for(&st, elapsed_secs);
    st.pending_prompt = Some(state::PendingPrompt { suggested });
    state::save(&st).context("saving state")
}

/// If a "prompt owed" marker exists, clear it and run the interactive prompt.
/// Clearing happens BEFORE inquire opens so a re-fire of `pre_prompt` during
/// inquire's redraws sees no marker and exits silently.
async fn run_redeem() -> Result<()> {
    let mut st = state::load().unwrap_or_default();
    let Some(pending) = st.pending_prompt.take() else {
        return Ok(());
    };

    // Persist the cleared state before doing any UI work.
    if let Err(e) = state::save(&st).context("clearing pending prompt") {
        eprintln!("tt tick: state save failed: {e:#}");
        return Ok(());
    }

    notify_new_failures(&mut st).await;
    // Persist the failure high-water mark (best-effort; harmless if it fails).
    let _ = state::save(&st);

    let outcome = prompt::run_with_default_duration(pending.suggested).await;

    // Reload so we don't clobber a `last_issue` that `tt prompt` just wrote.
    let mut st2 = state::load().unwrap_or_default();
    st2.last_prompt = state::now_secs();
    if let Err(e) = state::save(&st2).context("saving state") {
        eprintln!("tt tick: state save failed: {e:#}");
    }

    outcome
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
            f.op, f.issue_iid, detail, f.error
        );
        high_water = high_water.max(id);
    }
    st.last_seen_failure_id = high_water;
}

fn suggestion_for(st: &state::State, elapsed_secs: u64) -> Option<String> {
    // First-ever tick: we don't know how long the user has actually been
    // working, so don't bias them with a giant elapsed-since-epoch suggestion.
    if st.last_prompt == 0 {
        None
    } else {
        Some(format_duration(elapsed_secs))
    }
}

/// Round seconds to whole minutes and render in GitLab time-tracking syntax
/// (`30m`, `1h`, `1h15m`). At least `1m` so a sub-minute elapsed never produces
/// a useless `0m` suggestion in the UI.
fn format_duration(secs: u64) -> String {
    let mins = (secs / 60).max(1);
    if mins < 60 {
        format!("{mins}m")
    } else {
        let h = mins / 60;
        let m = mins % 60;
        if m == 0 {
            format!("{h}h")
        } else {
            format!("{h}h{m}m")
        }
    }
}
