//! `tt tick` — the shell pre-prompt hook entry point.
//!
//! Fires on every shell prompt, so the no-op fast path must be cheap (one
//! TOML read, one JSON read, no network). When elapsed time exceeds the
//! configured interval, hands off to [`crate::cmd::prompt`] for the
//! interactive flow.

use anyhow::{Context, Result};

use crate::{cmd::prompt, config, state};

pub async fn run() -> Result<()> {
    let cfg = config::load()?;
    let mut st = state::load().unwrap_or_default();
    let now = state::now_secs();
    let elapsed_secs = now.saturating_sub(st.last_prompt);
    let interval_secs = cfg.interval_minutes.saturating_mul(60);

    if elapsed_secs < interval_secs {
        return Ok(());
    }

    // First-ever tick: we don't know how long the user has actually been
    // working, so don't bias them with a giant elapsed-since-epoch suggestion.
    let suggested = if st.last_prompt == 0 {
        None
    } else {
        Some(format_duration(elapsed_secs))
    };

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
