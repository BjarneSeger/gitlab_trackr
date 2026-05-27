//! Persistent client-side state — currently just the last-prompt timestamp
//! and the last issue the user logged against.
//!
//! Stored as JSON under `$XDG_STATE_HOME/gitlab_trackr/state.json` (with a
//! `data_local_dir()` fallback for platforms that don't define a state dir).
//! Written atomically via tmpfile + rename so a Ctrl-C mid-write can't leave a
//! truncated file that would later fail to parse.

use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    /// Unix epoch seconds of the last completed `tt tick` prompt cycle.
    /// `0` means "never" and triggers a prompt on the next tick.
    pub last_prompt: u64,
    /// Most recently logged-against issue, used by `tt log <iid>` to skip the
    /// `--project-id` flag when the user re-logs against the same issue.
    pub last_issue: Option<LastIssue>,
    /// "Owe-a-prompt" marker for the nushell two-phase hook. `tt tick --mode
    /// defer` sets this when the interval has elapsed; `tt tick --mode
    /// redeem` clears it and runs the interactive prompt. See `hooks/nu.txt`.
    pub pending_prompt: Option<PendingPrompt>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LastIssue {
    pub project_id: i64,
    pub issue_iid: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PendingPrompt {
    /// Pre-computed duration suggestion (`"30m"`, `"1h15m"`) captured at the
    /// moment `--mode defer` decided a prompt was owed. `None` is reserved
    /// for the first-ever tick so the redeem step doesn't bias the user
    /// with a giant elapsed-since-epoch suggestion.
    pub suggested: Option<String>,
}

/// Current unix time in seconds. Clock skew before the epoch (negative
/// duration) collapses to `0`, which makes `now - last_prompt` saturate
/// rather than panic.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn state_path() -> PathBuf {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gitlab_trackr/state.json")
}

/// Read and parse the state file, returning defaults if it's absent.
pub fn load() -> Result<State> {
    let path = state_path();
    if !path.exists() {
        return Ok(State::default());
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let s: State = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(s)
}

/// Atomically persist `state`.
///
/// Writes to `state.json.tmp` first then renames into place — `rename(2)` is
/// atomic on POSIX, so a concurrent reader either sees the previous file or
/// the new one, never a half-written one.
pub fn save(state: &State) -> Result<()> {
    let path = state_path();
    let parent = path
        .parent()
        .context("state path has no parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating {}", parent.display()))?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        let bytes = serde_json::to_vec_pretty(state)?;
        f.write_all(&bytes)?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}
