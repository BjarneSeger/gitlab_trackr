//! Daemon configuration, loaded from a TOML file.
//!
//! Layered with [`confique`]: the user file at
//! `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` wins, then the
//! package-provided default at [`SYSTEM_CONFIG`], then the `#[config(default)]`
//! values baked into the structs below. Every field is optional in the file.
//!
//! The config is grouped into nested sections, one per concern, so the TOML
//! reads as `[server]` / `[refresh]` / `[history]` / `[queue]` tables instead
//! of a flat list of keys. Each [`Config`] field is a sub-struct owned by the
//! module that consumes it, alongside the helpers that turn raw values into the
//! runtime types those modules expect.
//!
//! Credentials are deliberately **not** here — the GitLab host/token live in the
//! OS keychain and are set through the varlink API (`tt login`), never via this
//! file or the environment.
//!
//! Run the `gen-config-template` binary to print an annotated TOML template with
//! the current defaults and doc comments inline (used to generate the shipped
//! default config).

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use confique::Config as ConfiqueConfig;
use tracing::warn;

/// Shared, swappable config read by every consumer at the moment of use, so a
/// hot reload (see `reload`) takes effect without a restart.
///
/// Reads must stay momentary — extract the `Copy` value you need in a single
/// statement so the guard drops before any `.await`; never hold it across one.
pub type SharedConfig = Arc<RwLock<Config>>;

/// Default install path for the package-provided config, layered under the
/// user's own file.
const SYSTEM_CONFIG: &str = "/usr/share/gitlab-trackrd/config.toml";

/// `gitlab-trackrd` configuration.
///
/// Each section is a nested sub-struct; missing keys fall back to the
/// `#[config(default = ...)]` value on the corresponding field.
#[derive(Debug, ConfiqueConfig)]
pub struct Config {
    /// Varlink server / listening socket.
    #[config(nested)]
    pub server: ServerConfig,

    /// Background refresh tiers — cadence plus timelog window for each of the
    /// `quick` and `slow` tiers.
    #[config(nested)]
    pub refresh: RefreshConfig,

    /// Timelog history retention.
    #[config(nested)]
    pub history: HistoryConfig,

    /// Retry-queue backoff and lifetime tuning.
    #[config(nested)]
    pub queue: QueueConfig,

    /// Background auto-reconnect backoff (re-establishing a dormant session).
    #[config(nested)]
    pub reconnect: ReconnectConfig,

    /// Search-cache population and sync cadence.
    #[config(nested)]
    pub search: SearchConfig,
}

/// Varlink server settings (see `server.rs`).
#[derive(Debug, ConfiqueConfig)]
pub struct ServerConfig {
    /// Unix socket the daemon listens on. If unset, defaults to
    /// `$XDG_RUNTIME_DIR/gitlab-trackrd.socket` (then `/tmp/...` as a last
    /// resort). Ignored under systemd socket activation.
    pub socket: Option<String>,
}

impl ServerConfig {
    /// The configured socket, or the `$XDG_RUNTIME_DIR` -> `/tmp` fallback chain
    /// when unset.
    pub fn resolved_socket(&self) -> String {
        if let Some(socket) = &self.socket {
            return socket.clone();
        }
        dirs::runtime_dir()
            .map(|d| {
                d.join("gitlab-trackrd.socket")
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_else(|| "/tmp/gitlab-trackrd.socket".to_string())
    }
}

/// Background refresh tiers (driven from `main.rs`). Work is split by cost and
/// volatility:
///
/// * `quick` — fast-changing and cheap to fetch: assigned issues, board
///   columns, and your most recent timelogs. Polled frequently.
/// * `slow` — the large, slow-moving body of timelog history. Polled rarely.
///
/// Each tier owns both its cadence (`interval_secs`) and how far back its
/// timelog pull reaches (`window_hours`), so the two are configured together
/// instead of split across tables. Keep the windows ordered
/// `quick.window_hours` ≤ `slow.window_hours` ≤ `history.retention_hours`.
#[derive(Debug, ConfiqueConfig)]
pub struct RefreshConfig {
    /// Quick tier: assigned issues, boards, and recent timelogs.
    #[config(nested)]
    pub quick: QuickRefreshConfig,

    /// Slow tier: the bulk of your timelog history.
    #[config(nested)]
    pub slow: SlowRefreshConfig,
}

/// The `quick` refresh tier — see [`RefreshConfig`]. Its own struct (rather than
/// a shared tier type) because confique bakes `#[config(default)]` per type, and
/// the two tiers ship different defaults.
#[derive(Debug, ConfiqueConfig)]
pub struct QuickRefreshConfig {
    /// Seconds between quick refreshes (assigned issues, boards, and the most
    /// recent timelogs). Five minutes by default.
    #[config(default = 300)]
    pub interval_secs: u64,

    /// How far back, in hours, the quick timelog pull reaches. Issues and boards
    /// are always fetched in full; this bounds only the timelog query. (24h by
    /// default.)
    #[config(default = 24)]
    pub window_hours: u64,
}

impl QuickRefreshConfig {
    /// Cadence between refreshes.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }

    /// Timelog look-back span.
    pub fn window(&self) -> Duration {
        Duration::from_hours(self.window_hours)
    }
}

/// The `slow` refresh tier — see [`RefreshConfig`]. Distinct from
/// [`QuickRefreshConfig`] only in its defaults (see that type's note).
#[derive(Debug, ConfiqueConfig)]
pub struct SlowRefreshConfig {
    /// Seconds between slow refreshes of the bulk timelog history. Once a day by
    /// default.
    #[config(default = 86400)]
    pub interval_secs: u64,

    /// How far back, in hours, the slow timelog pull reaches. (30 days by
    /// default.)
    #[config(default = 720)]
    pub window_hours: u64,
}

impl SlowRefreshConfig {
    /// Cadence between refreshes.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }

    /// Timelog look-back span.
    pub fn window(&self) -> Duration {
        Duration::from_hours(self.window_hours)
    }
}

/// Timelog history retention, consumed by `history.rs` via `Handlers`.
#[derive(Debug, ConfiqueConfig)]
pub struct HistoryConfig {
    /// Total timelog history to keep, in hours: fetched once at startup, and
    /// anything older is pruned. Should be ≥ `refresh.slow.window_hours`.
    /// (90 days by default.)
    #[config(default = 2160)]
    pub retention_hours: u64,
}

impl HistoryConfig {
    /// Retention horizon: the oldest timelog kept on disk.
    pub fn retention(&self) -> Duration {
        Duration::from_hours(self.retention_hours)
    }
}

/// Retry-queue timing, consumed by `queue.rs`. The `*_secs` fields come from the
/// TOML; the accessors hand `queue.rs` the [`Duration`]s its worker uses.
#[derive(Debug, Clone, Copy, ConfiqueConfig)]
pub struct QueueConfig {
    /// Retry-queue exponential backoff: initial delay, in seconds.
    #[config(default = 1)]
    pub base_delay_secs: u64,

    /// Retry-queue exponential backoff: maximum delay, in seconds. (30 min.)
    #[config(default = 1800)]
    pub max_delay_secs: u64,

    /// How long, in seconds, a queued task keeps retrying before it is
    /// dead-lettered. (7 days by default.)
    #[config(default = 604800)]
    pub max_lifetime_secs: u64,

    /// How long, in seconds, the retry worker sleeps while the daemon is
    /// dormant (no GitLab session) before checking again.
    #[config(default = 30)]
    pub session_wait_secs: u64,
}

impl QueueConfig {
    /// Initial exponential-backoff delay.
    pub fn base_delay(&self) -> Duration {
        Duration::from_secs(self.base_delay_secs)
    }

    /// Exponential-backoff cap.
    pub fn max_delay(&self) -> Duration {
        Duration::from_secs(self.max_delay_secs)
    }

    /// How long a task keeps retrying before it is dead-lettered.
    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_secs)
    }

    /// How long the worker sleeps while dormant (no session) before retrying.
    pub fn session_wait(&self) -> Duration {
        Duration::from_secs(self.session_wait_secs)
    }
}

/// Background auto-reconnect tuning, consumed by `reconnect.rs`.
///
/// When the daemon is dormant because GitLab was *unreachable* (the stored
/// credentials are known-good), a background task retries the connection with
/// exponential backoff using these values. Unlike the queue there is nothing to
/// dead-letter, so retries continue indefinitely — the delay is merely capped —
/// until the connection succeeds or the session state changes. The cap is kept
/// short (a minute) so recovery is noticed promptly, versus the queue's 30-min
/// cap tuned for long-lived write retries.
#[derive(Debug, Clone, Copy, ConfiqueConfig)]
pub struct ReconnectConfig {
    /// Whether the daemon auto-reconnects after an unreachable-GitLab dormancy
    /// (whether GitLab was down at boot or the connection dropped mid-run). When
    /// `false`, recovery is manual (`tt login` or a restart) — the session still
    /// honestly reports `unreachable`, it just isn't retried. Re-read on every
    /// retry, so disabling it via a hot config reload stops an in-flight reconnect
    /// on the next iteration. The supervisor task is long-lived (parked between
    /// outages) and re-checks the dormant slot on a periodic tick (≤ `max_delay`),
    /// so a `false`→`true` reload is picked up at the next tick — no disconnect,
    /// `tt login`, or restart required.
    #[config(default = true)]
    pub enabled: bool,

    /// Auto-reconnect exponential backoff: initial delay, in seconds.
    #[config(default = 2)]
    pub base_delay_secs: u64,

    /// Auto-reconnect exponential backoff: maximum delay between attempts, in
    /// seconds. (1 min.)
    #[config(default = 60)]
    pub max_delay_secs: u64,
}

impl ReconnectConfig {
    /// Initial exponential-backoff delay.
    pub fn base_delay(&self) -> Duration {
        Duration::from_secs(self.base_delay_secs)
    }

    /// Exponential-backoff cap.
    pub fn max_delay(&self) -> Duration {
        Duration::from_secs(self.max_delay_secs)
    }
}

/// Search-cache sync tuning, consumed by `handlers/search_sync.rs`.
///
/// The search cache holds issues, merge requests, projects, and groups as
/// individual entries. It is synced incrementally (`updated_after` deltas)
/// on the partial cadence and fully resynced — which also reconciles
/// deletions — on the full cadence. Both stamps persist across restarts, so
/// restarting the daemon inside the partial interval does not re-poll GitLab.
#[derive(Debug, ConfiqueConfig)]
pub struct SearchConfig {
    /// What the search cache holds for issues and merge requests:
    /// `"tracked"` (what the default `"auto"` resolves to) populates lazily —
    /// searches ask GitLab directly and cache what they find, and the
    /// background sync refreshes only projects with recent evidence of
    /// relevance (assigned items, time-tracking history, live-search hits
    /// in projects you are a member of).
    /// `"all"` eagerly syncs everything your token can see (GitLab
    /// `scope=all`; infeasible on large instances), `"member"` eagerly syncs
    /// every project you are a member of (two fetches per project). Projects
    /// and groups themselves are always membership-scoped.
    #[config(default = "auto")]
    pub population: SearchPopulation,

    /// Minimum seconds between incremental search-cache syncs. (30 min by
    /// default.)
    #[config(default = 1800)]
    pub partial_interval_secs: u64,

    /// Seconds between full search-cache resyncs, which also remove deleted
    /// items. (7 days by default.)
    #[config(default = 604800)]
    pub full_interval_secs: u64,

    /// Total time budget in milliseconds for the live GitLab lookup a search
    /// may run while connected; on timeout the reply falls back to cached
    /// results only. (3 seconds by default.)
    #[config(default = 3000)]
    pub live_deadline_ms: u64,

    /// Per-kind cap on results a search's live GitLab lookup fetches.
    /// (100 by default.)
    #[config(default = 100)]
    pub live_limit: u64,

    /// Seconds during which repeating an identical search skips the live
    /// GitLab lookup and serves the cache; 0 disables the debounce. (30 by
    /// default.)
    #[config(default = 30)]
    pub live_debounce_secs: u64,

    /// Hours a project stays tracked — its issues and MRs kept searchable
    /// offline and refreshed by the background sync — after the last evidence
    /// of relevance. (90 days by default.)
    #[config(default = 2160)]
    pub tracked_retention_hours: u64,
}

/// Which issues/MRs the search cache is populated with — see
/// [`SearchConfig::population`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchPopulation {
    /// What the daemon thinks is best; currently resolves to `Tracked` on
    /// every host.
    Auto,
    /// Everything the token can see (`scope=all` on the global endpoints).
    All,
    /// Only projects the user is a member of (one fetch per project).
    Member,
    /// Lazy population: live search lookups feed the corpus, and the
    /// background sync refreshes only projects with recent evidence of
    /// relevance (see `handlers/search_sync.rs`).
    Tracked,
}

impl SearchConfig {
    /// Minimum gap between incremental syncs; also the sync loop's tick.
    pub fn partial_interval(&self) -> Duration {
        Duration::from_secs(self.partial_interval_secs)
    }

    /// Gap between full resyncs.
    pub fn full_interval(&self) -> Duration {
        Duration::from_secs(self.full_interval_secs)
    }

    /// Time budget for a search's live GitLab lookup.
    pub fn live_deadline(&self) -> Duration {
        Duration::from_millis(self.live_deadline_ms)
    }

    /// Window during which an identical search skips the live lookup.
    pub fn live_debounce(&self) -> Duration {
        Duration::from_secs(self.live_debounce_secs)
    }

    /// Tracked-project inactivity eviction window.
    pub fn tracked_retention(&self) -> Duration {
        Duration::from_hours(self.tracked_retention_hours)
    }
}

/// `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` (falls back to `./`).
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gitlab-trackrd/config.toml")
}

/// Double `current`, capped at `max`, saturating instead of panicking on
/// overflow — the exponential-backoff step shared by the retry queue and the
/// auto-reconnect loop. `Duration * u32` panics on overflow, so an absurd
/// configured cap would crash the loop without the `checked_mul` guard.
pub fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.checked_mul(2).unwrap_or(max).min(max)
}

/// Clamp a section's exponential-backoff `*_secs` pair into a sane range and
/// warn on any change. A `base` of 0 would busy-spin the retry loop (a zero
/// sleep between attempts); a `max` below `base` would collapse the schedule
/// back to that zero after the first step. Values are otherwise left as-is —
/// `next_backoff` handles the overflow ceiling.
fn normalize_backoff(base_secs: &mut u64, max_secs: &mut u64, section: &str) {
    if *base_secs == 0 {
        warn!(
            section,
            "base_delay_secs of 0 would busy-spin the backoff loop; flooring to 1"
        );
        *base_secs = 1;
    }
    if *max_secs < *base_secs {
        warn!(
            section,
            base = *base_secs,
            max = *max_secs,
            "max_delay_secs is below base_delay_secs; raising it to the base"
        );
        *max_secs = *base_secs;
    }
}

/// Load the layered config: user file → system default → built-in defaults.
///
/// Missing files are treated as empty layers; parse errors propagate. Backoff
/// delays are normalized (see [`normalize_backoff`]) so a hand-edited config
/// can't busy-spin or overflow the retry loops.
pub fn load() -> Result<Config, confique::Error> {
    let mut config = Config::builder()
        .file(config_path())
        .file(Path::new(SYSTEM_CONFIG))
        .load()?;
    normalize_backoff(
        &mut config.reconnect.base_delay_secs,
        &mut config.reconnect.max_delay_secs,
        "reconnect",
    );
    normalize_backoff(
        &mut config.queue.base_delay_secs,
        &mut config.queue.max_delay_secs,
        "queue",
    );
    normalize_search(&mut config.search);
    Ok(config)
}

/// Clamp the search sync cadences into a sane range and warn on any change.
/// A partial interval of 0 would busy-spin the sync loop; a full interval
/// below the partial one would make every sync a full resync.
fn normalize_search(search: &mut SearchConfig) {
    if search.partial_interval_secs < 60 {
        warn!(
            configured = search.partial_interval_secs,
            "search.partial_interval_secs below 60 would hammer GitLab; flooring to 60"
        );
        search.partial_interval_secs = 60;
    }
    if search.full_interval_secs < search.partial_interval_secs {
        warn!(
            partial = search.partial_interval_secs,
            full = search.full_interval_secs,
            "search.full_interval_secs is below partial_interval_secs; raising it to the partial interval"
        );
        search.full_interval_secs = search.partial_interval_secs;
    }
    if search.live_deadline_ms < 50 {
        warn!(
            configured = search.live_deadline_ms,
            "search.live_deadline_ms below 50 leaves no time for any live lookup; flooring to 50"
        );
        search.live_deadline_ms = 50;
    }
    if search.live_limit == 0 {
        warn!("search.live_limit of 0 would fetch nothing; flooring to 1");
        search.live_limit = 1;
    }
    if search.live_limit > 500 {
        warn!(
            configured = search.live_limit,
            "search.live_limit above 500 strains the instance on every search; capping to 500"
        );
        search.live_limit = 500;
    }
    if search.tracked_retention_hours < 24 {
        warn!(
            configured = search.tracked_retention_hours,
            "search.tracked_retention_hours below 24 would evict projects near-immediately; flooring to 24"
        );
        search.tracked_retention_hours = 24;
    }
}

/// Load once and wrap for sharing across the daemon's tasks. Used at startup; a
/// parse error propagates so the caller can fail fast (no prior config exists).
pub fn load_shared() -> Result<SharedConfig, confique::Error> {
    Ok(Arc::new(RwLock::new(load()?)))
}

/// Re-run [`load`] and swap the contents in place.
///
/// On a parse error the existing config is left untouched and the error is
/// returned, so a malformed mid-edit save never disturbs the running daemon —
/// the caller logs and keeps serving the last-good values.
pub fn reload(shared: &SharedConfig) -> Result<(), confique::Error> {
    let fresh = load()?;
    *shared.write().unwrap() = fresh;
    Ok(())
}

/// A fully-defaulted config (no file layers), for tests and benches that need
/// a [`SharedConfig`] without touching the real XDG path.
pub fn defaults() -> Config {
    Config::builder()
        .load()
        .expect("built-in defaults are valid")
}

/// Render an annotated TOML template (current defaults + doc comments inline).
///
/// Used by the `gen-config-template` binary via the library target; the daemon
/// binary itself never calls it.
#[allow(dead_code)]
pub fn template() -> String {
    confique::toml::template::<Config>(confique::toml::FormatOptions::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn reconnect_defaults_are_short_and_enabled() {
        let c = defaults();
        assert!(c.reconnect.enabled);
        assert_eq!(c.reconnect.base_delay(), Duration::from_secs(2));
        assert_eq!(c.reconnect.max_delay(), Duration::from_secs(60));
    }

    proptest! {
        #[test]
        fn next_backoff_doubles_saturating_and_never_exceeds_the_cap(
            current_secs in any::<u64>(),
            max_secs in any::<u64>(),
        ) {
            let current = Duration::from_secs(current_secs);
            let max = Duration::from_secs(max_secs);
            let next = next_backoff(current, max);
            prop_assert!(next <= max);
            match current.checked_mul(2) {
                Some(doubled) => prop_assert_eq!(next, doubled.min(max)),
                // Doubling would overflow `Duration`: saturate, don't panic.
                None => prop_assert_eq!(next, max),
            }
        }
    }

    #[test]
    fn search_defaults() {
        let c = defaults();
        assert_eq!(c.search.population, SearchPopulation::Auto);
        assert_eq!(c.search.partial_interval(), Duration::from_secs(1800));
        assert_eq!(c.search.full_interval(), Duration::from_secs(604800));
        assert_eq!(c.search.live_deadline(), Duration::from_millis(3000));
        assert_eq!(c.search.live_limit, 100);
        assert_eq!(c.search.live_debounce(), Duration::from_secs(30));
        assert_eq!(c.search.tracked_retention(), Duration::from_hours(2160));
    }

    #[test]
    fn search_population_parses_tracked() {
        let s: SearchPopulation = serde_json::from_str(r#""tracked""#).unwrap();
        assert_eq!(s, SearchPopulation::Tracked);
    }

    proptest! {
        #[test]
        fn normalize_backoff_floors_the_base_and_orders_the_pair(
            base_in in any::<u64>(),
            max_in in any::<u64>(),
        ) {
            let (mut base, mut max) = (base_in, max_in);
            normalize_backoff(&mut base, &mut max, "test");
            prop_assert!(base >= 1);
            prop_assert!(max >= base);
            if base_in >= 1 && max_in >= base_in {
                prop_assert_eq!(
                    (base, max),
                    (base_in, max_in),
                    "in-range values are left untouched"
                );
            }
        }

        #[test]
        fn normalize_search_floors_the_partial_and_orders_the_pair(
            partial_in in any::<u64>(),
            full_in in any::<u64>(),
        ) {
            let mut s = defaults().search;
            s.partial_interval_secs = partial_in;
            s.full_interval_secs = full_in;
            normalize_search(&mut s);
            prop_assert!(s.partial_interval_secs >= 60);
            prop_assert!(s.full_interval_secs >= s.partial_interval_secs);
            if partial_in >= 60 && full_in >= partial_in {
                prop_assert_eq!(
                    (s.partial_interval_secs, s.full_interval_secs),
                    (partial_in, full_in),
                    "in-range values are left untouched"
                );
            }
        }

        #[test]
        fn normalize_search_clamps_the_live_and_tracked_knobs(
            deadline_in in any::<u64>(),
            limit_in in any::<u64>(),
            debounce_in in any::<u64>(),
            retention_in in any::<u64>(),
        ) {
            let mut s = defaults().search;
            s.live_deadline_ms = deadline_in;
            s.live_limit = limit_in;
            s.live_debounce_secs = debounce_in;
            s.tracked_retention_hours = retention_in;
            normalize_search(&mut s);
            prop_assert!(s.live_deadline_ms >= 50);
            prop_assert!((1..=500).contains(&s.live_limit));
            prop_assert_eq!(s.live_debounce_secs, debounce_in, "0 is a valid off switch");
            prop_assert!(s.tracked_retention_hours >= 24);
            if deadline_in >= 50 && (1..=500).contains(&limit_in) && retention_in >= 24 {
                prop_assert_eq!(
                    (s.live_deadline_ms, s.live_limit, s.tracked_retention_hours),
                    (deadline_in, limit_in, retention_in),
                    "in-range values are left untouched"
                );
            }
        }
    }
}
