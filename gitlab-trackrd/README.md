# gitlab-trackrd

A small daemon that exposes a [varlink](https://varlink.org) IPC socket for GitLab
time tracking with caching. Provides the basis for other tools in this workspace.

## Installation
gitlab-trackrd provides precompiled releases for arm64 and amd64, with packages for
debian, rpm and arch. See the `releases`-tab.

## Configuration

The daemon reads a TOML config file. Values are layered, highest priority first:

1. `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` — your overrides
2. `/usr/share/gitlab-trackrd/config.toml` — the package-provided default
3. the values baked into the daemon

Every key is optional; a missing key falls back to the next layer. Print an
annotated template with the current defaults:

```sh
cargo run -p gitlab-trackrd --bin gen-config-template
```

Keys are grouped into TOML tables, one per concern:

| Key | Default | Description |
|---|---|---|
| `[server]` `socket` | `$XDG_RUNTIME_DIR/gitlab-trackrd.socket` (falls back to `/tmp`) | Varlink Unix socket the daemon listens on. Ignored under systemd socket activation. |
| `[refresh.quick]` `interval_secs` | `300` | Seconds between quick refreshes of issues, boards, and the recent timelog window. |
| `[refresh.quick]` `window_hours` | `24` | How far back the quick timelog pull reaches (last 24h). Issues and boards are always fetched in full; this bounds only the timelog query. |
| `[refresh.slow]` `interval_secs` | `86400` | Seconds between slow refreshes of the bulk timelog history (once a day). |
| `[refresh.slow]` `window_hours` | `720` | How far back the slow timelog pull reaches (30 days). |
| `[history]` `retention_hours` | `2160` | Total timelog history kept (90 days); fetched once at startup, anything older is pruned. Should be ≥ `refresh.slow.window_hours`. |
| `[queue]` `base_delay_secs` | `1` | Retry-queue backoff initial delay. |
| `[queue]` `max_delay_secs` | `1800` | Retry-queue backoff cap (30 min). |
| `[queue]` `max_lifetime_secs` | `604800` | How long a task retries before being dead-lettered (7 days). |
| `[queue]` `session_wait_secs` | `30` | Worker sleep while the daemon is dormant (no session). |
| `[reconnect]` `enabled` | `true` | Auto-reconnect after an unreachable-GitLab dormancy (down at boot or dropped mid-run). When `false`, recovery is manual (`tt login` or restart). |
| `[reconnect]` `base_delay_secs` | `2` | Auto-reconnect backoff initial delay. |
| `[reconnect]` `max_delay_secs` | `60` | Auto-reconnect backoff cap (1 min); retries continue indefinitely at the cap. |
| `[search]` `population` | `"auto"` | What the search cache holds for issues/MRs: `"tracked"` (what `"auto"` resolves to) = lazy — live search lookups feed the cache and the background sync refreshes only projects with recent evidence of relevance; `"all"` = everything the token can see (`scope=all`; infeasible on large instances); `"member"` = every member project (two fetches per project). Projects and groups are always membership-scoped. |
| `[search]` `partial_interval_secs` | `1800` | Minimum seconds between incremental search-cache syncs (30 min). Restarting inside this window does not re-poll GitLab. |
| `[search]` `full_interval_secs` | `604800` | Seconds between full search-cache resyncs (7 days), which also remove deleted items. |
| `[search]` `live_deadline_ms` | `3000` | Time budget for the live GitLab lookup a search runs while connected (3 s); on timeout the reply falls back to cached results. |
| `[search]` `live_limit` | `100` | Per-kind cap on results the live search lookup fetches (clamped to 1–500). |
| `[search]` `live_debounce_secs` | `30` | Repeating an identical search within this window skips the live lookup; `0` disables the debounce. |
| `[search]` `tracked_retention_hours` | `2160` | How long a project stays tracked after its last evidence of relevance (90 days); evicted projects drop out of the offline search corpus. |

Credentials are configured through the `org.thehoster.gitlab.trackrd.Login`
interface or by just calling `tt login`.

Logging can be set by changing the `GITLAB_TRACKRD_LOG` environment variable to
`trace`, `debug`, `info`, `warn` or `error` (ordered from most to least verbose)

## Checking everything works
```sh
varlinkctl call unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket org.thehoster.gitlab.trackrd.GetAssignedIssues {}
```

## Building locally

### Requirements

- Rust 1.85+
- A GitLab personal access token with at least `read_api` + `write_api` scopes

### Build

```sh
cargo build --release
```

### Run

```sh
cargo run --release
```

## Varlink interface

The interface name is `org.thehoster.gitlab.trackrd`.
For more information, see [the interface docs](docs/varlink_interface.md) and
[the library crate](../gitlab-trackr-api/README.md).
