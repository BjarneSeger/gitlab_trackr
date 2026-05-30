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

| Key | Default | Description |
|---|---|---|
| `socket` | `$XDG_RUNTIME_DIR/gitlab-trackrd.socket` (falls back to `/tmp`) | Varlink Unix socket the daemon listens on. Ignored under systemd socket activation. |
| `refresh_interval` | `300` | Seconds between refreshes of issues, boards, and the active history tier (last 24h). |
| `semi_refresh_interval` | `86400` | Seconds between refreshes of the semi-active history tier (24h–30d). |
| `active_window_hours` | `24` | Active history tier span. |
| `semi_window_hours` | `720` | Semi-active history tier span (30 days). |
| `stale_window_hours` | `2160` | Overall history retention (90 days); the stale band is fetched once at startup. |
| `queue_base_delay_secs` | `1` | Retry-queue backoff initial delay. |
| `queue_max_delay_secs` | `1800` | Retry-queue backoff cap (30 min). |
| `queue_max_lifetime_secs` | `604800` | How long a task retries before being dead-lettered (7 days). |
| `queue_session_wait_secs` | `30` | Worker sleep while the daemon is dormant (no session). |

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
[the library crate](../gitlab_trackr_api/README.md).
