# gitlab-trackrd

A small daemon that exposes a [varlink](https://varlink.org) IPC socket for GitLab
time tracking with caching. Provides the basis for other tools in this workspace.

## Installation
gitlab-trackrd provides precompiled releases for arm64 and amd64, with packages for
debian, rpm and arch. See the `releases`-tab.

## Configuration

All configuration is done via environment variables. Only `GITLAB_TOKEN` is
required; everything else has a sensible default.

| Variable | Required | Default | Description |
|---|---|---|---|
| `GITLAB_TOKEN` | **yes** | — | Personal access token |
| `GITLAB_HOST` | no | `gitlab.com` | GitLab instance hostname (e.g. `gitlab.example.com`) |
| `GITLAB_TRACKRD_SOCKET` | no | `unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket` | Varlink socket address.  Falls back to `unix:/tmp/gitlab-trackrd.socket` when `$XDG_RUNTIME_DIR` is unset. |
| `GITLAB_TRACKRD_REFRESH_INTERVAL` | no | `300` | Seconds between refreshes of issues, boards, and the active history tier (last 24h) |
| `GITLAB_TRACKRD_SEMI_REFRESH_INTERVAL` | no | `86400` | Seconds between refreshes of the semi-active history tier (24h–30d). The stale tier (30d–90d) is fetched once at startup and never re-polled. |

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
