# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

GitLab time-tracking helpers: a caching daemon (`gitlab-trackrd`) that talks to GitLab and serves a varlink IPC socket, and a thin CLI (`tt`, crate `tt-cli`) that talks only to the daemon. Cargo workspace, Rust edition 2024, requires Rust 1.85+.

## Commands

```sh
cargo build                                   # whole workspace
cargo test                                    # all tests (inline #[cfg(test)] modules, no tests/ dirs)
cargo test -p gitlab-trackrd <name>           # single test by name filter
cargo fmt                                     # formatting is enforced; run before committing
cargo run -p gitlab-trackrd --bin gen-config-template   # annotated default daemon config
```

Daemon logging: `GITLAB_TRACKRD_LOG=debug` (env-filter syntax, default `gitlab_trackrd=info`).

To verify daemon/CLI changes end-to-end at the real varlink surface, use the `verify` skill (`.claude/skills/verify/SKILL.md`): it builds both binaries and runs an isolated instance via `XDG_*` overrides. Note the daemon reads real keychain credentials and talks to the real GitLab — avoid driving write commands during verification.

## Workspace layout

- `gitlab-trackr-api/` — the varlink interface crate. **Single source of truth is `gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink`**; Rust types/traits are generated from it at build time (`build.rs` + `varlink_generator`). Versioned independently from the workspace and dual-licensed MIT/Apache-2.0 (the rest is GPL-3.0-only).
- `gitlab-trackrd/` — the daemon. Human-facing interface docs in `gitlab-trackrd/docs/varlink_interface.md` — keep in sync with the `.varlink` file.
- `tt-cli/` — binary `tt`. One module per subcommand under `src/cmd/`; shell-hook snippets under `src/hooks/`.
- `clients/go/` — **generated** Go binding. After changing the `.varlink` interface, run `go generate ./...` in `clients/go` and commit the result; CI (`go-binding.yml`) fails if the committed binding is stale. Never hand-edit `orgthehostergitlabtrackrd.go`.

So an interface change typically touches: the `.varlink` file → daemon handler impls → `tt` command → `docs/varlink_interface.md` → regenerated Go binding. Use the `interface-change` skill for the full checklist. Related skills: `test-patterns` (mock/test conventions before writing daemon tests), `config-key` (adding daemon config keys).

## Daemon architecture (gitlab-trackrd)

The daemon is built around a shared session slot and the principle that it **never refuses to start or serve**:

- **Session state**: `ConnState` (`Connected(Session)` | `Dormant(DormancyReason)`) lives in `SessionSlot = Arc<RwLock<ConnState>>` (`handlers/mod.rs`). Dormant still serves cached reads; auth-requiring calls reply `NotAuthenticated` carrying the specific dormancy reason for the CLI to report.
- **Credentials** come only from the OS keychain (`secrets.rs`; oo7/Secret Service on Linux, Keychain on macOS), set via `tt login` — never from config files.
- **GitLab access** is confined to `gitlab.rs`, the only module that knows the `gitlab` crate. It sits behind the `GitlabApi` trait, which tests mock (`handlers/tests.rs`).
- **Caching**: fjall database at `$XDG_DATA_HOME/gitlab-trackrd/db/`, wrapped by `KvStore` (`db.rs`) and the typed caches `IssueCache`/`BoardCache`/`HistoryCache`. There is deliberately **no TTL**: background sync owns freshness, read handlers are pure cache readers serving whatever was last synced.
- **Refresh** (`handlers/refresh.rs`): startup warm-up (issues/boards first, then history backfill — order matters, history enrichment reads project IDs from the issue cache), a quick tier (default 5 min: issues, boards, recent timelog window) and a slow tier (daily: bulk history + retention pruning). Every tier self-gates on persisted last-run stamps (`refresh_meta.rs`, mirroring the search sync's `search_meta_v1`), advanced only on success — rapid restarts inside an interval serve the persisted caches instead of re-polling GitLab; `ClearCache` zeroes the stamps to force a refill. Intervals are re-read each tick so the hot config reload (`reload.rs`) applies without restart.
- **Writes** (`PostTime`, `CloseIssue`, …) go through the persistent `RetryQueue` (`queue.rs`): fjall-backed, exponential backoff, dead-lettered after the retry window and surfaced via `tt queue`. A transient write failure queues the task; it does *not* demote the session.
- **Reconnect** (`reconnect.rs`): a background supervisor retries the connection whenever the session is `Dormant(Unreachable)` — at boot or after a background refresh demotes it mid-run (`commit_unreachable`, guarded on client identity so a stale in-flight failure can't clobber a fresh `tt login` session). Only background-refresh paths demote; they are the demotion authority.
- **Config**: layered TOML via confique (user `$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` → package default → baked-in), hot-reloaded by `reload.rs` watching the file.

## tt-cli conventions

`tt` is deliberately thin: argument parsing, local state, interactive UI — all GitLab access goes through the daemon socket. GitLab issues have a global `id` and a per-project `iid`; users know the `iid`, so issue-acting commands take `iid` positionally and resolve the project lazily (`cmd/project.rs`).

## Error handling

Prefer thiserror `#[from]` derivation over hand-written `From` impls. The daemon distinguishes transient (network) from permanent errors (`error.rs`); that split drives queue-retry vs dead-letter and session demotion vs plain logging.
