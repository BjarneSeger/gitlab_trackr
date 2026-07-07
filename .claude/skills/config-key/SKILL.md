---
name: config-key
description: Checklist for adding or changing a daemon config key (confique struct conventions, accessors, clamping, hot-reload rules, which docs to touch and which artifacts are generated).
---

# Adding / changing a daemon config key

Config lives in `gitlab-trackrd/src/config.rs` (confique, layered TOML: user file →
`/usr/share/gitlab-trackrd/config.toml` → baked-in defaults; every key optional).

## 1. The field

- Add it to the sub-struct matching its TOML table (`ServerConfig`, `Quick/SlowRefreshConfig`,
  `HistoryConfig`, `QueueConfig`, `ReconnectConfig`) with `#[config(default = …)]` and a
  doc comment. **The doc comment becomes the annotation in the generated config
  template** — write it for end users, include the default's meaning ("30 min", "90 days").
- New TOML table → new `#[config(nested)]` struct, owned by the module that consumes it.
  confique bakes defaults per *type*, so two tables with the same shape but different
  defaults need two structs (see `QuickRefreshConfig` vs `SlowRefreshConfig`).
- Raw `*_secs`/`*_hours` field + typed accessor is the convention: `interval() -> Duration`,
  `retention() -> Duration`, etc. Consumers never convert units themselves.
- Backoff-style `base/max` pair? Add a `normalize_backoff(..)` call in `load()` so a
  hand-edited config can't busy-spin or invert the schedule.

## 2. The consumer

- Read through `SharedConfig` (`Arc<RwLock<Config>>`) **at the moment of use**: extract
  the value in a single statement so the guard drops before any `.await` — never hold it
  across one.
- Loops re-read their interval each tick; that is what makes hot reload work. Follow
  that shape for anything periodic.
- Hot-reload semantics (`reload.rs`): parse errors keep the last-good config; a changed
  `server.socket` only logs a warning (socket changes need a restart). If the new key
  cannot take effect without a restart, make `reload.rs` warn about it the same way.

## 3. Docs — one manual spot, the rest is generated

- Update the config table in `gitlab-trackrd/README.md` (key, default, description).
  This is the only hand-maintained copy.
- Do **not** touch `gitlab-trackrd/packaging/config.toml` or any template output: the
  shipped default config is regenerated on every release by the goreleaser hook running
  `cargo run -p gitlab-trackrd --bin gen-config-template`.

## 4. Verify

```sh
cargo run -p gitlab-trackrd --bin gen-config-template   # new key + annotation present?
cargo test -p gitlab-trackrd config                     # config unit tests
```

For reload behavior, the `verify` skill's isolated instance + editing
`$XDG_CONFIG_HOME/gitlab-trackrd/config.toml` exercises the watcher end-to-end.
