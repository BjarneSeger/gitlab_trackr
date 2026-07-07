---
name: verify
description: Build, launch, and drive gitlab-trackrd + tt end-to-end in an isolated environment for verifying daemon/CLI changes at their real surface (varlink socket).
---

# Verify gitlab-trackrd changes end-to-end

The daemon (`gitlab-trackrd`) serves a varlink unix socket; the CLI (`tt`) is
the user surface. Both resolve the socket from `$XDG_RUNTIME_DIR/gitlab-trackrd.socket`
and data/config from the XDG dirs, so a fully isolated instance only needs env vars.

## Recipe

```bash
cargo build -p gitlab-trackrd -p tt-cli

S=$(mktemp -d /tmp/gt-verify.XXXX)          # KEEP SHORT — socket path must fit SUN_LEN (~108 chars)
mkdir -p $S/{config,data,runtime}; chmod 700 $S/runtime
export XDG_CONFIG_HOME=$S/config XDG_DATA_HOME=$S/data XDG_RUNTIME_DIR=$S/runtime

RUST_LOG=info ./target/debug/gitlab-trackrd > $S/daemon.log 2>&1 &
# wait for $S/runtime/gitlab-trackrd.socket to appear, then drive:
./target/debug/tt list            # issue cache read
./target/debug/tt history         # timelog history read
./target/debug/tt queue           # dead-letter listing
./target/debug/tt refresh         # clears caches + re-fetches
```

## Gotchas

- **Credentials come from the OS keychain, not XDG** — the daemon will use the
  real `tt login` credentials and talk to the real GitLab (read-only refresh
  fetches). Avoid driving write commands (`tt log`, `tt close`, `tt assign`)
  unless the write target is intentional; they post to the live GitLab.
- **Stale socket**: after SIGKILL the daemon leaves the socket file and a
  restart dies with `AddrInUse` — Use SIGTERM or `rm` the socket before restarting.
- **Storage**: fjall database at `$XDG_DATA_HOME/gitlab-trackrd/db/`.
  Startup deletes legacy `*.redb` files in `$XDG_DATA_HOME/gitlab-trackrd/`;
  plant fakes there to test the cleanup path.
- Daemon starts dormant (still serving) if the keychain has no credentials;
  reads then serve whatever is cached.
