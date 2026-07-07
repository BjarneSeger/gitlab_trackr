# Various gitlab timetracking helpers

Ever wanted / had to use gitlabs timetracking, but never quite managed to integrate
it into you workflow? Then this is the repo for you! We have:

- [A background daemon that handles auth and caching](gitlab-trackrd/README.md)
- [A cli to communicate with it and to remind you to track](tt-cli/README.md)
- [A ready-to-import Go binding for the daemon's varlink interface](clients/go/README.md)

# Quickstart

## 1. Install

Install the `gitlab-trackr-utils` package (deb, rpm and arch packages plus prebuilt
binaries are on the releases tab). It ships the `gitlab-trackrd` daemon, the `tt`
CLI, shell completions, and systemd user units.

## 2. Start the daemon

The daemon is a systemd user unit — enable and start it:

```sh
systemctl enable --now --user gitlab-trackrd.service
```

## 3. Log in

```sh
tt login --host gitlab.com
```

This walks you through creating a personal access token with the appropriate scopes.
Paste it back to the prompt and you are logged in — the token is stored in your
platform's keystore (Secret Service/keyring on Linux, Keychain on macOS), never in a
file.

The daemon keeps working offline: reads serve the local cache, and time you log
while GitLab is unreachable is queued and posted once it reconnects.

## 4. Get reminded to track

```sh
tt hook YOUR_SHELL >> YOUR_SHELL_RC    # bash, zsh, fish or nu
```

The hook fires a prompt at regular intervals asking what you were working on, with a
list of your assigned issues to pick from.

## 5. Everyday use

```sh
tt list                  # your assigned issues, straight from the cache
tt log 42 1h30m          # log time on issue #42
tt history               # what you tracked recently (including queued entries)
tt queue                 # writes that failed permanently, with retry/dismiss
```

## Config

The CLI config lives at the path shown by `tt config path`; get an annotated default
with `tt config template`. The daemon reads its own config — see
[gitlab-trackrd/README.md](gitlab-trackrd/README.md#configuration).

# Scripting and integrating

Script `tt` itself, or talk to the daemon's varlink socket directly — the interface
is documented in [the interface docs](gitlab-trackrd/docs/varlink_interface.md) and
available as the [`gitlab-trackr-api`](gitlab-trackr-api/README.md) Rust crate or the
[Go binding](clients/go/README.md).
