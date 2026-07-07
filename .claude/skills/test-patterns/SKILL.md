---
name: test-patterns
description: The established mock and test conventions in this workspace (FakeGitlab styles, handler scaffolding, varlink call driving, timing rules) — read before writing or extending daemon tests so new tests reuse the existing helpers.
---

# Test patterns in gitlab-trackrd

All tests are inline `#[cfg(test)]` modules — no `tests/` dirs. Three suites have
established mocking styles; reuse their helpers instead of inventing new scaffolding.

## Handler tests (`src/handlers/tests.rs`) — the richest suite

**Mock**: `FakeGitlab` implements the `GitlabApi` trait.
- Call counts in `AtomicUsize` fields (e.g. `board_calls`), read via accessor methods.
- Canned responses in `Mutex<HashMap<..>>`, consumed with `.remove()` — each canned
  entry answers exactly one fetch.
- Error injection via `enum FetchErr { Transient, Permanent }`; builders
  `with_board_labels`, `with_board_error`, `failing`, `failing_write`.
- Trait methods a test never hits: `unimplemented!()`.

**Scaffolding**: `handlers_with(state: ConnState) -> (Handlers, TempDir)` opens a real
fjall DB in a `tempfile::tempdir()` and builds all caches + `RetryQueue` on it, config
from `config::defaults()`. Keep the `TempDir` alive for the test's duration.
Wrappers: `dormant_handlers()` (NoCredentials) and `connected_handlers(fake)`.

**Data builders**: `issue(..)`, `iwl(..)` (IssueWithLabels), `stored(..)`
(StoredTimelog), `seed_grouped_cache(&h)`, `reply_issues(call)` to parse a reply.

**Driving a varlink method**:

```rust
use gitlab_trackr_api::AsyncCall;
let mut call = AsyncCall::default();
h.post_time(&mut call as &mut dyn Call_PostTime, 1, 2, "1h".into(), None).await?;
let reply = call.take_reply();          // assert on .error / .parameters
```

`NotAuthenticated` is asserted via the full error string
`"org.thehoster.gitlab.trackrd.NotAuthenticated"`.

**Reconnect-signal assertions**: demotion woke the supervisor →
`tokio::time::timeout(Duration::from_millis(200), h.reconnect_signal.notified()).await`
is `Ok`; "did not fire" → assert `.is_err()`.

## Queue tests (`src/queue.rs`)

Own `FakeGitlab`: per-method `Mutex<VecDeque<Result<()>>>` response queues with
`push_*` helpers, plus `AtomicUsize` counters; unlisted methods panic. DB helpers
`test_db` / `store` on a tempdir.

## Reconnect tests (`src/reconnect.rs`)

No GitLab mock behavior at all — `NoopGitlab` is all `unimplemented!()`. The connect
attempt is injected as a closure into `reconnect_loop(session, config, || async { .. })`
returning `Attempt::*`. Timing is defeated with `instant_config()` (backoff delays set
to 0), not a mocked clock. Helpers: `unreachable_slot()`, `connected_session()`.

## Timing rules

- **No `tokio::time::pause()` anywhere.** Backoff code uses `SystemTime::now()`, which
  a paused tokio clock does not affect. Tests use short real sleeps and
  `tokio::time::timeout` with small budgets (≤ a few hundred ms).
- Backoff/lifetime schedules are deliberately not unit-tested for the same reason —
  see the comment near the top of the queue test module (`queue.rs:568`).
- Prefer defeating delays via config (`instant_config` pattern) over sleeping through
  them.
