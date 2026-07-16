# The `org.thehoster.gitlab.trackrd` interface

Machine-readable definition: [`gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink`](../../gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink)
— that file is the source of truth; this document explains the behavior behind it.

**Caching model**: the daemon has no TTL. Background sync owns freshness — a quick
tier refreshes issues, boards, and the recent timelog window every few minutes, a slow
tier re-polls the bulk history daily, and the search corpus (issues, merge requests,
projects, groups) syncs incrementally via `updated_after` deltas at most every
`search.partial_interval_secs` (default 30 min) with a full resync — which also
reconciles deletions — every `search.full_interval_secs` (default weekly). Read
methods serve whatever was last synced from the local store
(`$XDG_DATA_HOME/gitlab-trackrd/db/`). Reads never trigger a GitLab round-trip.

**Write model**: mutating methods reply success even when GitLab is unreachable — the
operation is persisted to a retry queue and drained on reconnect (exponential backoff,
dead-lettered after the retry window; see `GetFailures`). Only an actual GitLab
*rejection* surfaces as `GitlabError`.

# Types

```varlink
type Issue (
  id:           int,    # global issue ID (unique across the GitLab instance)
  iid:          int,    # per-project issue number (the "#42" shown in the UI)
  project_id:   int,
  title:        string,
  web_url:      string,
  state:        string, # "opened" | "closed"
  parent:       string, # URL of the issue's epic; empty when it has none
  total_time:   string, # GitLab's human-readable total spent time ("2h"); empty when none
  graph_status: string  # board column the issue sits in, derived from its labels
                        # matched against the project's issue board; empty when no
                        # board/label matches
)
```

```varlink
type HistoryEvent (
  timestamp:   int,    # unix seconds — spent_at for synced entries, enqueue time for queued ones
  source:      string, # "gitlab" (synced timelog) | "queued" (pending PostTime in the retry queue)
  project_id:  int,
  issue_iid:   int,
  issue_title: string, # empty on queued events whose issue is not in the cache
  web_url:     string,
  duration:    string,
  summary:     string
)
```

```varlink
type FailedTask (
  id:         int,    # handle for RetryFailure / DismissFailure
  op:         string, # which write failed (e.g. "PostTime", "CloseIssue")
  project_id: int,
  issue_iid:  int,
  detail:     string, # operation-specific summary (e.g. the duration)
  error:      string, # the GitLab error that dead-lettered it
  queued_at:  int,    # unix seconds
  failed_at:  int     # unix seconds
)
```

```varlink
type MergeRequest (
  id:         int,    # global MR ID (unique across the GitLab instance)
  iid:        int,    # per-project MR number (the "!7" shown in the UI)
  project_id: int,
  title:      string,
  web_url:    string,
  state:      string  # "opened" | "closed" | "merged" | "locked"
)
```

```varlink
type Project (
  id:      int,
  name:    string,
  path:    string,  # full namespace path ("team/backend/api")
  web_url: string
)
```

```varlink
type Group (
  id:      int,
  name:    string,
  path:    string,  # full group path ("team/backend")
  web_url: string
)
```

# Errors

`GitlabError (message: string)` — GitLab rejected the request (invalid input, API
error, rate limit), or a local precondition failed (malformed issue reference, invalid
duration, unknown failure id). `message` is human-readable.

`NotAuthenticated (reason: ?NotAuthReason, detail: ?string)` — the daemon has no live
GitLab session (it is *dormant*). `reason` says why; `detail` carries free text (host,
underlying error) for the reasons that have one. Both fields are optional so older
daemons that send neither stay compatible — clients fall back to a generic
"run `tt login`" message.

```varlink
type NotAuthReason (no_credentials, keychain_error, unreachable, token_rejected, logged_out)
```

| reason           | meaning                                                            |
|------------------|--------------------------------------------------------------------|
| `no_credentials` | no credentials stored (never logged in)                            |
| `keychain_error` | reading the OS keychain failed (`detail` = the error)              |
| `unreachable`    | credentials exist but GitLab could not be reached (`detail` set)   |
| `token_rejected` | credentials exist but GitLab rejected the token (`detail` set)     |
| `logged_out`     | the user explicitly logged out this session                        |

The daemon auto-recovers from `unreachable` in the background (unless disabled via
`[reconnect]` config); the other reasons need the user.

# Methods

## Reading

### `GetAssignedIssues(groups: ?[]string) -> (issues: []Issue)`

Open issues assigned to the authenticated user, served purely from the cache.
`groups` filters to the given group namespaces (parsed from each issue's `web_url`);
issues appearing under several requested groups are deduplicated. Omitted or empty
`groups` returns everything. When the cache has never been populated: replies with an
empty list if a session exists (first sync pending), `NotAuthenticated` otherwise.

### `Search(query: string, kinds: ?[]string, limit: ?int) -> (issues: []Issue, merge_requests: []MergeRequest, projects: []Project, groups: []Group)`

Searches the locally cached corpus — a pure cache read, no GitLab round-trip.
Matching is a case-insensitive substring test on issue/MR titles and labels and on
project/group names and paths; a query of the exact form `#123` additionally matches
issues and MRs by their per-project number. Descriptions are not cached and not
searched.

`kinds` restricts the reply to a subset of `issues`, `merge_requests`, `projects`,
`groups` (omitted or empty = all four; an unknown kind is an eager `GitlabError`).
`limit` caps each returned array separately (default 50; must be positive). Issues
and MRs come newest-updated first, projects and groups sorted by path. An empty or
whitespace-only `query` is an eager `GitlabError`.

What the corpus contains depends on the `[search]` daemon config: issues and MRs
from everything the token can see (`population = "all"`) or only from member
projects (`"member"`); the default `"auto"` resolves to `"member"` on gitlab.com
(which rejects the global fetch) and `"all"` elsewhere, falling back to `"member"`
until the next full resync if the instance rejects the global fetch too. Projects
and groups are always membership-scoped.
Issue `graph_status` is filled best-effort from already-cached board labels and is
empty for projects the board cache has never seen. When the cache has never been
synced: replies with empty arrays if a session exists (first sync pending),
`NotAuthenticated` otherwise.

### `GetHistory(days: ?int) -> (events: []HistoryEvent)`

Time-tracking events from the last `days` days (default 7). Merges two sources,
distinguished by `source`: `"gitlab"` — timelogs synced from GitLab; `"queued"` —
`PostTime` operations still waiting in the retry queue (so freshly logged time shows
up even while GitLab is unreachable). Served from local state; never errors on cache
trouble (degrades to whatever is readable).

### `WhoAmI() -> (host: string, user_id: int)`

The connected GitLab host and the authenticated user's ID, answered from the session
without a round-trip. `NotAuthenticated` when dormant.

## Writing (queued when GitLab is away)

All four validate the issue reference eagerly (`project_id`/`issue_iid` must be
positive) and reply `GitlabError` on a malformed one without attempting or queuing
anything. On an unreachable session or a transient network failure the operation is
queued for retry and the call **replies success**; a GitLab rejection replies
`GitlabError`. Other dormancy reasons reply `NotAuthenticated`.

### `PostTime(project_id: int, issue_iid: int, duration: string, summary: ?string) -> ()`

Records spent time. `duration` uses GitLab's time-tracking syntax (`"1h30m"`, `"45m"`,
`"2d"`); an obviously malformed duration is rejected up front. `summary` becomes the
timelog note.

### `CloseIssue(project_id: int, issue_iid: int) -> ()`

Closes the issue and immediately drops it from the assigned-issues cache so list reads
reflect the close before the next sync.

### `AssignSelf(project_id: int, issue_iid: int) -> ()`

Assigns the authenticated user to the issue. The issue appears in
`GetAssignedIssues` after the next sync.

### `UnassignSelf(project_id: int, issue_iid: int) -> ()`

Removes the authenticated user from the issue's assignees and immediately drops it
from the assigned-issues cache.

## Retry-queue failures (dead letters)

Writes that exhausted their retry window or were rejected while draining land in a
persistent dead-letter store.

### `GetFailures() -> (failures: []FailedTask)`

Lists dead-lettered tasks. Never errors; storage trouble degrades to an empty list.

### `RetryFailure(id: int) -> ()`

Moves a dead-lettered task back into the retry queue. `GitlabError` when `id` is
unknown.

### `DismissFailure(id: int) -> ()`

Deletes one dead-lettered task. `GitlabError` when `id` is unknown.

### `ClearFailures() -> ()`

Deletes all dead-lettered tasks.

## Cache control

### `ClearCache(scope: ?[]string) -> ()`

Clears cached state and re-fetches it when a session exists. Omitted or empty `scope`
clears everything (issues, boards, full history) and runs the full warm-up. Otherwise
each scope string selects a slice:

| scope    | clears                                              | re-fetches            |
|----------|-----------------------------------------------------|-----------------------|
| `issues` | assigned-issues and board caches                    | (next quick refresh)  |
| `search` | search corpus (issues, MRs, projects, groups) and its sync stamps | full search resync |
| `quick`  | history inside the quick window (last `refresh.quick.window_hours`) | that window |
| `slow`   | history between the quick and slow windows          | the slow window       |
| `stale`  | history older than the slow window                  | the full retention window, then prunes |

Replies success even when dormant — the cleared state then simply stays empty until
the next successful sync.

## Session

### `Login(host: string, token: string) -> ()`

Connects to `host` with the personal access token, stores the credentials in the OS
keychain, and flips the daemon to connected (waking the retry-queue drain).
`GitlabError` when GitLab rejects the token or the keychain write fails. Prefer
`tt login`, which walks through creating a PAT with the right scopes.

### `Logout() -> ()`

Drops the session (subsequent calls reply `NotAuthenticated` with reason
`logged_out`) and deletes the stored credentials from the keychain.

# Calling from the shell

```sh
SOCKET=unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket

# list assigned issues
varlinkctl call $SOCKET org.thehoster.gitlab.trackrd.GetAssignedIssues '{}'

# post 1h30m to project 42, issue #7
varlinkctl call $SOCKET org.thehoster.gitlab.trackrd.PostTime \
  '{"project_id": 42, "issue_iid": 7, "duration": "1h30m", "summary": "code review"}'

# introspect the live interface
varlinkctl introspect $SOCKET org.thehoster.gitlab.trackrd
```
