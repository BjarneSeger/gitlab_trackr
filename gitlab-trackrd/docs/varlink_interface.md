# Types

```varlink
type Issue (
  id:         int,    # global issue ID
  iid:        int,    # per-project issue number (shown in the GitLab UI)
  project_id: int,
  title:      string,
  web_url:    string,
  state:      string  # "opened" | "closed"
)
```

# Methods

## `GetAssignedIssues() -> (issues: []Issue)`

Returns all open issues assigned to the authenticated user.  Results are
served from the local cache (`~/.local/share/gitlab-trackrd/db/`) for
up to `GITLAB_TRACKRD_CACHE_TTL` seconds before a live GitLab request is made.

## `PostTime(project_id: int, issue_iid: int, duration: string) -> ()`

Records spent time on an issue.  `duration` uses GitLab's time-tracking
syntax: `"1h30m"`, `"45m"`, `"2h"`, etc.

# Errors

`GitlabError(message: string)` — the GitLab API call failed (network error,
API-level authentication failure, rate limit, invalid input, etc.). `message`
is a human-readable description.

`NotAuthenticated(reason: ?string, detail: ?string)` — the daemon has no live
GitLab session (it is *dormant*). Returned by every GitLab-touching method.
`reason` is a stable machine code the client maps to a user-facing message;
`detail` is optional free text (host + underlying error) for the codes that
carry one. Both fields are optional, so an older daemon that sends neither
stays compatible — clients fall back to a generic "run `tt login`" message.

| `reason` code    | meaning                                                     |
|------------------|-------------------------------------------------------------|
| `no-credentials` | no credentials stored (never logged in / logged out)        |
| `keychain-error` | reading the OS keychain failed (`detail` = the error)       |
| `unreachable`    | credentials exist but GitLab was unreachable (`detail` set) |
| `token-rejected` | credentials exist but GitLab rejected the token (`detail`)  |
| `logged-out`     | the user explicitly logged out this session                 |

# Accessing with varlink CLI

```sh
# list assigned issues
varlinkctl call unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket org.thehoster.gitlab.trackrd.GetAssignedIssues {}

# post 1h30m to project 42, issue #7
varlinkctl call unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket org.thehoster.gitlab.trackrd.PostTime \
  '{"project_id": 42, "issue_iid": 7, "duration": "1h30m"}'
```
