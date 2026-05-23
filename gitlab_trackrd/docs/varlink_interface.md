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
served from the redb cache (`~/.local/share/gitlab_trackrd/cache.redb`) for
up to `GITLAB_TRACKRD_CACHE_TTL` seconds before a live GitLab request is made.

## `PostTime(project_id: int, issue_iid: int, duration: string) -> ()`

Records spent time on an issue.  `duration` uses GitLab's time-tracking
syntax: `"1h30m"`, `"45m"`, `"2h"`, etc.

# Error

Both methods may return a `GitlabError(message: string)` varlink error when
the GitLab API call fails (network error, authentication failure, rate limit,
etc.).

# Accessing with varlink CLI

```sh
# list assigned issues
varlinkctl call unix:$XDG_RUNTIME_DIR/gitlab_trackrd.socket org.thehoster.gitlab.trackrd.GetAssignedIssues {}

# post 1h30m to project 42, issue #7
varlinkctl call unix:$XDG_RUNTIME_DIR/gitlab_trackrd.socket org.thehoster.gitlab.trackrd.PostTime \
  '{"project_id": 42, "issue_iid": 7, "duration": "1h30m"}'
```
