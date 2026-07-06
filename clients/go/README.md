# Go binding for gitlab-trackrd

A Go client for the `org.thehoster.gitlab.trackrd` [varlink](https://varlink.org)
interface exposed by the [`gitlab-trackrd`](../../gitlab-trackrd/README.md) daemon.

The low-level types and call helpers are **generated** from the single
source-of-truth interface definition
([`gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink`](../../gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink))
using the official [`varlink/go`](https://github.com/varlink/go) generator, so the
binding never drifts from the wire contract. A small hand-written `Client` wraps
those helpers with socket discovery and one method per varlink method.

## Install

```sh
go get github.com/BjarneSeger/gitlab_trackr/clients/go
```

The generated package is named after the interface, so import it under an alias:

```go
import trackr "github.com/BjarneSeger/gitlab_trackr/clients/go"
```

## Usage

```go
ctx := context.Background()

c, err := trackr.Dial(ctx) // resolves the daemon socket like tt-cli does
if err != nil {
	log.Fatal(err)
}
defer c.Close()

issues, err := c.GetAssignedIssues(ctx, nil)
if err != nil {
	var notAuth *trackr.NotAuthenticated
	if errors.As(err, &notAuth) {
		log.Fatal("not authenticated; run: tt login --host gitlab.com")
	}
	log.Fatal(err)
}
for _, is := range issues {
	fmt.Printf("#%d %s\n", is.Iid, is.Title)
}
```

### Connecting

`trackr.Dial` resolves the daemon address with the same precedence as `tt`:

1. `$GITLAB_TRACKRD_SOCKET` (used verbatim — include the `unix:` scheme)
2. `unix:$XDG_RUNTIME_DIR/gitlab-trackrd.socket`
3. `unix:/tmp/gitlab-trackrd.socket`

Use `trackr.DialAddress(ctx, "unix:/path/to.socket")` to point elsewhere, or
`trackr.DefaultAddress()` to inspect what `Dial` would pick.

### Errors

Daemon-side errors surface as typed values you match with `errors.As`:

- `*trackr.GitlabError` — an upstream GitLab API error (`.Message`).
- `*trackr.NotAuthenticated` — no valid credentials; `.Reason` is one of the
  `trackr.Reason*` constants (e.g. `trackr.ReasonLoggedOut`), `.Detail` is optional.

### Optional parameters

Optional varlink parameters are pointers; pass `nil` to omit them
(e.g. `c.GetHistory(ctx, nil)` for the daemon's default window, or
`c.PostTime(ctx, pid, iid, "1h", &summary)`).

## Regenerating

The generated file `orgthehostergitlabtrackrd.go` is committed and marked
`DO NOT EDIT`. After changing the `.varlink` interface, regenerate it:

```sh
cd clients/go
go generate ./...
```

The generator is pinned via the `tool` directive in `go.mod`, and the
[`Go binding`](../../.github/workflows/go-binding.yml) CI workflow re-runs
`go generate` and fails if the committed file is out of date.
