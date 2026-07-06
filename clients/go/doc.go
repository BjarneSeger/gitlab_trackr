// Package orgthehostergitlabtrackrd is a Go client for the
// org.thehoster.gitlab.trackrd varlink interface exposed by the gitlab-trackrd
// daemon over a Unix socket.
//
// The wire types and low-level call helpers (orgthehostergitlabtrackrd.go) are
// generated from the interface definition shared with the Rust crates; the
// Client type (client.go) is a thin, hand-written convenience layer that resolves
// the daemon socket and exposes one Go method per varlink method.
//
// Because the generated package name is derived from the interface name, callers
// usually import it under a shorter alias:
//
//	import trackr "github.com/BjarneSeger/gitlab_trackr/clients/go"
//
//	ctx := context.Background()
//	c, err := trackr.Dial(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer c.Close()
//
//	issues, err := c.GetAssignedIssues(ctx, nil)
//	if err != nil {
//		var notAuth *trackr.NotAuthenticated
//		if errors.As(err, &notAuth) {
//			log.Fatal("log in first: tt login --host gitlab.com")
//		}
//		log.Fatal(err)
//	}
//	for _, is := range issues {
//		fmt.Println(is.Iid, is.Title)
//	}
//
// Errors returned by the daemon surface as *GitlabError or *NotAuthenticated;
// match them with errors.As. Optional parameters are pointers, where nil omits
// the field on the wire.
package orgthehostergitlabtrackrd
