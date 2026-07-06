package orgthehostergitlabtrackrd_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	trackr "github.com/BjarneSeger/gitlab_trackr/clients/go"
)

// This example connects to a running gitlab-trackrd daemon and lists the issues
// assigned to the authenticated user. It has no // Output: comment, so `go test`
// compiles it (guarding the public API) without executing it.
func Example() {
	ctx := context.Background()

	c, err := trackr.Dial(ctx)
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
}
