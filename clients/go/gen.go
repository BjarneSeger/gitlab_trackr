// Code generation for the varlink binding. See doc.go for the package overview.
//
// orgthehostergitlabtrackrd.go is generated from the single source-of-truth
// interface definition at
// gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink. Do not edit it
// by hand; run `go generate ./...` after changing the .varlink interface.
//
// The generator writes its output next to its input file and names it after the
// interface, so we copy the .varlink into this module, generate, then remove the
// copy — keeping every write inside clients/go and leaving the api crate untouched.

//go:generate cp ../../gitlab-trackr-api/varlink/org.thehoster.gitlab.trackrd.varlink ./interface.varlink
//go:generate go tool varlink-go-interface-generator ./interface.varlink
//go:generate rm ./interface.varlink

package orgthehostergitlabtrackrd
