# Various gitlab timetracking helpers

Ever wanted / had to use gitlabs timetracking, but never quite managed to integrate
it into you workflow? Then this is the repo for you! We have:

- [A background daemon that handles auth and caching](gitlab_trackrd/README.md)
- [A cli to communicate with it and to remind you to track](tt_cli/README.md)

# Setup
Install `gitlab_trackrd`, which provides an abstraction over the gitlab api with
caching. Now you can go and use `tt`, or script the api (or `tt` itself) or build
your own application on top of `gitlab_trackr_api`, which provides the varlink
definition.
