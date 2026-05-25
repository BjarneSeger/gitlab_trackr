# Various gitlab timetracking helpers

Ever wanted / had to use gitlabs timetracking, but never quite managed to integrate
it into you workflow? Then this is the repo for you! We have:

- [A background daemon that handles auth and caching](gitlab_trackrd/README.md)
- [A cli to communicate with it and to remind you to track](tt_cli/README.md)

# Setup
Install the `gitlab_trackr-utils` package (see the releases tab), which provides
gitlab_trackrd as an abstraction over the gitlab api with caching. Now you can go
and use the `tt` command, which is part of `gitlab-trackr-utils`, or script the api
(or `tt` itself) or build your own application on top of `gitlab_trackr_api`, which
provides the varlink definition.
