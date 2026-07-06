# Various gitlab timetracking helpers

Ever wanted / had to use gitlabs timetracking, but never quite managed to integrate
it into you workflow? Then this is the repo for you! We have:

- [A background daemon that handles auth and caching](gitlab-trackrd/README.md)
- [A cli to communicate with it and to remind you to track](tt-cli/README.md)
- [A ready-to-import Go binding for the daemon's varlink interface](clients/go/README.md)

# Setup
Install the `gitlab-trackr-utils` package (see the releases tab), which provides
gitlab-trackrd as an abstraction over the gitlab api with caching. Now you can go
and use the `tt` command, which is part of `gitlab-trackr-utils`, or script the api
(or `tt` itself) or build your own application on top of `gitlab_trackr_api`, which
provides the varlink definition.

## Setting up the daemon
To use the daemon, you need to start it first:

```sh
systemctl enable --now gitlab-trackrd.service
```

Then, you will need to authenticate. For this, you can use `tt` like this:

```sh
tt login --host gitlab.com
```

This will take you to creating a PAT for gitlab-trackr with the appropriate scopes.
Paste it back to the prompt and now you are logged in, with the token stored
securely in your platforms keystore (keyring on Linux, keychain on macOS).

## Setting up the cli
To get the regular tick for reminding you to track your time, use:

```sh
tt hook YOUR_SHELL >> YOUR_SHELL_RC
```

which fires a prompt asking you to what you were working on, with a list of assigned
issues at regular intervals.

### Config
The config file will by default be located at

```sh
tt config path
```

And you can get a default one by running

```sh
tt config template
```
