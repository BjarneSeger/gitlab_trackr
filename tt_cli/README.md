# tt_cli
A cli helper for logging times in gitlab

tt allows native-ish integration of timetracking into you workflow by regularly
asking you what you worked on, in the terminal.

# Setup

## Installation
Prebuilt binaries are available in the releases and packages are built for debian,
rpm and arch.

> Note that you will need to install [gitlab_trackrd](../gitlab_trackrd/README.md)
> for this tool to work, as it provides the backend that actually talks to gitlab.

After installing `gitlab_trackrd` and `tt`, you can run

```sh
tt hook <SHELL>
```

to get the snippet to add to your respective shellrc. After that, you will be asked
after 30 minutes when the next cli prompt should appear what you are working on, 
with a list of assigned issues.

### Completions
Out of the box, completions are installed for fish, zsh and bash. Completions are
also provided for carapace, but they need to be manually linked from
`/usr/share/carapace/specs/tt.yaml` to `~/.config/carapace/specs/tt.yaml` manually,
as carapace does not currently support globally installed specs.

## Config
The config lives at `$XDG_CONFIG_HOME/` or `$HOME/.config/` under
`gitlab_trackr_cli/config.toml`. You can run `tt config path` to see what it 
resolves to on your system. To get a sample config, run

```sh
tt config template
# Save the default config
# tt config template > $(tt config path)
```
