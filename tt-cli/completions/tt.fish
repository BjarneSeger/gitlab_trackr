# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_tt_global_optspecs
	string join \n h/help V/version
end

function __fish_tt_needs_command
	# Figure out if the current invocation already has a command.
	set -l cmd (commandline -opc)
	set -e cmd[1]
	argparse -s (__fish_tt_global_optspecs) -- $cmd 2>/dev/null
	or return
	if set -q argv[1]
		# Also print the command, so this can be used to figure out what it is.
		echo $argv[1]
		return 1
	end
	return 0
end

function __fish_tt_using_subcommand
	set -l cmd (__fish_tt_needs_command)
	test -z "$cmd"
	and return 1
	contains -- $cmd[1] $argv
end

complete -c tt -n "__fish_tt_needs_command" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_needs_command" -s V -l version -d 'Print version'
complete -c tt -n "__fish_tt_needs_command" -f -a "list" -d 'List issues assigned to you'
complete -c tt -n "__fish_tt_needs_command" -f -a "log" -d 'Log time on an issue non-interactively'
complete -c tt -n "__fish_tt_needs_command" -f -a "prompt" -d 'Interactively pick an issue and log time'
complete -c tt -n "__fish_tt_needs_command" -f -a "tick" -d 'Hook entry: if enough time has elapsed, run the interactive prompt; otherwise exit silently'
complete -c tt -n "__fish_tt_needs_command" -f -a "hook" -d 'Print a shell snippet that wires `tt tick` into the pre-prompt hook'
complete -c tt -n "__fish_tt_needs_command" -f -a "refresh" -d 'Tell the daemon to drop its cached issue list'
complete -c tt -n "__fish_tt_needs_command" -f -a "config" -d 'Inspect or scaffold the user configuration file'
complete -c tt -n "__fish_tt_needs_command" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c tt -n "__fish_tt_using_subcommand list" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand log" -s p -l project-id -d 'Project ID. If omitted, resolved from the last-used issue cache or by scanning your assigned issues for one matching `iid`' -r
complete -c tt -n "__fish_tt_using_subcommand log" -s s -l summary -d 'Optional summary note' -r
complete -c tt -n "__fish_tt_using_subcommand log" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand prompt" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand tick" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand hook" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand refresh" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand config; and not __fish_seen_subcommand_from template path help" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand config; and not __fish_seen_subcommand_from template path help" -f -a "template" -d 'Print an annotated TOML template (with current defaults and doc comments) to stdout. Pipe into `$XDG_CONFIG_HOME/gitlab_trackr_cli/config.toml`'
complete -c tt -n "__fish_tt_using_subcommand config; and not __fish_seen_subcommand_from template path help" -f -a "path" -d 'Print the resolved path to the user config file'
complete -c tt -n "__fish_tt_using_subcommand config; and not __fish_seen_subcommand_from template path help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c tt -n "__fish_tt_using_subcommand config; and __fish_seen_subcommand_from template" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand config; and __fish_seen_subcommand_from path" -s h -l help -d 'Print help'
complete -c tt -n "__fish_tt_using_subcommand config; and __fish_seen_subcommand_from help" -f -a "template" -d 'Print an annotated TOML template (with current defaults and doc comments) to stdout. Pipe into `$XDG_CONFIG_HOME/gitlab_trackr_cli/config.toml`'
complete -c tt -n "__fish_tt_using_subcommand config; and __fish_seen_subcommand_from help" -f -a "path" -d 'Print the resolved path to the user config file'
complete -c tt -n "__fish_tt_using_subcommand config; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "list" -d 'List issues assigned to you'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "log" -d 'Log time on an issue non-interactively'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "prompt" -d 'Interactively pick an issue and log time'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "tick" -d 'Hook entry: if enough time has elapsed, run the interactive prompt; otherwise exit silently'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "hook" -d 'Print a shell snippet that wires `tt tick` into the pre-prompt hook'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "refresh" -d 'Tell the daemon to drop its cached issue list'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "config" -d 'Inspect or scaffold the user configuration file'
complete -c tt -n "__fish_tt_using_subcommand help; and not __fish_seen_subcommand_from list log prompt tick hook refresh config help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c tt -n "__fish_tt_using_subcommand help; and __fish_seen_subcommand_from config" -f -a "template" -d 'Print an annotated TOML template (with current defaults and doc comments) to stdout. Pipe into `$XDG_CONFIG_HOME/gitlab_trackr_cli/config.toml`'
complete -c tt -n "__fish_tt_using_subcommand help; and __fish_seen_subcommand_from config" -f -a "path" -d 'Print the resolved path to the user config file'
