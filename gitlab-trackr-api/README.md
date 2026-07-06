# gitlab_trackr_api
Basic api definitions for communicating with gitlab-trackrd.

See [varlink](./varlink/) for the definitions, [tt-cli](../tt-cli/README.md) for an
example user or [gitlab-trackrd](../gitlab-trackrd/README.md) for more information.

The [Go binding](../clients/go/README.md) is generated from the same `.varlink`
definition, so Go consumers stay in lock-step with the wire contract.
