# Configuration

pulse-github reads one YAML file, `/etc/pulse-github/config.yaml` by default (`--config`, `-c`, or
`PULSE_GITHUB_CONFIG`). The schema is versioned (`version: 1`) and strict: unknown keys are errors.

```sh
pulse-github config validate                   # schema and cross-field rules
pulse-github config validate --check-secrets   # also read every referenced secret (values are never printed)
```

## Secrets

A secret is never written in the file. Every secret field takes a reference:

```yaml
private_key: {file: /run/secrets/github_app_key.pem}   # a mounted file
token: {env: GITHUB_PAT_OCTOCAT}                        # an environment variable
```

Literal values are rejected at start-up. Files are re-read where it matters: PAT files when their modification time
changes or after a 401.

## Environment overrides

These operational settings can be overridden without editing the file:

| Variable | Setting |
|---|---|
| `PULSE_GITHUB_CONFIG` | path of the configuration file |
| `PULSE_GITHUB_LOG_LEVEL` | `log.level` |
| `PULSE_GITHUB_LOG_FORMAT` | `log.format` |
| `PULSE_GITHUB_STORAGE_PATH` | `storage.path` |
| `PULSE_GITHUB_PULSE_URL` | `pulse.url` |
| `PULSE_GITHUB_PULSE_CREDS_FILE` | `pulse.creds_file` |
| `PULSE_GITHUB_PULSE_CA_FILE` | `pulse.ca_file` |
| `PULSE_GITHUB_WEBHOOK_LISTEN` | `webhook.listen` |
| `PULSE_GITHUB_ADMIN_LISTEN` | `admin.listen` |
| `PULSE_GITHUB_GITHUB_API_URL` | `github.api_url` |
| `PULSE_GITHUB_SHUTDOWN_TIMEOUT` | `service.shutdown_timeout` |

## Defaults worth knowing

- `webhook.enabled` and each account's `webhook.enabled`, `notifications.enabled`, `activity.enabled` and
  `reconcile.enabled` are **off** unless set. A section does nothing until you enable it.
- `admin.listen` defaults to `127.0.0.1:9090`. In a container, set `0.0.0.0:9090` to reach it from outside, and keep
  that port on a private network.
- An account's `key` (the subject token) defaults to its lowercased `login`. Set it explicitly if you want subjects to
  survive a GitHub rename unchanged.
- `storage.path` defaults to `/data/pulse-github.db`, which is the image's state volume.

## Accounts

Each entry in `accounts` is one monitored GitHub account (`type: user` or `type: organization`). Per account you
choose which collectors run:

| Block | Collects | Needs |
|---|---|---|
| `webhook` | App webhook deliveries for the account's installation | `webhook.enabled`, `github.app` |
| `notifications` | the user's notification threads (users only) | `token`: a classic PAT |
| `activity.feeds` | Events API feeds: `user_events`, `received_events`, `user_org_events`, `org_events`, `repo_events`, `audit_log` | `token`, or `credential: installation` for org and repository feeds |
| `reconcile` | periodic snapshots and backfill of `repository`, `issues`, `pulls`, `releases`, `workflow_runs` | `github.app` |
| `repositories` | `allow` / `deny` globs (`owner/name`) applied to everything routed to the account | |

`routing.shares` sends selected sources of one account to another as well, for example an organization's webhooks for
some repositories to a personal account. Routing never guesses from actor logins.

## Annotated example

[`deploy/config.example.yaml`](https://github.com/aurimasniekis/go-pulse-github/blob/main/deploy/config.example.yaml)
contains every setting with its default and a comment. It is parsed by the test suite, so it stays valid.

<<< ../../deploy/config.example.yaml
