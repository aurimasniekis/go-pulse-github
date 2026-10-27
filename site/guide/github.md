# GitHub App and tokens

pulse-github needs up to three kinds of credentials. Which ones depends on what you want to collect:

| You want | Credential | Configured in |
|---|---|---|
| Real-time repository and organization activity (webhooks), installation reconciliation, snapshots and backfill | a **GitHub App** installed on the account | `github.app`, `webhook.secrets` |
| A user's notifications | a **classic PAT** of that user with `notifications` (or `repo`) | `accounts[].token` |
| A user's Events API feeds, including private events | a PAT of that user | `accounts[].token` |
| Organization or repository Events API feeds | the App installation (`credential: installation`) or a PAT | `accounts[].activity.feeds` |
| The organization audit log (Enterprise Cloud only) | an organization owner's classic PAT with `read:audit_log` | `accounts[].token` |
| Publishing to Pulse | a Pulse client with `publish:evt.github.>` | `pulse.creds_file` |

All of them are referenced as files (`{file: /run/secrets/…}`) or environment variables (`{env: NAME}`). Literal
secrets in the configuration are rejected at start-up.

## GitHub App

Create a GitHub App in your user or organization settings → **Developer settings → GitHub Apps**:

- **Webhook URL:** `https://hooks.example.com/webhook`, a public HTTPS endpoint in front of pulse-github's webhook
  listener ([reverse proxy](/operators/#reverse-proxy-for-webhooks)).
- **Webhook secret:** a random value of at least 32 bytes (`openssl rand -hex 32`), stored in the file referenced by
  `webhook.secrets`.
- **Permissions:** read-only is enough. Grant only what you want to collect:

  | Permission (read) | Enables |
  |---|---|
  | Repository: Metadata | mandatory; `star`, `watch`, `repository` |
  | Repository: Contents | `push`, `create`, `delete`, `release`, `fork`, `commit_comment` |
  | Repository: Issues | `issues`, `issue_comment`, `label`, `milestone`; issue snapshots |
  | Repository: Pull requests | `pull_request*`, review events; PR snapshots |
  | Repository: Discussions | `discussion`, `discussion_comment` |
  | Repository: Actions | `workflow_run`, `workflow_job`; workflow-run snapshots |
  | Repository: Checks | `check_run`, `check_suite` |
  | Repository: Deployments | `deployment`, `deployment_status` |
  | Repository: Administration | `member` (collaborator) events |
  | Organization: Members | `membership`, `team`, `organization` |

- **Subscribe to events** for each permission above. `pulse-github doctor` lists missing subscriptions.
- **Install** the App on every monitored account, with all or selected repositories. A personal-account installation
  covers that user's repositories, not repositories the user only contributes to.
- **Private key:** generate one (PEM) and store it in the file referenced by `github.app.private_key`. Note the
  **App ID** and the **client ID** (the recommended JWT issuer) for `github.app.app_id` and `github.app.client_id`.

The full list of webhook events, and what each permission unlocks, is on the [coverage](/reference/coverage#github-app-webhooks-primary)
page.

## Personal access tokens

- **Notifications** need a **classic** PAT with `notifications` (or `repo`). GitHub supports no other token type on
  `/notifications`: fine-grained PATs, installation tokens and App user tokens do not work.
- **Private personal Events API feeds** need a PAT of that user. Classic `repo` shows private events; fine-grained
  tokens with "Events: read" work for the Events API but not for notifications.
- **The audit log** needs an organization owner's classic PAT with `read:audit_log` (Enterprise Cloud only).
- Give each account its own token file. pulse-github checks `GET /user` and refuses a token that belongs to a
  different account. It only reads (`GET`): it never marks notifications read or changes anything.
- PAT files are re-read when they change on disk or after a 401, so rotation needs no restart.

## Pulse client (least privilege)

On the machine where you administer Pulse:

```sh
pulse clients create pulse-github --grant 'publish:evt.github.>' --out pulse_github.creds
# only with pulse.blobs.enabled: add --grant blobs
```

Use a narrower grant (`publish:evt.github.<account>.>`) when one deployment serves a single account. pulse-github never
creates or changes streams, users or accounts; it is an ordinary Pulse client.

Readers get `subscribe:evt.github.>` or per-account filters. The events contain private GitHub content (issue bodies,
comments, private repository names), so grant subscriptions only to tools that may read it. See
[Consuming events](/guide/consuming).
