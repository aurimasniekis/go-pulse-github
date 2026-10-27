# Operations

## Deployment shapes

| Shape | Needs | Coverage |
|---|---|---|
| **Webhook + polling** (recommended) | A GitHub-reachable HTTPS endpoint for the receiver, a GitHub App, PATs for personal accounts | Real-time App events, notifications, Events API, reconciliation |
| **Polling only** (`webhook.enabled: false`) | Outbound HTTPS only | Notifications, Events API (bounded, delayed), audit log, and reconciliation if the App is configured. No real-time repository events, and nothing GitHub only sends by webhook. |

"Headless" means there is no UI. It does not mean there is no inbound traffic: webhook mode needs an endpoint
GitHub can reach.

The process listens on two ports:

- `webhook.listen` (8080): only the configured path (`/webhook`), behind a TLS reverse proxy.
- `admin.listen` (9090): `/healthz`, `/readyz`, `/metrics`, and optionally `/debug/pprof` with a bearer token. Keep
  it on a private network.

### Reverse proxy for webhooks

- Terminate TLS with a public certificate and forward `POST /webhook` to `pulse-github:8080`. Pass the body
  unmodified: no re-encoding, no decompression rewrites, no body-size limit below 26 MiB.
- Keep proxy timeouts above 10 s (GitHub's own response window). pulse-github answers within milliseconds of the
  commit.
- Optionally restrict sources to GitHub's hook IP ranges (`GET https://api.github.com/meta`, key `hooks`).
  Signatures are verified regardless.

Example (Caddy): `hooks.example.com { reverse_proxy /webhook pulse-github:8080 }`.

## GitHub App

Create a GitHub App (user or organization settings → Developer settings → GitHub Apps):

- **Webhook URL:** `https://hooks.example.com/webhook`. **Webhook secret:** a random value of at least 32 bytes,
  stored in the file referenced by `webhook.secrets`.
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

- **Subscribe to events** for each permission above. `doctor` lists missing subscriptions.
- Install the App on every monitored account (all or selected repositories). Download a private key (PEM) and store
  it in the file referenced by `github.app.private_key`. Note the App ID and client ID.

## Personal access tokens

- **Notifications** need a **classic** PAT with `notifications` (or `repo`). GitHub supports no other token type on
  `/notifications`.
- **Private personal Events API feeds** need a PAT of that user. Classic `repo` shows private events; fine-grained
  tokens with "Events: read" work for the Events API but not for notifications.
- **The audit log** needs an organization owner's classic PAT with `read:audit_log` (Enterprise Cloud only).
- Give each account its own token file. pulse-github only reads (`GET`). It never marks notifications read or
  changes anything.

## Pulse client (least privilege)

```sh
pulse clients create pulse-github --grant 'publish:evt.github.>' --out pulse_github.creds
# only with pulse.blobs.enabled: add --grant blobs
```

Use a narrower grant (`publish:evt.github.<account>.>`) when one deployment serves a single account. Readers get
`subscribe:evt.github.>` or per-account filters. The private content in these events follows Pulse's access control
from then on.

## Configuration and commands

```sh
pulse-github config validate [--check-secrets]   # schema, cross-field rules, secret readability
pulse-github doctor [--test-publish]             # read-only checks of storage, Pulse, App, tokens, feeds
pulse-github run
pulse-github status                              # queues, checkpoints, installations, account identities
pulse-github backfill --account example-org --since 72h [--kinds issues,pulls] [--max-requests 500]
pulse-github quarantine list [--json]
pulse-github quarantine replay <id> | --all [--kind inbox|outbox]
pulse-github quarantine purge --older-than 720h --yes
```

`status`, `quarantine` and `backfill` can run next to the service (`docker compose exec pulse-github …`).

## Health, metrics, alerts

- `/readyz` returns 503 when storage cannot accept data (or during shutdown). Webhooks then get 503 as well. NATS
  outages do **not** affect readiness: data queues durably.
- Alert rules: `deploy/prometheus/alerts.yaml`. They cover publication lag, NATS disconnects, storage pressure,
  quarantine, authentication failures, degraded coverage, stale polling, sustained rate limiting, gaps, signature
  failures and SQLite errors.

## Troubleshooting

| Symptom | Check |
|---|---|
| Events not appearing in Pulse | `status` (pending outbox?), `pulse_github_nats_connected`, logs with `component=outbox` and their `error_class`. `publish_timeout` while connected usually means a missing `publish` grant: run `doctor --test-publish`. |
| `invalid_subject` in quarantine | Should not happen: subjects are validated before storage. Report it with the `quarantine list --json` output. |
| `payload_too_large` in quarantine | Enable `pulse.blobs` and add the `blobs` grant, then `quarantine replay`. |
| Webhooks answered 503 | `/readyz` body; free disk (`storage.min_free_bytes`), `max_db_bytes`, inbox depth (processor stuck? look for `installation_lookup` errors). |
| Webhooks answered 401 | Secret mismatch, or rotation not finished; `webhook_requests_total{result="rejected_signature"}`. |
| Deliveries `unrouted` | The installation's account is not configured, is not in `allowed_installation_ids`, or the repository filter excludes it. `status` lists installations. |
| `collector.access.lost` | Token revoked or expired, a scope missing, or an installation suspended. Run `doctor`. |
| Coverage gaps | Increase `max_pages`, shorten intervals, or accept the bound: the Events API keeps 300 events and 30 days. |
| Rate limiting | Lower poll frequency, `max_pages` and reconcile budgets, or split accounts across tokens. `github_rate_limit_remaining{credential}` shows each budget. |
| "another pulse-github process holds the database lock" | A second `run` on the same volume. Only one active process per database. |

## Outage recovery

- **pulse-github down:**
  - Polling sources resume from their checkpoints: notifications re-read from the stored `since` minus the overlap,
    and Events API feeds until the last seen ID (with a gap event if it fell out of the window).
  - Webhooks sent while it was down failed on GitHub's side. Redeliver them from the App's **Advanced → Recent
    Deliveries** (the last 3 days) or with `POST /app/hook/deliveries/{id}/attempts`. Redeliveries are deduplicated.
  - For longer outages, run `backfill` for the affected window to get current-state snapshots.
- **Pulse down:** nothing to do. Captured data waits in the outbox and drains after reconnect. Watch
  `outbox_pending` and free disk.
- **Disk full:** free space or grow the volume. Admission reopens within 5 s of the guard's next check. Redeliver
  webhooks that got 503.
- **Corrupted database:** stop the service and restore the last backup (below). Data captured after the backup but
  not yet published is lost. Published data is already in Pulse. Re-run `backfill` and redeliver recent webhooks.

## Backup, restore, migrations

- **Backup** while running: run `sqlite3 /data/pulse-github.db ".backup /backup/pulse-github.db"` (online,
  consistent) from the host or a sidecar that mounts the volume; the image itself has no shell or `sqlite3`. Or stop
  the container and copy `pulse-github.db*`. The database holds pending work, checkpoints and dedup state, but
  no credentials.
- **Restore:** stop the service, replace the files, start. Older checkpoints only cause re-reads, which dedup keys
  absorb.
- **Migrations** are embedded and applied at start, each in its own transaction (`schema_migrations` records them).
  A failed migration rolls back and the process exits. Downgrades are not supported: take a backup before upgrading.

## Upgrades

1. Back up the database.
2. Pull the new image (pin by digest) and restart. SIGTERM drains in-flight work (`stop_grace_period: 30s`).
3. Check `doctor`, `/readyz` and the logs.

The payload schema version only changes with a new `ce-type` version (`….v2`).

## Credential rotation

| Credential | Procedure |
|---|---|
| Webhook secret | Add the new secret file as a second entry in `webhook.secrets` and restart. Change the secret in the App settings, wait until deliveries verify against it, then remove the old entry and restart. |
| App private key | Generate a second key in the App settings, replace the referenced file, restart, then delete the old key on GitHub. |
| PAT | Overwrite the referenced file. It is re-read when its modification time changes or after a 401. No restart is needed. |
| Pulse creds | `pulse clients rotate pulse-github`, replace the file, restart. Pulse disconnects the old credentials within seconds, so publication pauses until the restart (data queues). |
| Installation tokens | Automatic (1 h lifetime, refreshed 5 min early). |

## Capacity planning

- **CPU and memory:** small. One process, a few goroutines per account; 64–256 MiB is typical. The largest
  allocations are webhook bodies (up to 25 MB each while being verified).
- **Disk:** roughly payload bytes × (1 inbox body + 1 outbox row per routed account), kept for 7 days, plus dedup
  keys for 35 days. A busy organization with 10k webhooks a day at 15 KB each is about 150 MB/day, so around 2 GB at
  the default retention. Size `storage.min_free_bytes` so that a Pulse outage of your tolerated length fits.
- **GitHub requests** per account per hour (defaults):
  - notifications: 60 polls, mostly 304s, which are free;
  - each activity feed: 60 polls, often 304s;
  - reconciliation: at most 300 requests every 6 h;
  - installation reconciliation: 1 + pages per installation per hour.

  All of this fits well within the 5,000/h primary limit per token.
- **Pulse:** one event per observation per routed account. Subjects stay low-cardinality (accounts × sources × event
  types × actions).
