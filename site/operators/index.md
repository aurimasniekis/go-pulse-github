# Running in production

This page covers how pulse-github is deployed and kept healthy. Setting up GitHub and Pulse credentials is in
[GitHub App and tokens](/guide/github); the first run is in the [quick start](/guide/quick-start).

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

## The container

The image (`ghcr.io/aurimasniekis/pulse-github`, mirrored to `docker.io/aurimasniekis/pulse-github`, linux/amd64 and
linux/arm64) is distroless with no shell. It runs as uid/gid `65532`, works on a read-only root filesystem and keeps
all state in `/data`:

| Path | Contents |
|---|---|
| `/usr/local/bin/pulse-github` | the binary (`ENTRYPOINT`, default command `run`) |
| `/etc/pulse-github/config.yaml` | configuration, mounted read-only (`PULSE_GITHUB_CONFIG`) |
| `/run/secrets/*` | credentials, mounted read-only, never baked into the image |
| `/data` | state volume: SQLite database, WAL and lock file. Use a local volume, not NFS. |

The image has a `HEALTHCHECK` (`pulse-github healthcheck` probes `/readyz`). Give the container a stop grace period
longer than `service.shutdown_timeout` (25 s by default) so SIGTERM can drain in-flight work.

The reference Compose file, [`deploy/compose.yaml`](https://github.com/aurimasniekis/go-pulse-github/blob/main/deploy/compose.yaml):

<<< ../../deploy/compose.yaml

## Health, metrics, alerts

- `/readyz` returns 503 when storage cannot accept data (or during shutdown). Webhooks then get 503 as well. NATS
  outages do **not** affect readiness: data queues durably.
- Alert rules: [`deploy/prometheus/alerts.yaml`](/reference/metrics#alerts). They cover publication lag, NATS disconnects, storage pressure,
  quarantine, authentication failures, degraded coverage, stale polling, sustained rate limiting, gaps, signature
  failures and SQLite errors.

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
