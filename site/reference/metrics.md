# Metrics and alerts

The admin listener (`admin.listen`, default `127.0.0.1:9090`) serves:

| Path | Purpose |
|---|---|
| `/healthz` | 200 while the process runs. |
| `/readyz` | 200 while webhook deliveries can be durably accepted; 503 when storage is broken, full or over its limits, or during shutdown. NATS state is reported in the body but does not affect readiness: data queues durably. |
| `/metrics` | Prometheus metrics, plus the Go and process collectors. |
| `/debug/pprof/` | Only with `admin.pprof.enabled` and a bearer token (`admin.pprof.token`). |

Keep this listener on a private network.

## Metrics

All metrics are prefixed `pulse_github_`. Labels are bounded: `account` only takes configured account keys, event
categories map unknown GitHub events to `other`, and `credential` is `app`, `token:<account>` or
`installation:<account>`. Delivery IDs, URLs, repositories and actors never appear as labels.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `webhook_requests_total` | counter | `result` | Webhook HTTP requests by result (accepted, duplicate, `rejected_signature`, …). |
| `webhook_admission_open` | gauge | | 1 while webhook deliveries can be durably accepted. |
| `inbox_pending` | gauge | | Webhook deliveries stored but not yet normalized. |
| `inbox_oldest_pending_seconds` | gauge | | Age of the oldest pending webhook delivery. |
| `inbox_unrouted` | gauge | | Webhook deliveries that matched no configured account. |
| `observations_total` | counter | `account`, `source`, `category` | New observations stored. |
| `observation_duplicates_total` | counter | `source` | Observations skipped because their dedup key was already stored. |
| `publications_total` | counter | `account`, `result` | Publication attempts. |
| `publish_duration_seconds` | histogram | | Time from the publish call to the JetStream acknowledgement. |
| `outbox_pending` | gauge | `account` | Pending publications. |
| `outbox_oldest_pending_seconds` | gauge | | Age of the oldest pending publication. |
| `quarantine_size` | gauge | | Quarantined inbox deliveries and publications. |
| `nats_connected` | gauge | | 1 while the Pulse NATS connection is up. |
| `nats_reconnects_total` | counter | | NATS reconnects. |
| `polls_total` | counter | `account`, `collector`, `result` | Collector polls. |
| `poll_last_success_timestamp_seconds` | gauge | `account`, `collector`, `feed` | Unix time of the last successful poll. |
| `github_requests_total` | counter | `credential`, `status` | GitHub API requests by status class. |
| `github_request_duration_seconds` | histogram | `credential` | GitHub API request latency. |
| `github_rate_limit_remaining` | gauge | `credential` | Last reported primary rate-limit budget. |
| `github_rate_limited_total` | counter | `credential`, `kind` | Responses that hit a primary or secondary rate limit. |
| `credential_failures_total` | counter | `credential`, `class` | Credential refresh or authentication failures. |
| `coverage_degraded` | gauge | `account`, `collector` | 1 while a collector cannot collect (auth, permission, access). |
| `coverage_gaps_total` | counter | `account`, `collector`, `kind` | Detected or suspected collection gaps. |
| `reconcile_resources_total` | counter | `account`, `kind`, `result` | Reconciled resources. |
| `reconcile_requests_total` | counter | `account`, `mode` | GitHub requests spent on reconciliation and backfill. |
| `enrichment_fetches_total` | counter | `account`, `result` | Enrichment fetches. |
| `sqlite_errors_total` | counter | `op` | SQLite operation failures. |
| `storage_db_bytes` | gauge | | State database size including the WAL. |
| `storage_free_bytes` | gauge | | Free bytes on the state volume. |
| `build_info` | gauge | `version`, `commit` | Build metadata. |

## Logs

`log/slog` JSON on stdout, one line per entry, UTC timestamps, with stable keys such as `component`, `account_key`,
`collector`, `feed`, `delivery_id`, `publication_id`, `error_class` and `duration_ms`. Payloads, comment bodies and
tokens are never logged. `log.format: text` is easier to read by hand.

## Alerts

[`deploy/prometheus/alerts.yaml`](https://github.com/aurimasniekis/go-pulse-github/blob/main/deploy/prometheus/alerts.yaml)
covers publication lag, NATS disconnects, storage pressure, quarantine, authentication failures, degraded coverage,
stale polling, sustained rate limiting, gaps, signature failures and SQLite errors. What to do for each is in
[troubleshooting](/operators/troubleshooting).

<<< ../../deploy/prometheus/alerts.yaml
