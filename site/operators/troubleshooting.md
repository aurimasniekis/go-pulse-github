# Troubleshooting and recovery

## Troubleshooting

| Symptom | Check |
|---|---|
| Events not appearing in Pulse | `status` (pending outbox?), `pulse_github_nats_connected`, logs with `component=outbox` and their `error_class`. `publish_timeout` while connected usually means a missing `publish` grant: run `doctor --test-publish`. |
| `invalid_subject` in quarantine | Should not happen: subjects are validated before storage. Report it with the `quarantine list --json` output. |
| `publish_rejected` in quarantine | JetStream refused the message several times in a row (stream limits, a changed stream configuration). `quarantine list --json` shows the server's error; fix the cause, then `quarantine replay`. |
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

