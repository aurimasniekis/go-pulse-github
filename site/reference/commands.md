# Commands

Every command takes `--config` / `-c` (default `/etc/pulse-github/config.yaml`, or `PULSE_GITHUB_CONFIG`). In the
container the binary is the entrypoint, so `docker compose run --rm pulse-github <command>` and
`docker compose exec pulse-github pulse-github <command>` both work.

| Command | What it does |
|---|---|
| `run` | Run the service (the image's default command). |
| `version` | Print the version, commit, build date, Go version and the linked Pulse SDK version. |
| `config validate [--check-secrets]` | Validate the configuration; with `--check-secrets`, also read every referenced secret. Values are never printed. |
| `doctor [--test-publish]` | Read-only checks of storage, Pulse connectivity, the GitHub App (permissions, event subscriptions, installations), every token (identity, scopes) and feed. `--test-publish` publishes one event to `evt.<domain>.<first account>.collector.doctor.test` and waits for the JetStream acknowledgement. |
| `status` | Queue depth, quarantine, checkpoints, installations and account identities from the state database. |
| `backfill --account <key> [--since 168h] [--kinds issues,pulls] [--max-requests N]` | Queue a bounded, resumable snapshot backfill for an account. The running service executes it. `--kinds` defaults to the account's `reconcile.kinds`, `--max-requests` to `reconcile.backfill_max_requests`. |
| `quarantine list [--json] [--limit 500]` | List quarantined webhook deliveries and publications. |
| `quarantine replay <id> \| --all [--kind inbox\|outbox]` | Return quarantined records to processing. Publications keep their event ID. |
| `quarantine purge [--older-than 720h] --yes` | Permanently delete quarantined records older than the given age. |
| `healthcheck` | Probe the local `/readyz` endpoint (the image's `HEALTHCHECK`). |
| `completion <shell>` | Shell completion script. |

`run` takes an exclusive lock on the database; a second `run` on the same volume exits. `status`, `quarantine` and
`backfill` open the database without the lock and can run next to the service.

## Examples

```sh
# After an outage: snapshots of everything updated in the last three days, within 500 requests.
pulse-github backfill --account example-org --since 72h --max-requests 500

# Blobs were enabled after a payload_too_large: publish the quarantined rows again.
pulse-github quarantine list
pulse-github quarantine replay --all --kind outbox

# Clean up old quarantine entries after looking at them.
pulse-github quarantine purge --older-than 720h --yes
```
