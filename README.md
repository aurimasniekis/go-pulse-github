# pulse-github

A headless, Docker-first service that collects GitHub activity for one or many personal and organization accounts
and publishes it into an existing [Pulse](https://github.com/aurimasniekis/go-pulse) NATS JetStream event bus.

- **GitHub App webhooks** (primary, real time). Every delivered event, including unknown ones, is signature-checked
  and committed to SQLite before GitHub gets a 2xx.
- **Personal notifications** (`GET /notifications`, classic PAT), with state-change detection.
- **Events API feeds** (supplementary, bounded), with gap detection, plus the optional Enterprise Cloud **audit log**.
- **Reconciliation and backfill**: installation and repository access, and bounded, resumable snapshots of
  repositories, issues, pull requests, releases and workflow runs.
- **At-least-once publication** through the Pulse Go SDK (v0.1.3). Event IDs are stable across retries, events are
  published per account in FIFO order, NATS outages are queued durably, and payloads too large for one message go to
  a blob or quarantine.
- Account-scoped subjects: `evt.github.<account>.<source>.<event>.<action>`.

What is and is not observable: [docs/coverage.md](docs/coverage.md). Event format:
[docs/event-contract.md](docs/event-contract.md). Design: [docs/architecture.md](docs/architecture.md) and
[docs/adr/](docs/adr). Running it: [docs/operations.md](docs/operations.md).

## Minimal deployment

1. **Pulse client** with least privilege:
   ```sh
   pulse clients create pulse-github --grant 'publish:evt.github.>' --out secrets/pulse_github.creds
   ```
2. **GitHub App** with read permissions and event subscriptions (see
   [docs/operations.md](docs/operations.md#github-app)), installed on each account. Save its private key to
   `secrets/github_app_key.pem` and the webhook secret to `secrets/github_webhook_secret`. Point the webhook URL at
   `https://<your host>/webhook` behind a TLS reverse proxy.
3. **Classic PAT** (`notifications` scope) for personal accounts, in `secrets/github_pat_<account>`.
4. **Configuration:** copy [deploy/config.example.yaml](deploy/config.example.yaml) to `config.yaml` and set the
   Pulse URL, App ID and accounts.
5. **Run** with [deploy/compose.yaml](deploy/compose.yaml):
   ```sh
   docker compose -f deploy/compose.yaml run --rm pulse-github config validate --check-secrets
   docker compose -f deploy/compose.yaml run --rm pulse-github doctor --test-publish
   docker compose -f deploy/compose.yaml up -d
   ```
6. **Consume:**
   ```sh
   pulse sub 'evt.github.>' --since 1h
   ```

Polling-only deployments set `webhook.enabled: false` and need no inbound access. They have reduced, delayed
coverage.

## Build and test

```sh
make build            # bin/pulse-github (CGO_ENABLED=0)
make test             # go vet + race tests (embedded JetStream with Pulse-equivalent least-privilege creds)
make lint vuln        # golangci-lint v2, govulncheck
make docker           # local image; make docker-multiarch builds linux/amd64 and linux/arm64
make e2e-pulse        # opt-in: real pulsed 0.1.3 + NATS in Docker, client created via the Pulse admin API
```

The Pulse SDK requires Go 1.27.3. With an older local Go, the `go` command downloads it (`GOTOOLCHAIN=auto`).

## Verification status

Verified with evidence in this repository:

- Unit and integration tests, including:
  - an embedded nats-server in operator/JWT mode with Pulse's `EVENTS` configuration and `publish:evt.github.>`
    permissions mirrored from Pulse v0.1.3;
  - an end-to-end run (signed webhook → SQLite → Pulse SDK → JetStream) across a NATS outage.
- `scripts/e2e-pulse.sh` against the real `ghcr.io/aurimasniekis/pulsed:0.1.3`, with a client issued by Pulse's
  admin API: published, then replayed with the SDK.
- Docker image built for linux/arm64 and run read-only as uid 65532 (health, readiness, metrics, SIGTERM exit 0).
  linux/amd64 cross-builds.

Not verified:

- No live GitHub credentials were used. All GitHub behaviour is tested against fake servers following the official
  documentation.
- GitHub Enterprise Server is configurable but untested.
- The audit-log query details (phrase date format, `order=asc`) come from the docs only.
- The amd64 image was built but not run.
