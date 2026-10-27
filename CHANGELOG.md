# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/spec/v2.0.0.html), see [docs/releasing.md](docs/releasing.md).

## [Unreleased]

### Added
- GitHub App webhook receiver: HMAC verification with secret rotation, size and time limits, and a durable SQLite
  inbox. GitHub gets its 2xx only after the delivery is committed. Unknown events and actions are kept and published.
- Personal notifications polling (`GET /notifications`, classic PAT) with state-change detection, periodic full
  sweeps for read-state changes and optional, rate-budgeted enrichment of the referenced resources.
- Events API feeds (user, received, user-org, org, repository) with ETag polling and gap detection, plus the optional
  Enterprise Cloud organization audit log.
- Installation and repository-access reconciliation, and bounded, resumable snapshot jobs (repositories, issues,
  pull requests, releases, workflow runs) for periodic reconciliation and `backfill`.
- At-least-once publication to Pulse through the Go SDK v0.1.3: deterministic event IDs, a per-account FIFO outbox,
  durable queuing during NATS outages, blob offload for oversized documents and a quarantine with replay.
- Account-scoped subjects `evt.github.<account>.<source>.<event>.<action>` with a collision-free token encoding,
  validated with Pulse's own subject validator before storage.
- Commands: `run`, `version`, `config validate`, `doctor`, `backfill`, `quarantine list|replay|purge`, `status`,
  `healthcheck`.
- Distroless, non-root, multi-arch container image; Compose example, commented configuration and Prometheus alerts.
- Documentation website (VitePress, GitHub Pages) with coverage, event contract, configuration and operations guides.
