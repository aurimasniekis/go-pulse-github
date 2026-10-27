# Contributing to pulse-github

pulse-github copies private GitHub content into Pulse and holds credentials for every monitored account. A change must
never weaken a security guarantee, default or check silently. If it relaxes one, say so explicitly in the pull request
and in the CHANGELOG `Security` section, and explain why.

> License: MIT (see [LICENSE](LICENSE)). Contributions are accepted under the same license.

## Development setup

- Go: the version in `go.mod` (the Pulse SDK requires 1.27.3). CI uses `GOTOOLCHAIN=local` with exactly that version.
- `golangci-lint` v2, `govulncheck`, `gitleaks` (the Makefile runs the last two with `go run`).
- Node 24 LTS with pnpm 10, for the documentation website (`site/`).
- Docker, for the image and the opt-in `make e2e-pulse` run against a real Pulse.

## Everyday commands

| Command | What it does |
|---|---|
| `make build` | `bin/pulse-github` (static, `CGO_ENABLED=0`). |
| `make test` | `go vet` and every Go test with the race detector, including the embedded JetStream integration tests. |
| `make lint` | golangci-lint with the repository configuration. |
| `make check` | Everything CI runs except the image build: gofmt, lint, tests, govulncheck, gitleaks. |
| `make image` / `make image-multiarch` | The container image for the local platform / for linux/amd64 and linux/arm64. |
| `make e2e-pulse` | Real `pulsed` and NATS in Docker, a least-privilege client from the Pulse admin API, a signed webhook through the service, replayed with the SDK. |
| `make site-dev` | The documentation website with live reload. |

`make help` lists every target.

## Rules

- Tests accompany every change. Every bug fix adds a regression test. Tests use the standard library only, live in the
  package they test, and follow the style of the existing ones (and of go-pulse).
- GitHub behaviour is tested against the fake server in `internal/githubtest`, built from GitHub's documentation.
  Say so when something has not been checked against the real GitHub API.
- Never commit secrets, `.creds`, PATs, App private keys or real infrastructure details. Tests generate keys at run
  time. Use `example.com` hosts and `example-org` style accounts in docs and tests.
- No `InsecureSkipVerify`. Crypto comes from the Go standard library.
- State lives in SQLite; events go to Pulse. No other datastore.
- Never call work done when it is only implemented: say whether it is implemented, tested or externally verified.
- Commits follow Conventional Commits. User-facing changes get a CHANGELOG entry under `[Unreleased]`.
- Design changes that alter a trade-off get a decision record in
  [site/design/decisions.md](site/design/decisions.md).

## Reporting security issues

See [SECURITY.md](SECURITY.md). Report privately; do not open public issues.
