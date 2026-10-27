# Security policy

pulse-github holds GitHub App private keys, webhook secrets, personal access tokens and Pulse credentials, and it
copies private repository content into Pulse. Please report vulnerabilities privately.

## Reporting

- Use GitHub's private vulnerability reporting: "Report a vulnerability" on the repository's
  [Security tab](https://github.com/aurimasniekis/go-pulse-github/security/advisories/new).
- Do not open public issues for suspected vulnerabilities.
- Include affected versions, configuration, reproduction steps and impact. Never include real tokens, private keys,
  webhook secrets, `.creds` files or event data.

We aim to acknowledge reports within 3 working days and to agree on a disclosure date with you. Fixes ship for the
latest release.

## Scope and guarantees

- Webhook deliveries are accepted only with a valid `X-Hub-Signature-256` for one of the configured secrets, checked in
  constant time over the unmodified body, and only up to the configured size.
- Secrets are referenced from files or environment variables; literal secrets in the configuration are rejected.
  Tokens are kept in memory only and never logged.
- URLs taken from GitHub content (pagination, enrichment) are followed only when they match the configured API origin
  and path prefix, so credentials cannot be sent elsewhere.
- pulse-github only reads from GitHub. It never marks notifications read, redelivers webhooks or changes anything.
- The container runs as a non-root user on a read-only root filesystem with no shell.

Accepted risks:

- Anyone who can subscribe to `evt.github.>` in Pulse can read the private GitHub content published there. Grant
  subscriptions narrowly.
- Content deleted on GitHub, or access lost there, does not remove events already in Pulse.
- The state database holds raw payloads for the configured retention (7 days by default) on the state volume. Disk
  encryption is the host's job.
- Nothing has had an external security review yet.
