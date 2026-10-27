# Quick start

This runs pulse-github with Docker Compose next to an existing [Pulse](https://aurimasniekis.github.io/go-pulse/)
installation. pulse-github does not run NATS or Pulse itself: it connects as an ordinary, least-privilege Pulse
client.

You need:

- a running Pulse and the `pulse` CLI with administrator access ([Pulse quick start](https://aurimasniekis.github.io/go-pulse/guide/try-locally));
- Docker with Compose v2;
- for webhooks, a public HTTPS hostname (for example `hooks.example.com`) that can reach the container. Without one,
  run [polling only](#polling-only).

::: tip No release yet
There is no published image yet. Build it locally under the name the Compose file uses:

```sh
git clone https://github.com/aurimasniekis/go-pulse-github.git && cd go-pulse-github
make image IMAGE=ghcr.io/aurimasniekis/pulse-github:latest
```
:::

## 1. Credentials

Create a `secrets/` directory next to your `compose.yaml` and put each credential in its own file:

| File | What | How |
|---|---|---|
| `secrets/pulse_github.creds` | Pulse client | `pulse clients create pulse-github --grant 'publish:evt.github.>' --out secrets/pulse_github.creds` |
| `secrets/github_app_key.pem` | GitHub App private key | App settings → **Private keys** → Generate ([GitHub App](/guide/github#github-app)) |
| `secrets/github_webhook_secret` | GitHub App webhook secret | `openssl rand -hex 32`; paste the same value into the App settings |
| `secrets/github_pat_<login>` | classic PAT with `notifications` | GitHub → Settings → Developer settings → **Tokens (classic)** |

```sh
sudo chown -R 65532:65532 secrets && sudo chmod 0400 secrets/*
```

The container runs as uid 65532 and reads these files as Compose secrets under `/run/secrets/`.

## 2. Configuration

Save this as `config.yaml` and change the Pulse URL, the App ID and client ID, and the accounts:

```yaml
version: 1

pulse:
  url: tls://r.events.example.com:4222
  creds_file: /run/secrets/pulse_github.creds

webhook:
  enabled: true
  secrets:
    - {file: /run/secrets/github_webhook_secret}

admin:
  listen: 0.0.0.0:9090            # health, readiness, metrics; keep it private

github:
  app:
    enabled: true
    app_id: 123456
    client_id: Iv23liEXAMPLE
    private_key: {file: /run/secrets/github_app_key.pem}

accounts:
  # You: App webhooks for your repositories, plus notifications and your activity feeds (classic PAT).
  - login: octocat
    type: user
    token: {file: /run/secrets/github_pat_octocat}
    webhook: {enabled: true}
    notifications: {enabled: true}
    activity:
      enabled: true
      feeds:
        - {kind: user_events}
        - {kind: received_events}

  # An organization: App webhooks only.
  - login: example-org
    type: organization
    webhook: {enabled: true}
```

Everything else has defaults. The [configuration reference](/reference/configuration) shows every setting.

## 3. Compose

Copy [`deploy/compose.yaml`](https://github.com/aurimasniekis/go-pulse-github/blob/main/deploy/compose.yaml) next to
`config.yaml`. It names the PAT secret `github_pat_aurimasniekis`: rename it (in `secrets:` and in the service's
`secrets:` list) to match your `secrets/github_pat_<login>` file, and set `PULSE_GITHUB_PULSE_URL`.

Check the configuration and the connections before starting the service:

```sh
docker compose run --rm pulse-github config validate --check-secrets
docker compose run --rm pulse-github doctor --test-publish
```

`doctor` is read-only. It checks storage, every GitHub credential and the App's permissions and event subscriptions,
and with `--test-publish` it publishes one event to `evt.github.<account>.collector.doctor.test` and waits for the
JetStream acknowledgement. A missing Pulse grant shows up there as a timeout.

```sh
docker compose up -d
curl -fsS http://127.0.0.1:9090/readyz
```

## 4. Point GitHub at it

Route `https://hooks.example.com/webhook` to the container's port 8080 through a TLS reverse proxy
([details](/operators/#reverse-proxy-for-webhooks)), then set that URL in the App's webhook settings. GitHub sends a
`ping` right away. App pings belong to no installation, so pulse-github stores it as unrouted instead of publishing it;
a `202` under the App's **Advanced → Recent Deliveries** confirms that the endpoint and the secret work.

## 5. Watch the events

With a Pulse client that has `subscribe:evt.github.>`:

```sh
pulse sub 'evt.github.>' --since 1h
```

Open an issue in a repository the App is installed on and it appears as
`evt.github.example-org.webhook.issues.opened`. [Consuming events](/guide/consuming) explains filters and
deduplication.

## Polling only

Without an inbound endpoint, set `webhook.enabled: false`, drop the `webhook:` blocks from the accounts, and remove
the webhook secret from `config.yaml` and `compose.yaml`. Notifications, Events API feeds, the audit log and (with the App configured) reconciliation still
work, with less coverage and more delay; see [coverage](/reference/coverage).
