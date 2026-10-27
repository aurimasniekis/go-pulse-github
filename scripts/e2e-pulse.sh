#!/usr/bin/env bash
# End-to-end check against a real Pulse (pulsed + NATS from the published images): creates a least-privilege client
# through the Pulse admin API, then runs the e2epulse Go test, which drives pulse-github with a fake GitHub API and
# replays the published event with the Pulse SDK.
#
# Requires Docker (Compose v2), curl and Go. Uses host ports 14222 (NATS) and 18080 (admin API) on 127.0.0.1.
set -euo pipefail

PULSED_IMAGE=${PULSED_IMAGE:-ghcr.io/aurimasniekis/pulsed:0.1.3}
NATS_IMAGE=${NATS_IMAGE:-nats:2.15.1-alpine@sha256:7ae34c25dfd6fe2e01c6483eabef20d796690fdcac6c4d94ca780a6cf9877fde}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
PROJECT=pulse-github-e2e-$$
cleanup() {
  docker compose -p "$PROJECT" -f "$WORK/compose.yaml" down -v >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

umask 077
openssl rand -hex 32 >"$WORK/master.key"
chmod 0444 "$WORK/master.key"
cat >"$WORK/compose.yaml" <<YAML
x-env: &env
  PULSE_PROFILE: dev
  PULSE_MASTER_KEY_FILE: /run/secrets/pulse_master_key
  PULSE_RAW_HOST: localhost
  PULSE_NATS_URL: tls://nats:4222
  PULSE_NATS_TLS_NAME: nats
  PULSE_ADMIN_ADDR: 0.0.0.0:8080
  PULSE_ADMIN_PUBLISHED_LOOPBACK: "true"
  PULSE_METRICS_ADDR: 0.0.0.0:9464
  PULSE_HEALTHCHECK_URL: http://127.0.0.1:9464/readyz
  PULSE_PUBLIC_ADDR: 0.0.0.0:8443
  PULSE_EVENTS_MAX_BYTES: 256MiB
secrets:
  pulse_master_key:
    file: ./master.key
volumes:
  pulsed-data:
  nats-data:
services:
  init:
    image: $PULSED_IMAGE
    command: ["init"]
    environment: *env
    secrets: [pulse_master_key]
    volumes: [pulsed-data:/data/pulsed, nats-data:/data/nats]
    network_mode: none
    healthcheck: {disable: true}
  nats:
    image: $NATS_IMAGE
    user: "65532:65532"
    command: ["-c", "/data/nats/nats.conf"]
    volumes: [nats-data:/data/nats]
    ports: ["127.0.0.1:14222:4222"]
    depends_on: {init: {condition: service_completed_successfully}}
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8222/healthz?js-enabled-only=true"]
      interval: 3s
      retries: 30
  pulsed:
    image: $PULSED_IMAGE
    command: ["serve"]
    environment: *env
    secrets: [pulse_master_key]
    volumes: [pulsed-data:/data/pulsed, nats-data:/data/nats]
    ports: ["127.0.0.1:18080:8080"]
    depends_on: {nats: {condition: service_healthy}}
YAML

echo "starting $PULSED_IMAGE"
docker compose -p "$PROJECT" -f "$WORK/compose.yaml" up -d --wait --quiet-pull

PAT=$(docker compose -p "$PROJECT" -f "$WORK/compose.yaml" exec -T pulsed \
  pulsed admin token --email e2e@example.com --create --role operator --days 1 2>/dev/null | tr -d '\r\n')
api() { curl -fsS -H "Authorization: Bearer $PAT" -H 'Content-Type: application/json' "$@"; }

api http://127.0.0.1:18080/api/admin/v1/system/info | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["connect"]["ca_cert"])' >"$WORK/ca.pem"
# publish for pulse-github; subscribe only so the test can replay what was published.
api -X POST http://127.0.0.1:18080/api/admin/v1/clients \
  -d '{"name":"pulse-github-e2e","grants":[{"action":"publish","subject":"evt.github.>"},{"action":"subscribe","subject":"evt.github.>"}]}' |
  python3 -I -c 'import json,sys; sys.stdout.write(json.load(sys.stdin)["creds"])' >"$WORK/client.creds"

echo "client created; running e2epulse test"
cd "$ROOT"
PULSE_E2E_URL=tls://localhost:14222 PULSE_E2E_CREDS="$WORK/client.creds" PULSE_E2E_CA="$WORK/ca.pem" \
  go test -tags e2epulse -count=1 -run TestRealPulse -v ./internal/app/
