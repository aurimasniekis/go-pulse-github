# Consuming events

pulse-github publishes ordinary Pulse events, so you read them with any Pulse client: the `pulse` CLI, the Go, Python
or TypeScript SDKs, or the HTTP API. The full format is in the [event contract](/reference/event-contract).

## Grant and subscribe

Give each consuming tool its own Pulse client with only the subjects it needs:

```sh
pulse clients create gh-digest --grant 'subscribe:evt.github.example-org.webhook.>' --out gh-digest.creds
```

Then replay and follow:

```sh
pulse sub 'evt.github.example-org.webhook.pull_request.*' --since 24h
```

## Useful filters

| Filter | Matches |
|---|---|
| `evt.github.>` | all GitHub activity |
| `evt.github.octocat.>` | one monitored account |
| `evt.github.*.webhook.>` | webhook activity of every account |
| `evt.github.example-org.webhook.pull_request.*` | every pull request action of one organization |
| `evt.github.*.webhook.issues.opened` | opened issues, all accounts |
| `evt.github.octocat.notifications.>` | one user's notification changes |
| `evt.github.*.collector.>` | coverage gaps and access changes |
| `evt.github.*.reconcile.*.changed` | inferred changes found by reconciliation |

Repository names never appear in subjects. To follow one repository, subscribe to the account and filter on
`repository.full_name` in the payload.

## Go example

A complete program; it compiles against the Pulse SDK v0.1.3.

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurimasniekis/go-pulse/pkg/pulse"
)

// The fields this tool needs from the pulse-github payload (schema_version 1).
type Event struct {
	PublicationID string `json:"publication_id"`
	Account       struct {
		Key string `json:"key"`
	} `json:"account"`
	GitHub struct {
		Event  string `json:"event"`
		Action string `json:"action"`
	} `json:"github"`
	Repository *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := pulse.Connect("tls://r.events.example.com:4222",
		pulse.WithCredsFile("gh-digest.creds"), pulse.WithSource("gh-digest"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	seen := map[string]bool{} // use a durable store in a real tool
	sub, err := c.Subscribe(ctx, "evt.github.example-org.webhook.>",
		pulse.Handle(func(ctx context.Context, e pulse.Event, ev Event) error {
			if seen[ev.PublicationID] { // at-least-once: deduplicate by the event ID
				return nil
			}
			seen[ev.PublicationID] = true
			repo := ""
			if ev.Repository != nil {
				repo = ev.Repository.FullName
			}
			log.Printf("%s %s.%s %s", ev.Account.Key, ev.GitHub.Event, ev.GitHub.Action, repo)
			return nil
		}),
		pulse.Since(time.Now().Add(-24*time.Hour)))
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Stop()
	<-ctx.Done()
}
```

See the [Pulse Go SDK](https://aurimasniekis.github.io/go-pulse/clients/go) for durable consumers, acknowledgements
and blobs.

## What consumers must handle

- **Duplicates.** Delivery is at least once. The same event ID (`Nats-Msg-Id`, also `publication_id` in the body) can
  arrive more than once; be idempotent on it.
- **The same activity from several sources.** A webhook, a notification, an Events API entry and a snapshot of the
  same change are **different** events and are never merged. Correlate them with `github.*` and `resource.*`
  identifiers if you need to.
- **Shares.** With `routing.shares`, one observation is published once per receiving account: same `observation_id`,
  different `publication_id` and subject.
- **Times.** `ce-time` is the time GitHub says it happened when GitHub reports one (header `github-time-basis:
  occurred`), otherwise when pulse-github saw it (`observed`). The Pulse stream time is when JetStream stored it,
  which can be much later after an outage.
- **Snapshots are not history.** `reconcile.*.snapshot` and `reconcile.*.changed` describe current state; they have no
  `occurred_at` and do not say who changed what.
- **Large documents.** When `payload` is `null` and `payload_blob` is set, the full document is in the Pulse blob
  store; read it with `GetBlob`.
- **Gaps.** Watch `evt.github.*.collector.>` if completeness matters to you.
