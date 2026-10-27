// Package publisher sends events to Pulse through the Pulse Go SDK. It never creates or changes streams, buckets or
// credentials; it only publishes (and, when enabled, stores blobs).
package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/aurimasniekis/go-pulse/pkg/pulse"
	"github.com/aurimasniekis/go-pulse/pkg/pulse/envelope"

	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// DefaultMaxPayload is the Pulse/NATS max_payload (1 MiB) used until the server reports its own.
const DefaultMaxPayload = 1 << 20

// Message is one event to publish.
type Message struct {
	ID      string
	Subject string
	Type    string
	Time    time.Time
	Body    []byte
	Headers map[string]string
	Blob    *storage.Blob
}

// Ack is the JetStream acknowledgement.
type Ack struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

// Publisher publishes events and stores blobs.
type Publisher interface {
	Publish(ctx context.Context, m Message) (*Ack, error)
	PutBlob(ctx context.Context, body []byte) (*storage.Blob, error)
	MaxPayload() int64
	Connected() bool
}

// Options configure the Pulse publisher.
type Options struct {
	URL       string
	CredsFile string
	CAFile    string
	Source    string
	Name      string
	Logger    *slog.Logger
	Metrics   *observability.Metrics
	// ExtraNATS adds nats.go options (tests).
	ExtraNATS []nats.Option
}

// Pulse publishes through the Pulse SDK.
type Pulse struct {
	c         *pulse.Client
	connected atomic.Bool
}

// Connect dials Pulse. The connection retries in the background when the server is unreachable at start, so a Pulse
// outage never blocks durable capture.
func Connect(o Options) (*Pulse, error) {
	p := &Pulse{}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	nopts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.ConnectHandler(func(*nats.Conn) {
			p.setConnected(true, o.Metrics)
			log.Info("connected to Pulse", observability.FieldComponent, "publisher")
		}),
		nats.ReconnectHandler(func(*nats.Conn) {
			p.setConnected(true, o.Metrics)
			if o.Metrics != nil {
				o.Metrics.NATSReconnects.Inc()
			}
			log.Info("reconnected to Pulse", observability.FieldComponent, "publisher")
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			p.setConnected(false, o.Metrics)
			log.Warn("disconnected from Pulse", observability.FieldComponent, "publisher", observability.Err(err))
		}),
		nats.ClosedHandler(func(*nats.Conn) { p.setConnected(false, o.Metrics) }),
	}
	nopts = append(nopts, o.ExtraNATS...)
	copts := []pulse.Option{
		pulse.WithCredsFile(o.CredsFile),
		pulse.WithSource(o.Source),
		pulse.WithName(o.Name),
		pulse.WithNATSOptions(nopts...),
	}
	if o.CAFile != "" {
		copts = append(copts, pulse.WithCAFile(o.CAFile))
	}
	c, err := pulse.Connect(o.URL, copts...)
	if err != nil {
		return nil, err
	}
	p.c = c
	if c.Conn().IsConnected() {
		p.setConnected(true, o.Metrics)
	}
	return p, nil
}

func (p *Pulse) setConnected(v bool, m *observability.Metrics) {
	p.connected.Store(v)
	if m != nil {
		if v {
			m.NATSConnected.Set(1)
		} else {
			m.NATSConnected.Set(0)
		}
	}
}

// Client exposes the SDK client.
func (p *Pulse) Client() *pulse.Client { return p.c }

// Connected reports whether NATS is connected.
func (p *Pulse) Connected() bool { return p.connected.Load() && p.c.Conn().IsConnected() }

// MaxPayload is the server's max_payload, or DefaultMaxPayload before the first connection.
func (p *Pulse) MaxPayload() int64 {
	if n := p.c.Conn().MaxPayload(); n > 0 {
		return n
	}
	return DefaultMaxPayload
}

// Publish publishes one event and waits for the JetStream acknowledgement.
func (p *Pulse) Publish(ctx context.Context, m Message) (*Ack, error) {
	opts := []pulse.PublishOption{pulse.WithID(m.ID), pulse.WithTime(m.Time)}
	for k, v := range m.Headers {
		opts = append(opts, pulse.WithHeader(k, v))
	}
	if m.Blob != nil {
		opts = append(opts, pulse.WithBlob(envelope.BlobRef{Bucket: m.Blob.Bucket, Name: m.Blob.Name, Size: m.Blob.Size,
			SHA256: m.Blob.SHA256}))
	}
	ack, err := p.c.Publish(ctx, m.Subject, m.Type, json.RawMessage(m.Body), opts...)
	if err != nil {
		return nil, err
	}
	return &Ack{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// PutBlob stores a payload document in the Pulse blobs bucket (needs the blobs grant).
func (p *Pulse) PutBlob(ctx context.Context, body []byte) (*storage.Blob, error) {
	ref, err := p.c.PutBlob(ctx, bytes.NewReader(body), ".json")
	if err != nil {
		return nil, err
	}
	return &storage.Blob{Bucket: ref.Bucket, Name: ref.Name, Size: ref.Size, SHA256: ref.SHA256}, nil
}

// Close drains the connection (pending acks complete first).
func (p *Pulse) Close() { p.c.Close() }

// Error classes.
const (
	ClassInvalidSubject  = "invalid_subject"
	ClassPayloadTooLarge = "payload_too_large"
	ClassTimeout         = "publish_timeout"
	ClassNoStream        = "no_stream"
	ClassUnavailable     = "nats_unavailable"
	ClassBlob            = "blob_error"
	// ClassRejected is a JetStream API error for this message: the server answered and refused it. It is retried,
	// and quarantined after outbox.poison_attempts so it cannot block its account's partition forever.
	ClassRejected = "publish_rejected"
	ClassOther    = "publish_error"
)

// jsMessageTooLarge is the JetStream API error code for a message above the stream's max_msg_size.
const jsMessageTooLarge = 10054

// Classify maps a publish error to a stable class and whether it is permanent (retrying cannot succeed).
func Classify(err error) (class string, permanent bool) {
	var api *jetstream.APIError
	var legacyAPI *nats.APIError
	switch {
	case errors.Is(err, envelope.ErrSubject):
		return ClassInvalidSubject, true
	case errors.Is(err, nats.ErrMaxPayload):
		return ClassPayloadTooLarge, true
	case errors.As(err, &api) && api.ErrorCode == jsMessageTooLarge:
		return ClassPayloadTooLarge, true
	case errors.Is(err, jetstream.ErrNoStreamResponse), errors.Is(err, nats.ErrNoResponders):
		return ClassNoStream, false
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		// Pulse reports a denied publish as a timeout, so persistent timeouts while connected may be a grant problem.
		return ClassTimeout, false
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrDisconnected),
		errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrConnectionDraining):
		return ClassUnavailable, false
	case errors.As(err, &api), errors.As(err, &legacyAPI):
		return ClassRejected, false
	default:
		return ClassOther, false
	}
}
