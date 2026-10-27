package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// FakePublisher records publications in memory. Like JetStream within its duplicate window, a repeated ID is
// acknowledged as a duplicate and not stored again.
type FakePublisher struct {
	mu      sync.Mutex
	Stored  []publisher.Message
	Seen    map[string]bool
	Blobs   map[string][]byte
	Err     error
	BlobErr error
	Max     int64
	Calls   int
}

// NewFakePublisher returns a connected fake with a 1 MiB limit.
func NewFakePublisher() *FakePublisher {
	return &FakePublisher{Seen: map[string]bool{}, Blobs: map[string][]byte{}, Max: publisher.DefaultMaxPayload}
}

// SetErr makes Publish fail (nil restores it).
func (f *FakePublisher) SetErr(err error) {
	f.mu.Lock()
	f.Err = err
	f.mu.Unlock()
}

// Publish implements publisher.Publisher.
func (f *FakePublisher) Publish(_ context.Context, m publisher.Message) (*publisher.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls++
	if f.Err != nil {
		return nil, f.Err
	}
	if f.Seen[m.ID] {
		return &publisher.Ack{Stream: "EVENTS", Sequence: uint64(len(f.Stored)), Duplicate: true}, nil
	}
	f.Seen[m.ID] = true
	f.Stored = append(f.Stored, m)
	return &publisher.Ack{Stream: "EVENTS", Sequence: uint64(len(f.Stored))}, nil
}

// PutBlob implements publisher.Publisher.
func (f *FakePublisher) PutBlob(_ context.Context, body []byte) (*storage.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.BlobErr != nil {
		return nil, f.BlobErr
	}
	name := fmt.Sprintf("b%d.json", len(f.Blobs))
	f.Blobs[name] = append([]byte(nil), body...)
	sum := sha256.Sum256(body)
	return &storage.Blob{Bucket: "blobs", Name: name, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}, nil
}

// MaxPayload implements publisher.Publisher.
func (f *FakePublisher) MaxPayload() int64 { return f.Max }

// Connected implements publisher.Publisher.
func (f *FakePublisher) Connected() bool { return true }

// Messages returns a copy of the stored messages.
func (f *FakePublisher) Messages() []publisher.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publisher.Message(nil), f.Stored...)
}

// Forget drops the duplicate memory, as JetStream does once its duplicate window has passed.
func (f *FakePublisher) Forget() {
	f.mu.Lock()
	f.Seen = map[string]bool{}
	f.mu.Unlock()
}
