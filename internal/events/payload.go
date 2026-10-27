package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SchemaVersion is the payload schema version.
const SchemaVersion = 1

// Collectors (the "source" subject token).
const (
	SourceWebhook       = "webhook"
	SourceNotifications = "notifications"
	SourceActivity      = "activity"
	SourceReconcile     = "reconcile"
	SourceEnrichment    = "enrichment"
	SourceCollector     = "collector"
)

// Collection modes.
const (
	ModeLive           = "live"
	ModeBackfill       = "backfill"
	ModeReconciliation = "reconciliation"
	ModeEnrichment     = "enrichment"
)

// Time bases of ce-time.
const (
	TimeOccurred = "occurred"
	TimeObserved = "observed"
)

// Payload is the JSON body of every published event (schema_version 1). Payload (the original source JSON) is last
// and embedded byte-for-byte.
type Payload struct {
	SchemaVersion   int             `json:"schema_version"`
	ObservationID   string          `json:"observation_id"`
	PublicationID   string          `json:"publication_id"`
	Account         AccountRef      `json:"account"`
	Source          SourceRef       `json:"source"`
	GitHub          GitHubRef       `json:"github"`
	Actor           *User           `json:"actor"`
	Repository      *Repository     `json:"repository"`
	Organization    *User           `json:"organization"`
	Resource        *Resource       `json:"resource"`
	OccurredAt      *time.Time      `json:"occurred_at"`
	OccurredAtBasis string          `json:"occurred_at_basis,omitempty"`
	ObservedAt      time.Time       `json:"observed_at"`
	CollectionMode  string          `json:"collection_mode"`
	Provenance      Provenance      `json:"provenance"`
	Routing         Routing         `json:"routing"`
	PayloadBlob     *BlobRef        `json:"payload_blob,omitempty"`
	Payload         json.RawMessage `json:"payload"`
}

// AccountRef identifies the monitored account the event is routed to.
type AccountRef struct {
	Key      string `json:"key"`
	GitHubID int64  `json:"github_id,omitempty"`
	Login    string `json:"login"`
	Type     string `json:"type"`
}

// SourceRef identifies the collector and credential scope that produced the observation.
type SourceRef struct {
	Collector      string `json:"collector"`
	InstallationID int64  `json:"installation_id,omitempty"`
	HookID         string `json:"hook_id,omitempty"`
	Feed           string `json:"feed,omitempty"`
}

// GitHubRef carries the GitHub-side identity of the observation.
type GitHubRef struct {
	Event          string `json:"event"`
	Action         string `json:"action,omitempty"`
	DeliveryID     string `json:"delivery_id,omitempty"`
	EventID        string `json:"event_id,omitempty"`
	NotificationID string `json:"notification_id,omitempty"`
	APIVersion     string `json:"api_version,omitempty"`
	PayloadSHA256  string `json:"payload_sha256,omitempty"`
}

// User is a GitHub user, bot or organization reference.
type User struct {
	ID    int64  `json:"id,omitempty"`
	Login string `json:"login,omitempty"`
	Type  string `json:"type,omitempty"`
}

// Repository is a repository reference.
type Repository struct {
	ID         int64  `json:"id,omitempty"`
	FullName   string `json:"full_name,omitempty"`
	Owner      *User  `json:"owner,omitempty"`
	Private    *bool  `json:"private,omitempty"`
	Visibility string `json:"visibility,omitempty"`
}

// Resource identifies the primary object of the observation.
type Resource struct {
	Kind    string `json:"kind"`
	ID      int64  `json:"id,omitempty"`
	NodeID  string `json:"node_id,omitempty"`
	Number  int64  `json:"number,omitempty"`
	Ref     string `json:"ref,omitempty"`
	SHA     string `json:"sha,omitempty"`
	URL     string `json:"url,omitempty"`
	HTMLURL string `json:"html_url,omitempty"`
}

// Provenance qualifies how authoritative the observation is.
type Provenance struct {
	// Authoritative is true for activity GitHub reported as it happened (webhooks, Events API, audit log).
	Authoritative bool `json:"authoritative"`
	// Snapshot is true for current-state observations (reconciliation); it is not a historical event.
	Snapshot bool `json:"snapshot"`
	// Inferred is true when the observation is a difference this service computed, not something GitHub reported.
	Inferred bool `json:"inferred"`
	// SourceObservationID links enrichment and derived observations to the observation they describe.
	SourceObservationID string   `json:"source_observation_id,omitempty"`
	PreviousFingerprint string   `json:"previous_fingerprint,omitempty"`
	Fingerprint         string   `json:"fingerprint,omitempty"`
	Qualifiers          []string `json:"qualifiers,omitempty"`
}

// Routing explains why the event was routed to its account.
type Routing struct {
	Match      string   `json:"match"`
	SharedFrom string   `json:"shared_from,omitempty"`
	AlsoRouted []string `json:"also_routed_to,omitempty"`
}

// Routing matches.
const (
	MatchInstallation = "installation_account"
	MatchCredential   = "credential_account" //nolint:gosec // a routing match name, not a credential
	MatchShare        = "share"
	MatchService      = "service"
)

// BlobRef is a Pulse blob holding the complete payload document.
type BlobRef struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

var nullTail = []byte(`,"payload":null}`)

// Marshal renders the payload with the original source JSON embedded byte-for-byte (no re-encoding, no number
// conversion). The source JSON must be valid.
func Marshal(p *Payload) ([]byte, error) {
	raw := p.Payload
	if raw == nil {
		raw = json.RawMessage("null")
	}
	if !json.Valid(raw) {
		return nil, errors.New("events: source payload is not valid JSON")
	}
	cp := *p
	cp.Payload = nil
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(&cp); err != nil {
		return nil, err
	}
	b := bytes.TrimRight(buf.Bytes(), "\n")
	if !bytes.HasSuffix(b, nullTail) {
		return nil, fmt.Errorf("events: unexpected payload encoding")
	}
	out := make([]byte, 0, len(b)+len(raw))
	out = append(out, b[:len(b)-len(nullTail)]...)
	out = append(out, `,"payload":`...)
	out = append(out, raw...)
	out = append(out, '}')
	return out, nil
}

// BlobStub turns a rendered payload document into the small event body published when the full document is stored as
// a Pulse blob: every metadata field is kept, payload becomes null and payload_blob points at the blob.
func BlobStub(doc []byte, ref BlobRef) ([]byte, error) {
	var p Payload
	if err := json.Unmarshal(doc, &p); err != nil {
		return nil, fmt.Errorf("events: decode payload document: %w", err)
	}
	p.Payload = nil
	p.PayloadBlob = &ref
	return Marshal(&p)
}
