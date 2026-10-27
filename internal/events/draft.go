package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aurimasniekis/go-pulse/pkg/pulse/envelope"
)

// Draft is a normalized observation before routing. Subject tokens come from Event and Action (the collector's
// vocabulary); GitHub.Event and GitHub.Action keep GitHub's own values.
type Draft struct {
	Source          string
	DedupKey        string
	Event           string
	Action          string
	OccurredAt      *time.Time
	OccurredAtBasis string
	ObservedAt      time.Time
	Mode            string
	SourceRef       SourceRef
	GitHub          GitHubRef
	Actor           *User
	Repository      *Repository
	Organization    *User
	Resource        *Resource
	Provenance      Provenance
	Raw             json.RawMessage
}

// ObservationID is the draft's stable observation ID.
func (d *Draft) ObservationID() string { return ObservationID(d.Source, d.DedupKey) }

// Rendered is one routed publication, ready for the outbox.
type Rendered struct {
	PublicationID string
	Subject       string
	CEType        string
	CETime        time.Time
	TimeBasis     string
	Body          []byte
}

// Subject builds evt.<domain>.<account>.<source>.<event>.<action> and validates it against the Pulse grammar.
func Subject(domain, accountKey, source, event, action string) (string, error) {
	s := "evt." + domain + "." + accountKey + "." + source + "." + Token(event) + "." + ActionToken(action)
	if err := envelope.ValidateSubject(s); err != nil {
		return "", err
	}
	return s, nil
}

// CEType is the versioned Pulse event type: github.<source>.<event>.<action>.v1.
func CEType(source, event, action string) string {
	return "github." + source + "." + Token(event) + "." + ActionToken(action) + ".v1"
}

// Render produces the publication of a draft for one account.
func Render(d *Draft, domain string, acct AccountRef, routing Routing) (*Rendered, error) {
	if d.Source == "" || d.DedupKey == "" || d.Event == "" || d.ObservedAt.IsZero() {
		return nil, fmt.Errorf("events: incomplete draft (source %q, event %q)", d.Source, d.Event)
	}
	subject, err := Subject(domain, acct.Key, d.Source, d.Event, d.Action)
	if err != nil {
		return nil, err
	}
	obsID := d.ObservationID()
	pubID := PublicationID(obsID, acct.Key)
	observed := d.ObservedAt.UTC()
	var occurred *time.Time
	if d.OccurredAt != nil {
		t := d.OccurredAt.UTC()
		occurred = &t
	}
	p := &Payload{
		SchemaVersion:   SchemaVersion,
		ObservationID:   obsID,
		PublicationID:   pubID,
		Account:         acct,
		Source:          d.SourceRef,
		GitHub:          d.GitHub,
		Actor:           d.Actor,
		Repository:      d.Repository,
		Organization:    d.Organization,
		Resource:        d.Resource,
		OccurredAt:      occurred,
		OccurredAtBasis: d.OccurredAtBasis,
		ObservedAt:      observed,
		CollectionMode:  d.Mode,
		Provenance:      d.Provenance,
		Routing:         routing,
		Payload:         d.Raw,
	}
	p.Source.Collector = d.Source
	body, err := Marshal(p)
	if err != nil {
		return nil, err
	}
	r := &Rendered{
		PublicationID: pubID,
		Subject:       subject,
		CEType:        CEType(d.Source, d.Event, d.Action),
		CETime:        observed,
		TimeBasis:     TimeObserved,
		Body:          body,
	}
	if occurred != nil {
		r.CETime, r.TimeBasis = *occurred, TimeOccurred
	}
	return r, nil
}
