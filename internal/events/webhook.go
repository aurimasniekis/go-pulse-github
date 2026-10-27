package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// WebhookInput is a stored, signature-verified webhook delivery.
type WebhookInput struct {
	DeliveryID string
	HookID     string
	Event      string
	Body       []byte
	ReceivedAt time.Time
}

// WebhookMeta is what routing needs from a delivery.
type WebhookMeta struct {
	Action         string
	InstallationID int64
	// InstallationAccount is present on installation lifecycle events.
	InstallationAccount *User
	RepositoryFullName  string
	RepositoryOwnerID   int64
	OrganizationID      int64
}

type ghUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
	Type  string `json:"type"`
}

func (u *ghUser) ref() *User {
	if u == nil || (u.ID == 0 && u.Login == "") {
		return nil
	}
	return &User{ID: u.ID, Login: u.Login, Type: u.Type}
}

type ghRepo struct {
	ID         int64   `json:"id"`
	FullName   string  `json:"full_name"`
	Owner      *ghUser `json:"owner"`
	Private    *bool   `json:"private"`
	Visibility string  `json:"visibility"`
}

func (r *ghRepo) ref() *Repository {
	if r == nil || (r.ID == 0 && r.FullName == "") {
		return nil
	}
	return &Repository{ID: r.ID, FullName: r.FullName, Owner: r.Owner.ref(), Private: r.Private, Visibility: r.Visibility}
}

type webhookEnvelope struct {
	Action       json.RawMessage `json:"action"`
	Installation *struct {
		ID      int64   `json:"id"`
		Account *ghUser `json:"account"`
	} `json:"installation"`
	Sender       *ghUser `json:"sender"`
	Repository   *ghRepo `json:"repository"`
	Organization *ghUser `json:"organization"`
}

// resourceSpec says where the primary object of a known webhook event lives.
type resourceSpec struct {
	field string
	kind  string
}

var webhookResources = map[string]resourceSpec{
	"issues":                      {"issue", "issue"},
	"issue_comment":               {"comment", "issue_comment"},
	"pull_request":                {"pull_request", "pull_request"},
	"pull_request_review":         {"review", "pull_request_review"},
	"pull_request_review_comment": {"comment", "pull_request_review_comment"},
	"pull_request_review_thread":  {"thread", "pull_request_review_thread"},
	"commit_comment":              {"comment", "commit_comment"},
	"release":                     {"release", "release"},
	"discussion":                  {"discussion", "discussion"},
	"discussion_comment":          {"comment", "discussion_comment"},
	"workflow_run":                {"workflow_run", "workflow_run"},
	"workflow_job":                {"workflow_job", "workflow_job"},
	"check_run":                   {"check_run", "check_run"},
	"check_suite":                 {"check_suite", "check_suite"},
	"deployment":                  {"deployment", "deployment"},
	"deployment_status":           {"deployment_status", "deployment_status"},
	"fork":                        {"forkee", "repository"},
	"member":                      {"member", "user"},
	"membership":                  {"member", "user"},
	"team":                        {"team", "team"},
	"team_add":                    {"team", "team"},
	"milestone":                   {"milestone", "milestone"},
	"label":                       {"label", "label"},
	"installation":                {"installation", "installation"},
	"installation_repositories":   {"installation", "installation"},
	"organization":                {"membership", "membership"},
}

// occurredSpec maps a known event/action to the payload timestamp that records when it happened. Only fields with that
// unambiguous meaning are listed; everything else leaves occurred_at null.
var webhookOccurred = map[string]string{
	"issues.opened":                       "issue.created_at",
	"issues.closed":                       "issue.closed_at",
	"issue_comment.created":               "comment.created_at",
	"pull_request.opened":                 "pull_request.created_at",
	"pull_request.closed":                 "pull_request.closed_at",
	"pull_request_review.submitted":       "review.submitted_at",
	"pull_request_review_comment.created": "comment.created_at",
	"commit_comment.created":              "comment.created_at",
	"release.created":                     "release.created_at",
	"release.published":                   "release.published_at",
	"discussion.created":                  "discussion.created_at",
	"discussion_comment.created":          "comment.created_at",
	"workflow_job.in_progress":            "workflow_job.started_at",
	"workflow_job.completed":              "workflow_job.completed_at",
	"check_run.completed":                 "check_run.completed_at",
	"deployment.created":                  "deployment.created_at",
	"deployment_status.created":           "deployment_status.created_at",
	"fork":                                "forkee.created_at",
	"star.created":                        "starred_at",
	"push":                                "repository.pushed_at",
}

// ParseWebhook extracts routing metadata. It fails only for bodies that are not JSON objects.
func ParseWebhook(body []byte) (*WebhookMeta, *webhookEnvelope, error) {
	var env webhookEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, nil, fmt.Errorf("decode webhook body: %w", err)
	}
	m := &WebhookMeta{Action: rawString(env.Action)}
	if env.Installation != nil {
		m.InstallationID = env.Installation.ID
		m.InstallationAccount = env.Installation.Account.ref()
	}
	if env.Repository != nil {
		m.RepositoryFullName = env.Repository.FullName
		if env.Repository.Owner != nil {
			m.RepositoryOwnerID = env.Repository.Owner.ID
		}
	}
	if env.Organization != nil {
		m.OrganizationID = env.Organization.ID
	}
	return m, &env, nil
}

// NormalizeWebhook builds the draft of a webhook delivery. The dedup key is the delivery ID: a redelivery of the same
// delivery is the same observation.
func NormalizeWebhook(in WebhookInput, mode string) (*Draft, *WebhookMeta, error) {
	meta, env, err := ParseWebhook(in.Body)
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(in.Body)
	d := &Draft{
		Source:     SourceWebhook,
		DedupKey:   "webhook:" + in.DeliveryID,
		Event:      in.Event,
		Action:     meta.Action,
		ObservedAt: in.ReceivedAt.UTC(),
		Mode:       mode,
		SourceRef:  SourceRef{InstallationID: meta.InstallationID, HookID: in.HookID},
		GitHub: GitHubRef{Event: in.Event, Action: meta.Action, DeliveryID: in.DeliveryID,
			PayloadSHA256: hex.EncodeToString(sum[:])},
		Actor:        env.Sender.ref(),
		Repository:   env.Repository.ref(),
		Organization: env.Organization.ref(),
		Provenance:   Provenance{Authoritative: true},
		Raw:          json.RawMessage(in.Body),
	}
	if len(env.Action) > 0 && meta.Action == "" && string(env.Action) != "null" {
		d.Provenance.Qualifiers = append(d.Provenance.Qualifiers, "non_string_action")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(in.Body, &obj); err == nil {
		d.Resource = webhookResource(in.Event, obj)
		key := in.Event
		if meta.Action != "" {
			key += "." + meta.Action
		}
		if path, ok := webhookOccurred[key]; ok {
			if t := timeAt(obj, path); t != nil {
				d.OccurredAt, d.OccurredAtBasis = t, "payload:"+path
			}
		}
	}
	switch in.Event {
	case "push":
		d.Provenance.Qualifiers = append(d.Provenance.Qualifiers, "push_commits_max_2048", "no_events_for_more_than_3_tags")
	}
	return d, meta, nil
}

func webhookResource(event string, obj map[string]json.RawMessage) *Resource {
	if event == "push" || event == "create" || event == "delete" {
		var ref struct {
			Ref     string `json:"ref"`
			RefType string `json:"ref_type"`
			After   string `json:"after"`
		}
		if b, err := json.Marshal(obj); err == nil && json.Unmarshal(b, &ref) == nil && ref.Ref != "" {
			return &Resource{Kind: "ref", Ref: ref.Ref, SHA: ref.After}
		}
		return nil
	}
	spec, ok := webhookResources[event]
	if !ok {
		return nil
	}
	raw, ok := obj[spec.field]
	if !ok {
		return nil
	}
	return ResourceFrom(spec.kind, raw)
}

// ResourceFrom extracts identifiers from a GitHub object.
func ResourceFrom(kind string, raw json.RawMessage) *Resource {
	var o struct {
		ID      json.RawMessage `json:"id"`
		NodeID  string          `json:"node_id"`
		Number  int64           `json:"number"`
		URL     string          `json:"url"`
		HTMLURL string          `json:"html_url"`
		HeadSHA string          `json:"head_sha"`
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil
	}
	r := &Resource{Kind: kind, NodeID: o.NodeID, Number: o.Number, URL: o.URL, HTMLURL: o.HTMLURL, SHA: o.HeadSHA}
	if id, err := strconv.ParseInt(string(o.ID), 10, 64); err == nil {
		r.ID = id
	}
	if r.ID == 0 && r.NodeID == "" && r.URL == "" && r.Number == 0 {
		return nil
	}
	return r
}

// timeAt reads an RFC 3339 string or a Unix-seconds number at a dotted path.
func timeAt(obj map[string]json.RawMessage, path string) *time.Time {
	parts := strings.Split(path, ".")
	cur := obj
	for i, p := range parts {
		raw, ok := cur[p]
		if !ok {
			return nil
		}
		if i == len(parts)-1 {
			return parseTime(raw)
		}
		next := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &next); err != nil {
			return nil
		}
		cur = next
	}
	return nil
}

func parseTime(raw json.RawMessage) *time.Time {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil
		}
		t = t.UTC()
		return &t
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		if v, err := n.Int64(); err == nil && v > 0 {
			t := time.Unix(v, 0).UTC()
			return &t
		}
	}
	return nil
}

// ParseTime parses a GitHub RFC 3339 timestamp string; empty or invalid values give nil.
func ParseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

func rawString(raw json.RawMessage) string {
	var s string
	if len(raw) == 0 || json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}
