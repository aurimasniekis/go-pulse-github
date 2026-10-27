package routing

import (
	"slices"
	"testing"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

const cfgYAML = `
version: 1
pulse: {url: x, creds_file: y}
webhook: {enabled: true, secrets: [{env: S}]}
github: {app: {enabled: true, app_id: 1, private_key: {env: K}}}
accounts:
  - {login: alice, type: user, github_id: 1, webhook: {enabled: true}}
  - {login: acme, type: organization, github_id: 2, webhook: {enabled: true}, repositories: {deny: [acme/private-*]}}
  - {login: bob, type: user, webhook: {enabled: false}}
routing:
  shares:
    - {from: acme, to: alice, sources: [webhook], repositories: {allow: [acme/shared-*]}}
`

func keys(ts []Target) []string {
	out := []string{}
	for _, t := range ts {
		out = append(out, t.Account.Key)
	}
	return out
}

func TestRoute(t *testing.T) {
	r := New(testutil.Config(t, cfgYAML))
	for _, c := range []struct {
		name, primary, source, repo string
		want                        []string
	}{
		{"org event stays in org", "acme", events.SourceWebhook, "acme/tool", []string{"acme"}},
		{"explicit share", "acme", events.SourceWebhook, "acme/shared-lib", []string{"acme", "alice"}},
		{"share limited to its sources", "acme", events.SourceActivity, "acme/shared-lib", []string{"acme"}},
		{"deny filter", "acme", events.SourceWebhook, "acme/private-x", []string{}},
		{"repository-restricted share skips org-level events", "acme", events.SourceWebhook, "", []string{"acme"}},
		{"webhook routing disabled", "bob", events.SourceWebhook, "", []string{}},
		{"polling ignores the webhook flag", "bob", events.SourceNotifications, "", []string{"bob"}},
	} {
		if got := keys(r.Route(c.primary, events.MatchInstallation, c.source, c.repo)); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	ts := r.Route("acme", events.MatchInstallation, events.SourceWebhook, "acme/shared-lib")
	if ts[1].Routing.Match != events.MatchShare || ts[1].Routing.SharedFrom != "acme" || !slices.Equal(ts[0].Routing.AlsoRouted, []string{"alice"}) {
		t.Errorf("routing metadata: %+v", ts)
	}
}

// Routing never follows actors: an observation is routed to the scope that collected it, never to an account just
// because its login appears in the payload.
func TestRouteIgnoresUnknownPrimary(t *testing.T) {
	r := New(testutil.Config(t, cfgYAML))
	if got := r.Route("someone-else", events.MatchInstallation, events.SourceWebhook, ""); len(got) != 0 {
		t.Fatalf("got %v", keys(got))
	}
}

func TestBind(t *testing.T) {
	r := New(testutil.Config(t, cfgYAML))
	if k, ok := r.UnboundAccountForLogin("BOB"); !ok || k != "bob" {
		t.Fatalf("unbound lookup = %q %v", k, ok)
	}
	if _, ok := r.UnboundAccountForLogin("alice"); ok {
		t.Fatal("bound accounts must not be matched by login")
	}
	if r.Bind("bob", 1, "bob") {
		t.Fatal("binding an ID owned by another key must fail")
	}
	if !r.Bind("bob", 3, "bob") || r.Bind("bob", 4, "bob") {
		t.Fatal("bind once, then refuse a different ID")
	}
	if k, ok := r.AccountForGitHubID(3); !ok || k != "bob" {
		t.Fatal("bound ID must resolve")
	}
	// A rename keeps the routing key (and therefore the subjects) and updates the login.
	if !r.Bind("alice", 1, "alice-renamed") || r.Ref("alice").Login != "alice-renamed" || r.Ref("alice").Key != "alice" {
		t.Fatal("rename handling")
	}
}
