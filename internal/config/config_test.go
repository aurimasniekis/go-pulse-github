package config

import (
	"strings"
	"testing"
	"time"
)

const base = `
version: 1
pulse:
  url: tls://nats:4222
  creds_file: /run/secrets/pulse.creds
webhook:
  enabled: true
  secrets: [{file: /run/secrets/hook}]
github:
  app:
    enabled: true
    app_id: 1
    private_key: {file: /run/secrets/app.pem}
`

func TestParseValid(t *testing.T) {
	env := map[string]string{"PULSE_GITHUB_STORAGE_PATH": "/state/x.db", "PULSE_GITHUB_SHUTDOWN_TIMEOUT": "40s"}
	c, err := Parse([]byte(base+`
accounts:
  - login: AurimasNiekis
    type: user
    token: {env: GH_TOKEN}
    webhook: {enabled: true}
    notifications: {enabled: true}
    activity: {enabled: true, feeds: [{kind: user_events}, {kind: received_events}]}
  - login: example-org
    type: organization
    webhook: {enabled: true}
routing:
  shares: [{from: example-org, to: aurimasniekis, sources: [webhook]}]
`), func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.Accounts[0].Key != "aurimasniekis" {
		t.Errorf("key %q: want the lowercased login", c.Accounts[0].Key)
	}
	if c.Storage.Path != "/state/x.db" || c.Service.ShutdownTimeout.D() != 40*time.Second {
		t.Errorf("env overrides not applied: %q %v", c.Storage.Path, c.Service.ShutdownTimeout.D())
	}
	if c.GitHub.APIVersion != "2022-11-28" || c.Pulse.SubjectDomain != "github" || c.Accounts[1].Activity.Interval.D() != time.Minute {
		t.Error("defaults not applied")
	}
}

func TestParseRejects(t *testing.T) {
	for want, doc := range map[string]string{
		"not a literal value":           base + "accounts:\n  - {login: a, type: user, token: ghp_literal}\n",
		"duplicate key":                 base + "accounts:\n  - {login: a, type: user}\n  - {login: b, key: a, type: user}\n",
		"configured twice":              base + "accounts:\n  - {login: A, type: user}\n  - {login: a, key: other, type: user}\n",
		`key "a.b" must match`:          base + "accounts:\n  - {login: a, key: a.b, type: user}\n",
		`key ">" must match`:            base + "accounts:\n  - {login: a, key: '>', type: user}\n",
		`key "Upper" must match`:        base + "accounts:\n  - {login: a, key: Upper, type: user}\n",
		"classic personal access token": base + "accounts:\n  - {login: a, type: user, notifications: {enabled: true}}\n",
		"only available for user":       base + "accounts:\n  - {login: o, type: organization, token: {env: X}, notifications: {enabled: true}}\n",
		"at least 10s":                  base + "accounts:\n  - {login: a, type: user, token: {env: X}, notifications: {enabled: true, interval: 1s}}\n",
		"field colour not found":        base + "accounts:\n  - {login: a, type: user, colour: red}\n",
		"must be configured account":    base + "accounts:\n  - {login: a, type: user}\nrouting:\n  shares: [{from: a, to: b, sources: [webhook]}]\n",
		"not supported":                 "version: 2\n",
		"requires github.app":           "version: 1\npulse: {url: x, creds_file: y}\nwebhook: {enabled: true, secrets: [{env: S}]}\naccounts:\n  - {login: a, type: user}\n",
		"needs a user account":          base + "accounts:\n  - {login: o, type: organization, token: {env: X}, activity: {enabled: true, feeds: [{kind: user_events}]}}\n",
		"GitHub Enterprise Cloud":       base + "accounts:\n  - {login: o, type: organization, activity: {enabled: true, feeds: [{kind: audit_log, org: o, credential: installation}]}}\n",
		"at least one account":          base + "accounts: []\n",
		"webhook.enabled is false":      "version: 1\npulse: {url: x, creds_file: y}\naccounts:\n  - {login: a, type: user, webhook: {enabled: true}}\n",
	} {
		_, err := Parse([]byte(doc), nil)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want error containing %q, got %v", want, err)
		}
	}
}

func TestRepoFilter(t *testing.T) {
	f := RepoFilter{Allow: []string{"acme/*"}, Deny: []string{"acme/secret-*"}}
	for repo, want := range map[string]bool{"acme/tool": true, "ACME/Tool": true, "acme/secret-plans": false, "other/tool": false} {
		if f.Allows(repo) != want {
			t.Errorf("%s: want %v", repo, want)
		}
	}
	if !(RepoFilter{}).Allows("any/repo") {
		t.Error("an empty filter allows everything")
	}
}

func TestByteSize(t *testing.T) {
	for in, want := range map[string]int64{"512MiB": 512 << 20, "1GiB": 1 << 30, "4096": 4096, "2KB": 2000} {
		if got, err := ParseByteSize(in); err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	for _, in := range []string{"-1", "lots", "1.5GiB"} {
		if _, err := ParseByteSize(in); err == nil {
			t.Errorf("%s: want error", in)
		}
	}
}

func TestSecretRef(t *testing.T) {
	t.Setenv("PG_TEST_SECRET", "  value \n")
	if b, err := (SecretRef{Env: "PG_TEST_SECRET"}).Resolve(); err != nil || string(b) != "value" {
		t.Fatalf("%q %v", b, err)
	}
	if _, err := (SecretRef{Env: "PG_TEST_UNSET"}).Resolve(); err == nil {
		t.Error("unset env secret must fail")
	}
	if s := (SecretRef{File: "/run/x"}).String(); s != "file:/run/x" {
		t.Errorf("String() = %q", s)
	}
}

// The shipped example must always parse.
func TestExampleConfig(t *testing.T) {
	c, err := Load("../../deploy/config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Accounts) != 2 || c.Accounts[1].Key != "example-org" {
		t.Fatalf("%+v", c.Accounts)
	}
}
