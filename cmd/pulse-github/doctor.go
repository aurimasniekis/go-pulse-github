package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/aurimasniekis/go-pulse-github/internal/app"
	"github.com/aurimasniekis/go-pulse-github/internal/buildinfo"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Event subscriptions the App should have for broad coverage. Missing ones are reported as warnings only: an App
// may deliberately subscribe to fewer events.
var wantedEvents = []string{"issues", "issue_comment", "pull_request", "pull_request_review", "pull_request_review_comment",
	"pull_request_review_thread", "push", "create", "delete", "release", "discussion", "discussion_comment", "workflow_run",
	"workflow_job", "check_run", "check_suite", "deployment", "deployment_status", "star", "watch", "fork", "repository",
	"member", "membership", "organization", "team"}

type doctorReport struct {
	w     *tabwriter.Writer
	fails int
	warns int
}

func (r *doctorReport) add(status, check, detail string) {
	switch status {
	case "FAIL":
		r.fails++
	case "WARN":
		r.warns++
	}
	fmt.Fprintf(r.w, "%s\t%s\t%s\n", status, check, strings.ReplaceAll(detail, "\n", " "))
}

func newDoctorCmd(f *rootFlags) *cobra.Command {
	var testPublish bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check configuration, storage, GitHub credentials and permissions, and Pulse connectivity (read-only)",
		Long: "Runs read-only checks. It never changes GitHub state (no notifications are marked read, nothing is " +
			"subscribed). It publishes to Pulse only with --test-publish, to evt.<domain>.<first account>.collector.doctor.test.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			r := &doctorReport{w: tabwriter.NewWriter(cmd.OutOrStdout(), 2, 4, 2, ' ', 0)}
			fmt.Fprintln(r.w, "STATUS\tCHECK\tDETAIL")
			cfg, err := f.load()
			if err != nil {
				r.add("FAIL", "config", err.Error())
				_ = r.w.Flush()
				return exitCode(1)
			}
			r.add("OK", "config", fmt.Sprintf("%d account(s), pulse-github %s, pulse sdk %s", len(cfg.Accounts), buildinfo.Version,
				buildinfo.PulseSDKVersion()))
			if err := checkSecrets(cfg); err != nil {
				r.add("FAIL", "secrets", err.Error())
			} else {
				r.add("OK", "secrets", "every secret reference resolves")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()
			doctorStorage(ctx, cfg, r)
			doctorPulse(ctx, cfg, r, testPublish)
			doctorGitHub(ctx, cfg, r)
			_ = r.w.Flush()
			fmt.Fprintf(cmd.OutOrStdout(), "\n%d failure(s), %d warning(s)\n", r.fails, r.warns)
			if r.fails > 0 {
				return exitCode(1)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&testPublish, "test-publish", false, "publish one test event to Pulse and wait for its acknowledgement")
	return cmd
}

func doctorStorage(ctx context.Context, cfg *config.Config, r *doctorReport) {
	dir := filepath.Dir(cfg.Storage.Path)
	free, err := storage.FreeBytes(dir)
	switch {
	case err != nil:
		r.add("FAIL", "storage", fmt.Sprintf("state directory %s: %v", dir, err))
		return
	case free < uint64(cfg.Storage.MinFreeBytes): //nolint:gosec // validated non-negative
		r.add("FAIL", "storage", fmt.Sprintf("%d bytes free, below storage.min_free_bytes %d", free, cfg.Storage.MinFreeBytes))
	default:
		r.add("OK", "storage", fmt.Sprintf("%s: %d MiB free", dir, free>>20))
	}
	if _, err := os.Stat(cfg.Storage.Path); errors.Is(err, os.ErrNotExist) {
		probe := filepath.Join(dir, ".pulse-github-doctor")
		if err := os.WriteFile(probe, nil, 0o600); err != nil {
			r.add("FAIL", "storage", "state directory is not writable: "+err.Error())
			return
		}
		_ = os.Remove(probe)
		r.add("OK", "database", "not created yet; `run` creates it")
		return
	}
	db, err := storage.Open(ctx, cfg.Storage.Path, storage.Options{ReadOnly: true})
	if err != nil {
		r.add("FAIL", "database", err.Error())
		return
	}
	defer db.Close() //nolint:errcheck,contextcheck // Close checkpoints the WAL on its own context
	v, _ := db.SchemaVersion(ctx)
	ob, _ := db.OutboxStats(ctx)
	q, _ := db.QuarantineCount(ctx)
	status := "OK"
	if q > 0 {
		status = "WARN"
	}
	r.add(status, "database", fmt.Sprintf("schema %s, %d MiB, %d pending publications, %d quarantined", v, db.SizeBytes()>>20, ob.Pending, q))
	if l, err := storage.AcquireLock(cfg.Storage.Path); errors.Is(err, storage.ErrLocked) {
		r.add("OK", "process lock", "held by a running service")
	} else if err == nil {
		_ = l.Release()
		r.add("WARN", "process lock", "no running service holds the database")
	}
}

func doctorPulse(ctx context.Context, cfg *config.Config, r *doctorReport, testPublish bool) {
	if _, err := os.ReadFile(cfg.Pulse.CredsFile); err != nil {
		r.add("FAIL", "pulse creds", err.Error())
		return
	}
	p, err := publisher.Connect(publisher.Options{URL: cfg.Pulse.URL, CredsFile: cfg.Pulse.CredsFile, CAFile: cfg.Pulse.CAFile,
		Source: cfg.Service.Source, Name: cfg.Service.Source + "-doctor", Logger: observability.NewLogger(io.Discard, "error", "json", "")})
	if err != nil {
		r.add("FAIL", "pulse", err.Error())
		return
	}
	defer p.Close()
	deadline := time.Now().Add(10 * time.Second)
	for !p.Connected() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !p.Connected() {
		r.add("FAIL", "pulse", "could not connect to "+cfg.Pulse.URL+" within 10s")
		return
	}
	r.add("OK", "pulse", fmt.Sprintf("connected to %s as client %s (max_payload %d)", cfg.Pulse.URL, p.Client().ClientID(), p.MaxPayload()))
	if !testPublish {
		r.add("OK", "pulse publish", "skipped (use --test-publish to publish one test event)")
		return
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	subject := "evt." + cfg.Pulse.SubjectDomain + "." + cfg.Accounts[0].Key + ".collector.doctor.test"
	pctx, cancel := context.WithTimeout(ctx, cfg.Pulse.PublishTimeout.D())
	defer cancel()
	ack, err := p.Publish(pctx, publisher.Message{ID: "doctor-" + hex.EncodeToString(b), Subject: subject,
		Type: "github.collector.doctor.test.v1", Time: time.Now(), Body: []byte(`{"schema_version":1,"doctor":true}`)})
	if err != nil {
		class, _ := publisher.Classify(err)
		hint := ""
		if class == publisher.ClassTimeout {
			hint = " (Pulse reports a missing publish grant as a timeout: the client needs publish:evt." + cfg.Pulse.SubjectDomain + ".>)"
		}
		r.add("FAIL", "pulse publish", err.Error()+hint)
		return
	}
	r.add("OK", "pulse publish", fmt.Sprintf("%s acknowledged by stream %s seq %d", subject, ack.Stream, ack.Sequence))
}

func doctorGitHub(ctx context.Context, cfg *config.Config, r *doctorReport) {
	hc, err := app.GitHubHTTPClient(cfg.GitHub)
	if err != nil {
		r.add("FAIL", "github", err.Error())
		return
	}
	gh, err := githubapi.New(githubapi.Options{BaseURL: cfg.GitHub.APIURL, APIVersion: cfg.GitHub.APIVersion, HTTPClient: hc,
		UserAgent: "pulse-github-doctor/" + buildinfo.Version, Logger: observability.NewLogger(io.Discard, "error", "json", ""),
		MaxInlineWait: time.Second})
	if err != nil {
		r.add("FAIL", "github", err.Error())
		return
	}
	if cfg.GitHub.App.Enabled {
		doctorApp(ctx, cfg, gh, r)
	}
	for _, a := range cfg.Accounts {
		if a.Token.IsZero() {
			continue
		}
		cred := githubauth.NewPAT("token:"+a.Key, a.Token)
		id, err := gh.AuthenticatedUser(ctx, cred)
		check := "token " + a.Key
		switch {
		case err != nil:
			r.add("FAIL", check, githubapi.ClassOf(err)+": "+err.Error())
			continue
		case !strings.EqualFold(id.Login, a.Login) && a.Type == config.AccountUser:
			r.add("FAIL", check, fmt.Sprintf("authenticates as %s, account is %s", id.Login, a.Login))
			continue
		}
		scopes := strings.Join(id.Scopes, ",")
		if a.Notifications.Enabled {
			switch {
			case !id.Classic:
				r.add("FAIL", check, "notifications need a classic personal access token (fine-grained and App tokens are not supported by GET /notifications)")
			case !id.HasScope("notifications") && !id.HasScope("repo"):
				r.add("FAIL", check, "token lacks the notifications or repo scope (has "+scopes+")")
			default:
				r.add("OK", check, fmt.Sprintf("%s (id %d), classic token, scopes %s", id.Login, id.ID, scopes))
			}
		} else {
			r.add("OK", check, fmt.Sprintf("%s (id %d), scopes %q", id.Login, id.ID, scopes))
		}
		for _, f := range a.Activity.Feeds {
			if !a.Activity.Enabled || f.Credential != config.CredentialToken {
				continue
			}
			u := feedProbeURL(a, f)
			if _, err := gh.Do(ctx, cred, githubapi.Request{URL: u}); err != nil {
				status := "FAIL"
				if f.Kind == config.FeedAuditLog {
					status = "WARN"
				}
				r.add(status, "feed "+a.Key+" "+f.ID(), githubapi.ClassOf(err)+": "+err.Error())
			} else {
				r.add("OK", "feed "+a.Key+" "+f.ID(), "readable")
			}
		}
	}
}

func feedProbeURL(a config.Account, f config.FeedConfig) string {
	switch f.Kind {
	case config.FeedAuditLog:
		return "/orgs/" + f.Org + "/audit-log?per_page=1"
	case config.FeedUserEvents:
		return "/users/" + a.Login + "/events?per_page=1"
	case config.FeedReceivedEvents:
		return "/users/" + a.Login + "/received_events?per_page=1"
	case config.FeedUserOrgEvents:
		return "/users/" + a.Login + "/events/orgs/" + f.Org + "?per_page=1"
	case config.FeedOrgEvents:
		return "/orgs/" + f.Org + "/events?per_page=1"
	default:
		return "/repos/" + f.Repo + "/events?per_page=1"
	}
}

func doctorApp(ctx context.Context, cfg *config.Config, gh *githubapi.Client, r *doctorReport) {
	pem, err := cfg.GitHub.App.PrivateKey.Resolve()
	if err != nil {
		r.add("FAIL", "github app", err.Error())
		return
	}
	key, err := githubauth.ParsePrivateKey(pem)
	if err != nil {
		r.add("FAIL", "github app", err.Error())
		return
	}
	ghApp := githubauth.NewApp(cfg.GitHub.App.AppID, cfg.GitHub.App.ClientID, key, nil)
	resp, err := gh.Do(ctx, ghApp.Credential(), githubapi.Request{URL: "/app"})
	if err != nil {
		r.add("FAIL", "github app", "JWT rejected or App unreachable: "+err.Error())
		return
	}
	var meta struct {
		Slug        string            `json:"slug"`
		Events      []string          `json:"events"`
		Permissions map[string]string `json:"permissions"`
	}
	_ = json.Unmarshal(resp.Body, &meta)
	r.add("OK", "github app", fmt.Sprintf("%s, %d permission(s), %d event subscription(s)", meta.Slug, len(meta.Permissions), len(meta.Events)))
	var missing []string
	for _, e := range wantedEvents {
		found := false
		for _, have := range meta.Events {
			found = found || have == e
		}
		if !found {
			missing = append(missing, e)
		}
	}
	if len(missing) > 0 {
		r.add("WARN", "github app events", "not subscribed (no webhooks for these): "+strings.Join(missing, ", "))
	}
	resp, err = gh.Do(ctx, ghApp.Credential(), githubapi.Request{URL: "/app/installations?per_page=100"})
	if err != nil {
		r.add("FAIL", "installations", err.Error())
		return
	}
	var insts []struct {
		ID          int64                  `json:"id"`
		Account     struct{ Login string } `json:"account"`
		Selection   string                 `json:"repository_selection"`
		SuspendedAt *string                `json:"suspended_at"`
		Permissions map[string]string      `json:"permissions"`
	}
	_ = json.Unmarshal(resp.Body, &insts)
	for _, a := range cfg.Accounts {
		found := false
		for _, in := range insts {
			if !strings.EqualFold(in.Account.Login, a.Login) {
				continue
			}
			found = true
			status, detail := "OK", fmt.Sprintf("installation %d, %s repositories, %d permission(s)", in.ID, in.Selection, len(in.Permissions))
			if in.SuspendedAt != nil {
				status, detail = "FAIL", "installation "+strconv.FormatInt(in.ID, 10)+" is suspended"
			}
			r.add(status, "installation "+a.Key, detail)
		}
		if !found && (a.Webhook.Enabled || a.Reconcile.Enabled) {
			r.add("FAIL", "installation "+a.Key, "the App is not installed on "+a.Login+" (webhooks and reconciliation need it)")
		}
	}
}
