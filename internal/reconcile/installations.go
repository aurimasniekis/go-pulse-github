package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// TokenSource issues installation credentials.
type TokenSource interface {
	Credential(id int64, name string) githubapi.Credential
}

// Installations periodically reconciles App installations and their repository access with GitHub. Webhooks deliver
// these changes live; reconciliation repairs what was missed while the receiver was down. Changes it detects are
// inferred differences, published as reconcile.installation*.* events with provenance.inferred = true.
type Installations struct {
	App       *AppAPI
	Tokens    TokenSource
	DB        *storage.DB
	Router    *routing.Router
	Capturer  *capture.Capturer
	Interval  time.Duration
	Log       *slog.Logger
	Metrics   *observability.Metrics
	Now       func() time.Time
	Bind      func(ctx context.Context, key string, id int64, login, typ string) error
	Published func()
	// Requeue reprocesses unrouted webhook deliveries after installations changed.
	Requeue func(ctx context.Context)
}

// Run reconciles at start and then every Interval.
func (r *Installations) Run(ctx context.Context) error {
	for {
		if err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.Log.Warn("installation reconciliation failed", observability.FieldComponent, "reconcile_installations",
				observability.FieldErrorClass, githubapi.ClassOf(err), observability.Err(err))
		}
		_ = collectors.Sleep(ctx, collectors.Jitter(r.Interval))
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// RunOnce lists the App's installations and each mapped installation's repositories.
func (r *Installations) RunOnce(ctx context.Context) error {
	listed, err := r.App.Installations(ctx)
	if err != nil {
		return err
	}
	now := r.Now()
	stored, err := r.DB.Installations(ctx)
	if err != nil {
		return err
	}
	seen := map[int64]bool{}
	changed := false
	for _, in := range listed {
		seen[in.ID] = true
		key := r.accountFor(ctx, in.Installation)
		var prev *storage.Installation
		for i := range stored {
			if stored[i].ID == in.ID {
				prev = &stored[i]
			}
		}
		err := r.DB.Tx(ctx, func(tx *storage.Tx) error {
			if err := tx.UpsertInstallation(ctx, *in.Installation, now); err != nil {
				return err
			}
			if key != "" && (prev == nil || prev.Status != in.Status) {
				changed = true
				return r.publish(ctx, tx, key, "installation", statusAction(prev, in.Status), in.ID, in.Raw)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	// The App's own installation listing is authoritative: an installation missing from it is uninstalled.
	for _, in := range stored {
		if seen[in.ID] || in.Status == storage.InstallRemoved {
			continue
		}
		key, _ := r.Router.AccountForGitHubID(in.AccountGitHubID)
		changed = true
		err := r.DB.Tx(ctx, func(tx *storage.Tx) error {
			if err := tx.SetInstallationStatus(ctx, in.ID, storage.InstallRemoved, now); err != nil {
				return err
			}
			if key == "" {
				return nil
			}
			if err := r.Capturer.AccessChange(ctx, tx, r.Router, key, "webhook", "installation:"+strconv.FormatInt(in.ID, 10),
				true, "installation_removed", "the App installation is no longer listed"); err != nil {
				return err
			}
			body, _ := json.Marshal(map[string]any{"id": in.ID, "account": map[string]any{"id": in.AccountGitHubID,
				"login": in.AccountLogin, "type": in.AccountType}})
			return r.publish(ctx, tx, key, "installation", "removed", in.ID, body)
		})
		if err != nil {
			return err
		}
	}
	for _, in := range listed {
		key, ok := r.Router.AccountForGitHubID(in.AccountGitHubID)
		if !ok || in.Status != storage.InstallActive {
			continue
		}
		c, err := r.repositories(ctx, key, in.Installation)
		if err != nil {
			r.Log.Warn("installation repository reconciliation failed", observability.FieldAccount, key,
				observability.FieldInstallation, in.ID, observability.Err(err))
			continue
		}
		changed = changed || c
	}
	if changed {
		if r.Requeue != nil {
			r.Requeue(ctx)
		}
		if r.Published != nil {
			r.Published()
		}
	}
	return nil
}

func statusAction(prev *storage.Installation, status string) string {
	switch {
	case prev == nil:
		return "snapshot"
	case status == storage.InstallSuspended:
		return "suspended"
	case status == storage.InstallActive:
		return "reactivated"
	default:
		return status
	}
}

func (r *Installations) accountFor(ctx context.Context, in *storage.Installation) string {
	if key, ok := r.Router.AccountForGitHubID(in.AccountGitHubID); ok {
		return key
	}
	key, ok := r.Router.UnboundAccountForLogin(in.AccountLogin)
	if !ok {
		return ""
	}
	if r.Bind != nil && r.Bind(ctx, key, in.AccountGitHubID, in.AccountLogin, in.AccountType) != nil {
		return ""
	}
	if !r.Router.Bind(key, in.AccountGitHubID, in.AccountLogin) {
		return ""
	}
	return key
}

// repositories diffs the installation's accessible repositories against the stored set.
func (r *Installations) repositories(ctx context.Context, key string, in *storage.Installation) (bool, error) {
	cred := r.Tokens.Credential(in.ID, "installation:"+key)
	type repo struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
		Private  bool   `json:"private"`
	}
	current := map[int64]json.RawMessage{}
	meta := map[int64]repo{}
	next := "/installation/repositories?per_page=100"
	for pages := 0; next != ""; pages++ {
		if pages >= 200 {
			return false, fmt.Errorf("installation %d lists more than 20000 repositories", in.ID)
		}
		resp, err := r.App.Client.Do(ctx, cred, githubapi.Request{URL: next})
		if err != nil {
			return false, err
		}
		var page struct {
			Repositories []json.RawMessage `json:"repositories"`
		}
		if err := json.Unmarshal(resp.Body, &page); err != nil {
			return false, fmt.Errorf("decode installation repositories: %w", err)
		}
		for _, raw := range page.Repositories {
			var m repo
			if err := json.Unmarshal(raw, &m); err != nil || m.ID == 0 {
				return false, fmt.Errorf("decode repository: %w", err)
			}
			current[m.ID], meta[m.ID] = raw, m
		}
		next = resp.Next
	}
	stored, err := r.DB.InstallationRepos(ctx, in.ID)
	if err != nil {
		return false, err
	}
	now := r.Now()
	first := len(stored) == 0
	changed := false
	err = r.DB.Tx(ctx, func(tx *storage.Tx) error {
		known := map[int64]storage.InstallationRepo{}
		for _, s := range stored {
			known[s.RepoID] = s
		}
		for id, raw := range current {
			m := meta[id]
			s, ok := known[id]
			if ok && s.Status == storage.InstallActive && s.FullName == m.FullName {
				continue
			}
			if err := tx.SetInstallationRepo(ctx, storage.InstallationRepo{InstallationID: in.ID, RepoID: id, FullName: m.FullName,
				Private: m.Private, Status: storage.InstallActive}, now); err != nil {
				return err
			}
			action := "added"
			switch {
			case first:
				action = "snapshot"
			case ok && s.Status == storage.InstallActive:
				action = "renamed"
			}
			changed = true
			if err := r.publishRepo(ctx, tx, key, action, in.ID, m.FullName, raw); err != nil {
				return err
			}
		}
		for _, s := range stored {
			if _, ok := current[s.RepoID]; ok || s.Status != storage.InstallActive {
				continue
			}
			// Lost access, not deletion: the repository may still exist outside this installation's scope.
			s.Status = storage.InstallRemoved
			if err := tx.SetInstallationRepo(ctx, s, now); err != nil {
				return err
			}
			changed = true
			body, _ := json.Marshal(map[string]any{"id": s.RepoID, "full_name": s.FullName})
			if err := r.publishRepo(ctx, tx, key, "access_removed", in.ID, s.FullName, body); err != nil {
				return err
			}
		}
		return tx.MarkInstallationReconciled(ctx, in.ID, now)
	})
	return changed, err
}

func (r *Installations) publish(ctx context.Context, tx *storage.Tx, key, event, action string, instID int64, raw []byte) error {
	now := r.Now()
	d := &events.Draft{
		Source: events.SourceReconcile, Event: event, Action: action,
		DedupKey:   fmt.Sprintf("reconcile:%s:%s:%d:%s:%d", key, event, instID, action, now.UnixNano()),
		ObservedAt: now, Mode: events.ModeReconciliation,
		SourceRef:  events.SourceRef{InstallationID: instID},
		GitHub:     events.GitHubRef{Event: event},
		Provenance: events.Provenance{Snapshot: true, Inferred: action != "snapshot"},
		Raw:        raw,
	}
	_, err := r.Capturer.Store(ctx, tx, d, r.Router.Route(key, events.MatchInstallation, events.SourceReconcile, ""), 0)
	return err
}

func (r *Installations) publishRepo(ctx context.Context, tx *storage.Tx, key, action string, instID int64, fullName string, raw []byte) error {
	now := r.Now()
	d := &events.Draft{
		Source: events.SourceReconcile, Event: "installation_repository", Action: action,
		DedupKey:   fmt.Sprintf("reconcile:%s:installation_repository:%d:%s:%s:%d", key, instID, fullName, action, now.UnixNano()),
		ObservedAt: now, Mode: events.ModeReconciliation,
		SourceRef:  events.SourceRef{InstallationID: instID},
		GitHub:     events.GitHubRef{Event: "installation_repository"},
		Repository: &events.Repository{FullName: fullName},
		Provenance: events.Provenance{Snapshot: true, Inferred: action != "snapshot"},
		Raw:        raw,
	}
	_, err := r.Capturer.Store(ctx, tx, d, r.Router.Route(key, events.MatchInstallation, events.SourceReconcile, fullName), 0)
	return err
}
