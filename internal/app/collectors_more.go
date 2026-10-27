package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aurimasniekis/go-pulse-github/internal/collectors/activity"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/reconcile"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

func (s *Service) addActivityAndReconcile(ctx context.Context) error {
	for _, key := range s.Router.Keys() {
		a, _ := s.Router.Account(key)
		if a.Activity.Enabled {
			var feeds []activity.Feed
			for _, f := range a.Activity.Feeds {
				var cred githubapi.Credential
				if f.Credential == config.CredentialInstallation {
					cred = s.installationCred(key)
				} else {
					cred = s.tokenCred(key)
				}
				feeds = append(feeds, activity.Feed{Config: f, Cred: cred})
			}
			s.add("activity:"+key, &activity.Poller{Account: a, Feeds: feeds, DB: s.DB, Router: s.Router, Capturer: s.Capturer,
				GitHub: s.GitHub, Pressure: s.Guard, Log: s.Log, Metrics: s.Metrics, Now: s.Now, Published: s.Dispatcher.Notify})
		}
	}
	return s.addReconcile(ctx)
}

// installationCred authenticates as the App installation on the account. The installation is looked up on use, so
// it may be discovered (by webhook or reconciliation) after start.
func (s *Service) installationCred(key string) githubapi.Credential {
	return &lazyInstallation{s: s, key: key}
}

type lazyInstallation struct {
	s   *Service
	key string

	mu sync.Mutex
	id int64
}

func (l *lazyInstallation) Name() string { return "installation:" + l.key }

func (l *lazyInstallation) resolve(ctx context.Context) (githubapi.Credential, error) {
	if l.s.Tokens == nil {
		return nil, errors.New("github.app is not enabled")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.id == 0 {
		id, err := l.s.InstallationFor(ctx, l.key)
		if err != nil {
			return nil, err
		}
		l.id = id
	}
	return l.s.Tokens.Credential(l.id, l.Name()), nil
}

func (l *lazyInstallation) Authorization(ctx context.Context) (string, error) {
	c, err := l.resolve(ctx)
	if err != nil {
		return "", err
	}
	return c.Authorization(ctx)
}

func (l *lazyInstallation) Invalidate() {
	l.mu.Lock()
	id := l.id
	l.id = 0
	l.mu.Unlock()
	if id != 0 && l.s.Tokens != nil {
		l.s.Tokens.Invalidate(id)
	}
}

// InstallationFor returns the active installation on an account.
func (s *Service) InstallationFor(ctx context.Context, key string) (int64, error) {
	id := s.Router.Ref(key).GitHubID
	if id == 0 {
		return 0, fmt.Errorf("account %s has no known GitHub ID yet (no installation or identity seen)", key)
	}
	insts, err := s.DB.Installations(ctx)
	if err != nil {
		return 0, err
	}
	for _, in := range insts {
		if in.AccountGitHubID == id && in.Status == storage.InstallActive {
			return in.ID, nil
		}
	}
	return 0, fmt.Errorf("no active App installation on account %s", key)
}

var _ githubapi.Credential = (*lazyInstallation)(nil)

// addReconcile registers installation reconciliation and the snapshot job runner (which also serves backfill jobs
// queued by `pulse-github backfill`). Start-up backfill is queued once per account, not on every restart.
func (s *Service) addReconcile(ctx context.Context) error {
	if s.AppAPI == nil {
		return nil
	}
	s.add("reconcile_installations", &reconcile.Installations{App: s.AppAPI, Tokens: s.Tokens, DB: s.DB, Router: s.Router,
		Capturer: s.Capturer, Interval: s.Cfg.GitHub.App.ReconcileInterval.D(), Log: s.Log, Metrics: s.Metrics, Now: s.Now,
		Bind: s.bindAccount, Published: s.Dispatcher.Notify,
		Requeue: func(ctx context.Context) {
			if n, err := s.DB.RequeueUnrouted(ctx, s.requeueLimit()); err == nil && n > 0 && s.Processor != nil {
				s.Processor.Notify()
			}
		}})
	for _, key := range s.Router.Keys() {
		a, _ := s.Router.Account(key)
		if !a.Reconcile.BackfillOnStartup {
			continue
		}
		if _, err := s.DB.LastJob(ctx, key, reconcile.ModeBackfill); errors.Is(err, storage.ErrNotFound) {
			if _, _, err := reconcile.EnqueueBackfill(ctx, s.DB, a, s.Now().Add(-a.Reconcile.BackfillLookback.D()), nil, 0, s.Now()); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	s.add("reconcile_snapshots", &reconcile.Snapshots{DB: s.DB, Router: s.Router, Capturer: s.Capturer, GitHub: s.GitHub,
		Tokens: s.Tokens, Installation: s.InstallationFor, Pressure: s.Guard, Log: s.Log, Metrics: s.Metrics, Now: s.Now,
		Published: s.Dispatcher.Notify})
	return nil
}
