package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aurimasniekis/go-pulse-github/internal/collectors/activity"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

func (s *Service) addActivityAndReconcile() error {
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
	return s.addReconcile()
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

func (s *Service) addReconcile() error { return nil }
