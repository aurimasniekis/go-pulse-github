package app

import (
	"github.com/aurimasniekis/go-pulse-github/internal/collectors/notifications"
	"github.com/aurimasniekis/go-pulse-github/internal/enrich"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
)

// tokenCred returns the shared PAT credential of an account (nil when none is configured).
func (s *Service) tokenCred(key string) githubapi.Credential {
	if s.pats == nil {
		s.pats = map[string]*githubauth.PAT{}
	}
	if p, ok := s.pats[key]; ok {
		return p
	}
	a, ok := s.Router.Account(key)
	if !ok || a.Token.IsZero() {
		return nil
	}
	p := githubauth.NewPAT("token:"+key, a.Token)
	s.pats[key] = p
	return p
}

// addCollectors registers polling collectors, enrichment and reconciliation for configured accounts.
func (s *Service) addCollectors() error {
	enrichAccounts := map[string]enrich.Account{}
	for _, key := range s.Router.Keys() {
		a, _ := s.Router.Account(key)
		if a.Notifications.Enabled {
			s.add("notifications:"+key, &notifications.Poller{
				Account: a, DB: s.DB, Router: s.Router, Capturer: s.Capturer, GitHub: s.GitHub, Cred: s.tokenCred(key),
				Pressure: s.Guard, Log: s.Log, Metrics: s.Metrics, Now: s.Now, Bind: s.bindAccount,
				Published: s.Dispatcher.Notify,
			})
			if a.Notifications.Enrichment.Enabled {
				enrichAccounts[key] = enrich.Account{Cred: s.tokenCred(key), MaxPerHour: a.Notifications.Enrichment.MaxPerHour}
			}
		}
	}
	if len(enrichAccounts) > 0 {
		s.add("enrichment", &enrich.Worker{DB: s.DB, Router: s.Router, Capturer: s.Capturer, GitHub: s.GitHub,
			Accounts: enrichAccounts, Pressure: s.Guard, Log: s.Log, Metrics: s.Metrics, Now: s.Now,
			Published: s.Dispatcher.Notify})
	}
	return s.addActivityAndReconcile()
}
