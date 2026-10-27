// Package routing decides which configured accounts receive an observation. Routing uses only verified scope
// (the installation's account, or the account whose credential collected the data) plus explicit shares; it never
// infers routing from actors or other payload content.
package routing

import (
	"slices"
	"strings"
	"sync"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
)

// Router maps GitHub identities to configured accounts and applies shares and repository filters.
type Router struct {
	accounts map[string]*config.Account
	order    []string
	shares   []config.Share

	mu         sync.RWMutex
	byGitHubID map[int64]string
	ids        map[string]int64
	logins     map[string]string
}

// New builds a router from configuration. Configured github_id values are bound immediately.
func New(cfg *config.Config) *Router {
	r := &Router{
		accounts:   map[string]*config.Account{},
		shares:     cfg.Routing.Shares,
		byGitHubID: map[int64]string{},
		ids:        map[string]int64{},
		logins:     map[string]string{},
	}
	for i := range cfg.Accounts {
		a := &cfg.Accounts[i]
		r.accounts[a.Key] = a
		r.order = append(r.order, a.Key)
		r.logins[a.Key] = a.Login
		if a.GitHubID != 0 {
			r.byGitHubID[a.GitHubID] = a.Key
			r.ids[a.Key] = a.GitHubID
		}
	}
	return r
}

// Keys lists configured account keys in configuration order.
func (r *Router) Keys() []string { return slices.Clone(r.order) }

// Account returns a configured account.
func (r *Router) Account(key string) (*config.Account, bool) {
	a, ok := r.accounts[key]
	return a, ok
}

// Bind records the immutable GitHub ID (and current login) of an account. It refuses to rebind a key or an ID.
func (r *Router) Bind(key string, githubID int64, login string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.accounts[key]; !ok || githubID == 0 {
		return false
	}
	if cur, ok := r.ids[key]; ok && cur != githubID {
		return false
	}
	if other, ok := r.byGitHubID[githubID]; ok && other != key {
		return false
	}
	r.ids[key] = githubID
	r.byGitHubID[githubID] = key
	if login != "" {
		r.logins[key] = login
	}
	return true
}

// AccountForGitHubID returns the account bound to a GitHub account ID.
func (r *Router) AccountForGitHubID(id int64) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k, ok := r.byGitHubID[id]
	return k, ok
}

// UnboundAccountForLogin returns a configured account that has no GitHub ID bound yet and whose configured login
// matches (case-insensitively). It is used once, to pin the ID from an authenticated source (App API, /user).
func (r *Router) UnboundAccountForLogin(login string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, k := range r.order {
		if _, bound := r.ids[k]; bound {
			continue
		}
		if strings.EqualFold(r.accounts[k].Login, login) {
			return k, true
		}
	}
	return "", false
}

// Ref returns the payload account reference.
func (r *Router) Ref(key string) events.AccountRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a := r.accounts[key]
	return events.AccountRef{Key: key, GitHubID: r.ids[key], Login: r.logins[key], Type: string(a.Type)}
}

// Target is one account an observation is published to.
type Target struct {
	Account events.AccountRef
	Routing events.Routing
}

// Route returns the targets for an observation collected in primary's scope. match is how primary was determined
// (events.MatchInstallation or events.MatchCredential). repo is "owner/name" or "".
func (r *Router) Route(primary, match, source, repo string) []Target {
	var keys []string
	routing := map[string]events.Routing{}
	if a, ok := r.accounts[primary]; ok && sourceEnabled(a, source) && (repo == "" || a.Repositories.Allows(repo)) {
		keys = append(keys, primary)
		routing[primary] = events.Routing{Match: match}
	}
	for _, s := range r.shares {
		if s.From != primary || !slices.Contains(s.Sources, source) {
			continue
		}
		to, ok := r.accounts[s.To]
		if !ok || slices.Contains(keys, s.To) {
			continue
		}
		// A share restricted to repositories never carries repository-less events (organization, membership,
		// installation activity); an unrestricted share carries everything of its sources.
		if repo == "" && (len(s.Repositories.Allow) > 0 || len(to.Repositories.Allow) > 0) {
			continue
		}
		if repo != "" && (!s.Repositories.Allows(repo) || !to.Repositories.Allows(repo)) {
			continue
		}
		keys = append(keys, s.To)
		routing[s.To] = events.Routing{Match: events.MatchShare, SharedFrom: primary}
	}
	out := make([]Target, 0, len(keys))
	for _, k := range keys {
		rt := routing[k]
		for _, o := range keys {
			if o != k {
				rt.AlsoRouted = append(rt.AlsoRouted, o)
			}
		}
		out = append(out, Target{Account: r.Ref(k), Routing: rt})
	}
	return out
}

func sourceEnabled(a *config.Account, source string) bool {
	if source == events.SourceWebhook {
		return a.Webhook.Enabled
	}
	return true
}
