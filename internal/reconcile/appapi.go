// Package reconcile discovers installations and repository access, takes bounded resource snapshots, and runs
// resumable backfills. Snapshots describe current state; they are never presented as historical events.
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/aurimasniekis/go-pulse-github/internal/collectors/webhook"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// AppAPI reads installation metadata with the App JWT.
type AppAPI struct {
	Client *githubapi.Client
	Cred   githubapi.Credential
}

// Installation implements webhook.InstallationSource. A 404 with a valid App JWT means the installation is gone.
func (a *AppAPI) Installation(ctx context.Context, id int64) (*storage.Installation, error) {
	resp, err := a.Client.Do(ctx, a.Cred, githubapi.Request{URL: "/app/installations/" + strconv.FormatInt(id, 10)})
	if err != nil {
		if githubapi.IsClass(err, githubapi.ClassNotFound) {
			return nil, webhook.ErrInstallationGone
		}
		return nil, err
	}
	return webhook.ParseInstallation(resp.Body)
}

// Listed is an installation with GitHub's original JSON object.
type Listed struct {
	*storage.Installation
	Raw json.RawMessage
}

// Installations lists every installation of the App.
func (a *AppAPI) Installations(ctx context.Context) ([]Listed, error) {
	var out []Listed
	next := "/app/installations?per_page=100"
	for pages := 0; next != "" && pages < 100; pages++ {
		resp, err := a.Client.Do(ctx, a.Cred, githubapi.Request{URL: next})
		if err != nil {
			return nil, err
		}
		items, err := githubapi.SplitArray(resp.Body)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			in, err := webhook.ParseInstallation(it)
			if err != nil {
				return nil, err
			}
			out = append(out, Listed{Installation: in, Raw: it})
		}
		next = resp.Next
	}
	if next != "" {
		return out, errors.New("installation listing exceeded 100 pages")
	}
	return out, nil
}
