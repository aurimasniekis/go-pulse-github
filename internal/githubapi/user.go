package githubapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Identity is the authenticated user of a token.
type Identity struct {
	ID     int64
	Login  string
	Type   string
	Scopes []string
	// Classic is true when GitHub reported OAuth scopes (classic personal access tokens and OAuth tokens). Fine-grained
	// tokens and GitHub App tokens carry no X-OAuth-Scopes header.
	Classic bool
}

// HasScope reports whether a classic token carries a scope.
func (i *Identity) HasScope(s string) bool {
	for _, x := range i.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

// AuthenticatedUser calls GET /user. It never changes GitHub state.
func (c *Client) AuthenticatedUser(ctx context.Context, cred Credential) (*Identity, error) {
	resp, err := c.Do(ctx, cred, Request{URL: "/user"})
	if err != nil {
		return nil, err
	}
	var u struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
		Type  string `json:"type"`
	}
	if err := json.Unmarshal(resp.Body, &u); err != nil || u.ID == 0 {
		return nil, fmt.Errorf("decode /user: %w", err)
	}
	id := &Identity{ID: u.ID, Login: u.Login, Type: u.Type}
	if h, ok := resp.Header["X-Oauth-Scopes"]; ok {
		id.Classic = true
		for _, s := range strings.Split(strings.Join(h, ","), ",") {
			if s = strings.TrimSpace(s); s != "" {
				id.Scopes = append(id.Scopes, s)
			}
		}
	}
	return id, nil
}
