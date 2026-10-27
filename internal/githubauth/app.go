package githubauth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
)

// ParsePrivateKey parses a PEM RSA private key (PKCS#1, as GitHub issues it, or PKCS#8).
func ParsePrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("private key is not PEM")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return rk, nil
}

// App signs GitHub App JWTs (RS256, iat 60 s in the past, exp 9 minutes ahead; GitHub allows at most 10).
type App struct {
	issuer string
	key    *rsa.PrivateKey
	now    func() time.Time

	mu     sync.Mutex
	cached string
	exp    time.Time
}

// NewApp builds an App signer. The client ID is the recommended issuer; the numeric App ID also works.
func NewApp(appID int64, clientID string, key *rsa.PrivateKey, now func() time.Time) *App {
	iss := clientID
	if iss == "" {
		iss = strconv.FormatInt(appID, 10)
	}
	if now == nil {
		now = time.Now
	}
	return &App{issuer: iss, key: key, now: now}
}

// JWT returns a valid App JWT, reusing it until a minute before expiry.
func (a *App) JWT() (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if a.cached != "" && now.Before(a.exp.Add(-time.Minute)) {
		return a.cached, nil
	}
	iat, exp := now.Add(-60*time.Second), now.Add(9*time.Minute)
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"iat": iat.Unix(), "exp": exp.Unix(), "iss": a.issuer})
	if err != nil {
		return "", err
	}
	signing := hdr + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, a.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	a.cached, a.exp = signing+"."+base64.RawURLEncoding.EncodeToString(sig), exp
	return a.cached, nil
}

// Credential returns the App-level credential (JWT bearer) for /app endpoints.
func (a *App) Credential() githubapi.Credential { return appCred{a} }

type appCred struct{ a *App }

func (appCred) Name() string { return "app" }
func (c appCred) Authorization(context.Context) (string, error) {
	t, err := c.a.JWT()
	if err != nil {
		return "", err
	}
	return "Bearer " + t, nil
}
func (c appCred) Invalidate() {
	c.a.mu.Lock()
	c.a.cached = ""
	c.a.mu.Unlock()
}

// Installations issues and caches installation access tokens. Tokens are refreshed early (5 minutes before expiry)
// and concurrent refreshes of one installation are coalesced. Tokens are opaque and only held in memory.
type Installations struct {
	client *githubapi.Client
	app    *App
	now    func() time.Time
	early  time.Duration

	mu    sync.Mutex
	cache map[int64]instToken
	sf    singleflight.Group
}

type instToken struct {
	token string
	exp   time.Time
}

// NewInstallations builds the token cache.
func NewInstallations(client *githubapi.Client, app *App, now func() time.Time) *Installations {
	if now == nil {
		now = time.Now
	}
	return &Installations{client: client, app: app, now: now, early: 5 * time.Minute, cache: map[int64]instToken{}}
}

// Token returns a valid installation token.
func (t *Installations) Token(ctx context.Context, id int64) (string, error) {
	t.mu.Lock()
	c, ok := t.cache[id]
	t.mu.Unlock()
	if ok && t.now().Before(c.exp.Add(-t.early)) {
		return c.token, nil
	}
	v, err, _ := t.sf.Do(strconv.FormatInt(id, 10), func() (any, error) {
		t.mu.Lock()
		c, ok := t.cache[id]
		t.mu.Unlock()
		if ok && t.now().Before(c.exp.Add(-t.early)) {
			return c.token, nil
		}
		resp, err := t.client.Do(context.WithoutCancel(ctx), t.app.Credential(), githubapi.Request{
			Method: http.MethodPost, URL: "/app/installations/" + strconv.FormatInt(id, 10) + "/access_tokens",
		})
		if err != nil {
			return nil, err
		}
		var tok struct {
			Token     string    `json:"token"`
			ExpiresAt time.Time `json:"expires_at"`
		}
		if err := json.Unmarshal(resp.Body, &tok); err != nil || tok.Token == "" {
			return nil, errors.New("installation token response without token")
		}
		t.mu.Lock()
		t.cache[id] = instToken{token: tok.Token, exp: tok.ExpiresAt}
		t.mu.Unlock()
		return tok.Token, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// Invalidate drops a cached token.
func (t *Installations) Invalidate(id int64) {
	t.mu.Lock()
	delete(t.cache, id)
	t.mu.Unlock()
}

// Credential returns the credential of one installation. name must be bounded (e.g. "installation:<account_key>").
func (t *Installations) Credential(id int64, name string) githubapi.Credential {
	return instCred{t: t, id: id, name: name}
}

type instCred struct {
	t    *Installations
	id   int64
	name string
}

func (c instCred) Name() string { return c.name }
func (c instCred) Authorization(ctx context.Context) (string, error) {
	tok, err := c.t.Token(ctx, c.id)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok, nil
}
func (c instCred) Invalidate() { c.t.Invalidate(c.id) }

// PAT is a personal access token read from a secret reference. File-backed tokens are re-read when the file changes
// or after a 401, so rotating the mounted secret needs no restart.
type PAT struct {
	name string
	ref  config.SecretRef

	mu    sync.Mutex
	value string
	mtime time.Time
}

// NewPAT builds a PAT credential. name must be bounded (e.g. "token:<account_key>").
func NewPAT(name string, ref config.SecretRef) *PAT { return &PAT{name: name, ref: ref} }

// Name implements githubapi.Credential.
func (p *PAT) Name() string { return p.name }

// Authorization implements githubapi.Credential.
func (p *PAT) Authorization(context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ref.File != "" {
		if st, err := os.Stat(p.ref.File); err == nil && !st.ModTime().Equal(p.mtime) {
			p.value, p.mtime = "", st.ModTime()
		}
	}
	if p.value == "" {
		b, err := p.ref.Resolve()
		if err != nil {
			return "", err
		}
		p.value = string(b)
	}
	return "Bearer " + p.value, nil
}

// Invalidate implements githubapi.Credential.
func (p *PAT) Invalidate() {
	p.mu.Lock()
	p.value = ""
	p.mu.Unlock()
}
