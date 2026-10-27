package githubauth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
)

func TestAppJWT(t *testing.T) {
	pemBytes, _ := os.ReadFile(githubtest.AppKey(t))
	key, err := ParsePrivateKey(pemBytes)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	tok, err := NewApp(42, "Iv1.abc", key, func() time.Time { return now }).JWT()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")
	claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct {
		Iat, Exp int64
		Iss      string
	}
	_ = json.Unmarshal(claims, &c)
	if c.Iss != "Iv1.abc" || c.Iat != now.Unix()-60 || c.Exp-now.Unix() > 600 {
		t.Fatalf("claims %+v (exp must be at most 10 minutes ahead)", c)
	}
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, sum[:], sig); err != nil {
		t.Fatal(err)
	}
}

// Concurrent requests for an expired installation token share one refresh; tokens refresh 5 minutes early.
func TestInstallationTokens(t *testing.T) {
	var issued atomic.Int32
	var mu sync.Mutex
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/9/access_tokens" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ey") {
			w.WriteHeader(400)
			return
		}
		n := issued.Add(1)
		time.Sleep(20 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{"token": fmt.Sprintf("ghs_%c", 'a'+n), "expires_at": clock().Add(time.Hour)})
	}))
	defer srv.Close()
	pemBytes, _ := os.ReadFile(githubtest.AppKey(t))
	key, _ := ParsePrivateKey(pemBytes)
	gh, _ := githubapi.New(githubapi.Options{BaseURL: srv.URL, HTTPClient: srv.Client(), Now: clock})
	tokens := NewInstallations(gh, NewApp(1, "", key, clock), clock)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = tokens.Token(context.Background(), 9) }()
	}
	wg.Wait()
	if issued.Load() != 1 {
		t.Fatalf("refresh stampede: %d tokens issued", issued.Load())
	}
	mu.Lock()
	now = now.Add(56 * time.Minute)
	mu.Unlock()
	if tok, _ := tokens.Token(context.Background(), 9); tok != "ghs_c" || issued.Load() != 2 {
		t.Fatalf("early refresh: %q after %d", tok, issued.Load())
	}
}

// An installation token refresh goes through the same client and its concurrency limit. A request on an
// installation credential must not hold a slot while the refresh waits for one (deadlock with MaxConcurrent 1).
func TestInstallationRequestWithOneSlot(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app/installations/9/access_tokens":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "ghs_x", "expires_at": time.Now().Add(time.Hour)})
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"total_count":0,"repositories":[]}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	pemBytes, _ := os.ReadFile(githubtest.AppKey(t))
	key, _ := ParsePrivateKey(pemBytes)
	gh, _ := githubapi.New(githubapi.Options{BaseURL: srv.URL, HTTPClient: srv.Client(), MaxConcurrent: 1})
	tokens := NewInstallations(gh, NewApp(1, "", key, time.Now), time.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := gh.Do(ctx, tokens.Credential(9, "installation:x"), githubapi.Request{URL: "/installation/repositories"}); err != nil {
		t.Fatal(err)
	}
}

func TestPATRereadsRotatedFile(t *testing.T) {
	p := t.TempDir() + "/token"
	_ = os.WriteFile(p, []byte("one\n"), 0o600)
	pat := NewPAT("token:x", config.SecretRef{File: p})
	if a, _ := pat.Authorization(context.Background()); a != "Bearer one" {
		t.Fatal(a)
	}
	_ = os.WriteFile(p, []byte("two"), 0o600)
	pat.Invalidate()
	if a, _ := pat.Authorization(context.Background()); a != "Bearer two" {
		t.Fatal(a)
	}
}
