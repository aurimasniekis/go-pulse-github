// Package githubtest is a scriptable fake GitHub REST API for tests. Live GitHub credentials are never needed.
package githubtest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Request is a recorded request.
type Request struct {
	Method string
	Path   string
	Query  string
	Header http.Header
}

// Server is a TLS fake of api.github.com.
type Server struct {
	*httptest.Server
	mu       sync.Mutex
	handlers map[string]http.HandlerFunc
	requests []Request
}

// New starts a fake. Unregistered routes answer 404 like GitHub.
func New(t testing.TB) *Server {
	t.Helper()
	s := &Server{handlers: map[string]http.HandlerFunc{}}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

// Handle registers "METHOD /path" (exact path, query ignored).
func (s *Server) Handle(route string, h http.HandlerFunc) {
	s.mu.Lock()
	s.handlers[route] = h
	s.mu.Unlock()
}

// JSON registers a fixed JSON response.
func (s *Server) JSON(route string, status int, body string, headers ...string) {
	s.Handle(route, func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

// Requests returns recorded requests.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Count counts requests to a path.
func (s *Server) Count(path string) int {
	n := 0
	for _, r := range s.Requests() {
		if r.Path == path {
			n++
		}
	}
	return n
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, Request{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone()})
	h := s.handlers[r.Method+" "+r.URL.Path]
	s.mu.Unlock()
	if h == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
		return
	}
	if !strings.HasPrefix(r.Header.Get("Accept"), "application/vnd.github") || r.Header.Get("X-GitHub-Api-Version") == "" {
		http.Error(w, `{"message":"missing API headers"}`, http.StatusBadRequest)
		return
	}
	h(w, r)
}

// AppKey writes a fresh RSA key in PEM (PKCS#1, as GitHub issues App keys) and returns its path.
func AppKey(t testing.TB) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "app.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
