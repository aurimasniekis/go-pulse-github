package githubapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type staticCred struct {
	invalidated atomic.Int32
}

func (*staticCred) Name() string                                  { return "token:test" }
func (*staticCred) Authorization(context.Context) (string, error) { return "Bearer t", nil }
func (c *staticCred) Invalidate()                                 { c.invalidated.Add(1) }

func newClient(t *testing.T, h http.HandlerFunc) (*Client, *[]time.Duration) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	var slept []time.Duration
	c, err := New(Options{BaseURL: srv.URL, HTTPClient: srv.Client(), Now: func() time.Time { return time.Unix(1_800_000_000, 0) },
		Sleep: func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return c, &slept
}

func TestHeaders(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/vnd.github+json" || r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" ||
			r.Header.Get("Authorization") != "Bearer t" || r.Header.Get("If-None-Match") != `"e1"` {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusNotModified)
	})
	resp, err := c.Do(context.Background(), &staticCred{}, Request{URL: "/x", ETag: `"e1"`})
	if err != nil || !resp.NotModified {
		t.Fatalf("%+v %v", resp, err)
	}
}

// Error classes follow GitHub's documented rules: retry-after first, then x-ratelimit-remaining=0 with the reset time,
// then a one-minute floor for secondary limits without headers.
func TestClassify(t *testing.T) {
	for _, c := range []struct {
		name    string
		status  int
		headers map[string]string
		body    string
		class   string
		retry   time.Duration
	}{
		{"primary", 403, map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1800003600"}, `{"message":"API rate limit exceeded"}`, ClassRateLimited, time.Hour + time.Second},
		{"retry-after", 429, map[string]string{"Retry-After": "120"}, `{}`, ClassSecondaryLimited, 120 * time.Second},
		{"secondary without headers", 403, nil, `{"message":"You have exceeded a secondary rate limit"}`, ClassSecondaryLimited, time.Minute},
		{"missing permission", 403, nil, `{"message":"Resource not accessible by integration"}`, ClassForbidden, 0},
		{"bad credentials", 401, nil, `{"message":"Bad credentials"}`, ClassUnauthorized, 0},
		{"ambiguous not found", 404, nil, `{"message":"Not Found"}`, ClassNotFound, 0},
		{"gone", 410, nil, `{}`, ClassGone, 0},
		{"server", 502, nil, `bad gateway`, ClassServer, 0},
		{"validation", 422, nil, `{"message":"Validation Failed"}`, ClassInvalid, 0},
	} {
		cl, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			for k, v := range c.headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(c.body))
		})
		cl.opts.MaxRetries = 0
		_, err := cl.Do(context.Background(), &staticCred{}, Request{URL: "/x"})
		if ClassOf(err) != c.class || RetryAfter(err) != c.retry {
			t.Errorf("%s: class %q retry %v (%v)", c.name, ClassOf(err), RetryAfter(err), err)
		}
	}
}

func TestRetriesTransientAndShortLimits(t *testing.T) {
	var n atomic.Int32
	c, slept := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			w.WriteHeader(502)
		case 2:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	})
	if _, err := c.Do(context.Background(), &staticCred{}, Request{URL: "/x"}); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 3 || len(*slept) != 2 || (*slept)[1] != 7*time.Second {
		t.Fatalf("calls %d sleeps %v", n.Load(), *slept)
	}
	// A long wait is returned to the caller to reschedule instead of blocking a worker.
	c2, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(1_800_000_000+3600))
		w.WriteHeader(403)
	})
	_, err := c2.Do(context.Background(), &staticCred{}, Request{URL: "/x"})
	if !IsClass(err, ClassRateLimited) || RetryAfter(err) < time.Hour {
		t.Fatalf("%v", err)
	}
	// The budget stays blocked for this credential until the reset; no request is sent meanwhile.
	if _, err := c2.Do(context.Background(), &staticCred{}, Request{URL: "/x"}); !strings.Contains(err.Error(), "budget exhausted") {
		t.Fatalf("%v", err)
	}
}

func TestUnauthorizedInvalidatesOnce(t *testing.T) {
	cred := &staticCred{}
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) })
	_, err := c.Do(context.Background(), cred, Request{URL: "/x"})
	if !IsClass(err, ClassUnauthorized) || cred.invalidated.Load() != 1 {
		t.Fatalf("%v invalidated %d", err, cred.invalidated.Load())
	}
}

// URLs from payloads and Link headers must stay on the configured API origin and path, so credentials never leak.
func TestResolveURL(t *testing.T) {
	c, err := New(Options{BaseURL: "https://ghe.example.com/api/v3"})
	if err != nil {
		t.Fatal(err)
	}
	for u, ok := range map[string]bool{
		"/repos/a/b/issues/1":                               true,
		"/repos/a/b/issues/1#frag":                          false,
		"https://ghe.example.com/api/v3/repos/a/b":          true,
		"https://GHE.example.com/api/v3/user":               true,
		"https://evil.example.com/api/v3/repos/a/b":         false,
		"http://ghe.example.com/api/v3/repos/a/b":           false,
		"https://ghe.example.com/other/path":                false,
		"https://user:pw@ghe.example.com/api/v3/x":          false,
		"//evil.example.com/x":                              false,
		"https://ghe.example.com/api/v3/../../admin":        false,
		"https://ghe.example.com.evil.com/api/v3/repos/a/b": false,
	} {
		if _, err := c.ResolveURL(u); (err == nil) != ok {
			t.Errorf("%s: ok=%v err=%v", u, ok, err)
		}
	}
}

func TestPaginationRefusesForeignNext(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<https://evil.example.com/x?page=2>; rel="next", <https://evil.example.com/x?page=9>; rel="last"`)
		_, _ = w.Write([]byte(`[]`))
	})
	if _, err := c.Do(context.Background(), &staticCred{}, Request{URL: "/x"}); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("%v", err)
	}
	if got := NextLink(`<https://api.github.com/x?page=2>; rel="next", <https://api.github.com/x?page=5>; rel="last"`); got != "https://api.github.com/x?page=2" {
		t.Fatalf("NextLink %q", got)
	}
}

func TestNoRedirects(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example.com/steal", http.StatusFound)
	})
	if _, err := c.Do(context.Background(), &staticCred{}, Request{URL: "/x"}); err == nil {
		t.Fatal("redirects must not be followed with credentials")
	}
}
