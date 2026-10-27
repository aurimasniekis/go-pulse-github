// Package githubapi is a small GitHub REST client: explicit API version and Accept headers, bounded timeouts,
// conditional requests, Link pagination restricted to the configured API origin, per-credential rate-limit tracking,
// FIFO-fair concurrency and typed error classes. Response bodies are returned as raw bytes so callers keep the
// original JSON.
package githubapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/aurimasniekis/go-pulse-github/internal/observability"
)

// MaxResponseBytes bounds a response body.
const MaxResponseBytes = 32 << 20

// Credential authenticates requests.
type Credential interface {
	// Name is a bounded identifier used for rate-limit tracking and metric labels (e.g. "token:alice", "app").
	Name() string
	// Authorization returns the Authorization header value.
	Authorization(ctx context.Context) (string, error)
	// Invalidate drops cached material after a 401 (expired installation token, rotated PAT file).
	Invalidate()
}

// Error classes.
const (
	ClassUnauthorized     = "unauthorized"      // 401: expired, revoked or wrong credential
	ClassForbidden        = "forbidden"         // 403 (not rate limiting): missing permission, suspended installation, SSO
	ClassNotFound         = "not_found"         // 404: deleted, never existed, or not visible to this credential (ambiguous)
	ClassGone             = "gone"              // 410
	ClassRateLimited      = "rate_limited"      // primary limit exhausted
	ClassSecondaryLimited = "secondary_limited" // secondary limit / abuse detection
	ClassInvalid          = "invalid_request"   // 400, 422
	ClassServer           = "server_error"      // 5xx
	ClassNetwork          = "network_error"     // transport failure or timeout
	ClassCredential       = "credential_error"  //nolint:gosec // error class: could not obtain a credential
)

// Error is a classified GitHub API failure.
type Error struct {
	Class      string
	Status     int
	Message    string
	RetryAfter time.Duration
	Path       string
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("github %s %d %s: %s", e.Path, e.Status, e.Class, e.Message)
	}
	return fmt.Sprintf("github %s %s: %s", e.Path, e.Class, e.Message)
}

// ClassOf returns the class of a GitHub error ("" for other errors).
func ClassOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ""
}

// IsClass reports whether err is a GitHub error of the class.
func IsClass(err error, class string) bool { return ClassOf(err) == class }

// RetryAfter returns how long to wait before retrying err (0 when unknown).
func RetryAfter(err error) time.Duration {
	var e *Error
	if errors.As(err, &e) {
		return e.RetryAfter
	}
	return 0
}

// Options configure a client.
type Options struct {
	BaseURL         string
	APIVersion      string
	UserAgent       string
	HTTPClient      *http.Client
	MaxConcurrent   int
	RateReserve     int
	MaxRetries      int
	MaxInlineWait   time.Duration
	Logger          *slog.Logger
	Metrics         *observability.Metrics
	Now             func() time.Time
	Sleep           func(ctx context.Context, d time.Duration) error
	SecondaryFloor  time.Duration
	RetryBackoffMin time.Duration
}

// Client is a GitHub REST client.
type Client struct {
	base    *url.URL
	opts    Options
	http    *http.Client
	sem     *semaphore.Weighted
	log     *slog.Logger
	metrics *observability.Metrics

	mu    sync.Mutex
	rates map[string]*rateState
}

type rateState struct {
	remaining int
	reset     time.Time
	blocked   time.Time // secondary limit or retry-after: no requests before this time
	known     bool
}

// New builds a client.
func New(o Options) (*Client, error) {
	base, err := url.Parse(strings.TrimRight(o.BaseURL, "/"))
	if err != nil || base.Scheme != "https" && base.Scheme != "http" || base.Host == "" {
		return nil, fmt.Errorf("invalid GitHub API URL %q", o.BaseURL)
	}
	if o.APIVersion == "" {
		o.APIVersion = "2022-11-28"
	}
	if o.UserAgent == "" {
		o.UserAgent = "pulse-github"
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	// Never follow redirects automatically: a redirect could carry the Authorization header to another origin.
	hc := *o.HTTPClient
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if o.MaxConcurrent < 1 {
		o.MaxConcurrent = 8
	}
	if o.MaxRetries < 0 {
		o.MaxRetries = 0
	} else if o.MaxRetries == 0 {
		o.MaxRetries = 3
	}
	if o.MaxInlineWait == 0 {
		o.MaxInlineWait = 2 * time.Minute
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = sleepCtx
	}
	if o.SecondaryFloor == 0 {
		o.SecondaryFloor = time.Minute
	}
	if o.RetryBackoffMin == 0 {
		o.RetryBackoffMin = time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Client{base: base, opts: o, http: &hc, sem: semaphore.NewWeighted(int64(o.MaxConcurrent)),
		log: o.Logger.With(observability.FieldComponent, "githubapi"), metrics: o.Metrics, rates: map[string]*rateState{}}, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// BaseURL is the configured API origin.
func (c *Client) BaseURL() *url.URL { u := *c.base; return &u }

// ResolveURL turns a path ("/notifications?all=true") or an absolute URL into a request URL. Absolute URLs must use
// the configured API scheme, host and path prefix; anything else is refused, so URLs taken from payloads or Link
// headers can never send credentials elsewhere.
func (c *Client) ResolveURL(raw string) (*url.URL, error) {
	if strings.HasPrefix(raw, "/") && !strings.HasPrefix(raw, "//") {
		raw = c.base.String() + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != c.base.Scheme || !strings.EqualFold(u.Host, c.base.Host) || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("URL %q is outside the GitHub API origin %s", redactURL(u), c.base.Scheme+"://"+c.base.Host)
	}
	prefix := c.base.Path
	if prefix != "" && u.Path != prefix && !strings.HasPrefix(u.Path, prefix+"/") {
		return nil, fmt.Errorf("URL path %q is outside the GitHub API path %q", u.Path, prefix)
	}
	if strings.Contains(u.Path, "/../") || strings.HasSuffix(u.Path, "/..") {
		return nil, errors.New("URL path contains traversal")
	}
	return u, nil
}

func redactURL(u *url.URL) string { return u.Scheme + "://" + u.Host + u.Path }

// Request is one API call.
type Request struct {
	Method          string
	URL             string
	Body            []byte
	ETag            string
	IfModifiedSince string
	Accept          string
}

// Response is a successful (2xx or 304) response.
type Response struct {
	Status       int
	Header       http.Header
	Body         []byte
	NotModified  bool
	Next         string
	ETag         string
	LastModified string
	PollInterval time.Duration
}

// Do performs a request with rate-limit awareness and bounded retries. Short waits (rate limits, transient errors)
// happen inline; longer waits return an *Error with RetryAfter so the caller can reschedule.
func (c *Client) Do(ctx context.Context, cred Credential, r Request) (*Response, error) {
	u, err := c.ResolveURL(r.URL)
	if err != nil {
		return nil, err
	}
	if r.Method == "" {
		r.Method = http.MethodGet
	}
	reauthed := false
	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if err := c.waitForBudget(ctx, cred, u.Path); err != nil {
			return nil, err
		}
		resp, err := c.once(ctx, cred, r, u)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		var ge *Error
		if !errors.As(err, &ge) {
			return nil, err
		}
		switch ge.Class {
		case ClassUnauthorized:
			if reauthed {
				return nil, err
			}
			cred.Invalidate()
			reauthed = true
			continue
		case ClassRateLimited, ClassSecondaryLimited:
			// classify blocked the credential until the retry time; waitForBudget performs the wait.
			if ge.RetryAfter > c.opts.MaxInlineWait {
				return nil, err
			}
		case ClassServer, ClassNetwork:
			d := c.opts.RetryBackoffMin << attempt
			if ge.RetryAfter > d {
				d = ge.RetryAfter
			}
			if err := c.opts.Sleep(ctx, d); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *Client) state(name string) *rateState {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.rates[name]
	if !ok {
		s = &rateState{}
		c.rates[name] = s
	}
	return s
}

// waitForBudget holds requests while a credential is blocked by a secondary limit or has spent its primary budget
// down to the reserve.
func (c *Client) waitForBudget(ctx context.Context, cred Credential, path string) error {
	s := c.state(cred.Name())
	now := c.opts.Now()
	c.mu.Lock()
	var wait time.Duration
	if s.blocked.After(now) {
		wait = s.blocked.Sub(now)
	}
	if s.known && s.remaining <= c.opts.RateReserve && s.reset.After(now) {
		wait = max(wait, s.reset.Sub(now)+time.Second)
	}
	c.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	if wait > c.opts.MaxInlineWait {
		return &Error{Class: ClassRateLimited, Message: "request budget exhausted for credential " + cred.Name(),
			RetryAfter: wait, Path: path}
	}
	return c.opts.Sleep(ctx, wait)
}

func (c *Client) once(ctx context.Context, cred Credential, r Request, u *url.URL) (*Response, error) {
	if err := c.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer c.sem.Release(1)
	auth, err := cred.Authorization(ctx)
	if err != nil {
		if c.metrics != nil {
			c.metrics.CredentialFailures.WithLabelValues(cred.Name(), ClassCredential).Inc()
		}
		return nil, &Error{Class: ClassCredential, Message: err.Error(), Path: u.Path}
	}
	var body io.Reader
	if r.Body != nil {
		body = bytes.NewReader(r.Body)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u.String(), body)
	if err != nil {
		return nil, err
	}
	accept := r.Accept
	if accept == "" {
		accept = "application/vnd.github+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", c.opts.APIVersion)
	req.Header.Set("User-Agent", c.opts.UserAgent)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if r.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.ETag != "" {
		req.Header.Set("If-None-Match", r.ETag)
	}
	if r.IfModifiedSince != "" {
		req.Header.Set("If-Modified-Since", r.IfModifiedSince)
	}
	start := c.opts.Now()
	resp, err := c.http.Do(req)
	if c.metrics != nil {
		c.metrics.GitHubLatency.WithLabelValues(cred.Name()).Observe(c.opts.Now().Sub(start).Seconds())
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.count(cred, "network")
		return nil, &Error{Class: ClassNetwork, Message: err.Error(), Path: u.Path}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		c.count(cred, "network")
		return nil, &Error{Class: ClassNetwork, Message: "read body: " + err.Error(), Path: u.Path}
	}
	if len(data) > MaxResponseBytes {
		return nil, &Error{Class: ClassInvalid, Status: resp.StatusCode, Message: "response body too large", Path: u.Path}
	}
	c.count(cred, statusClass(resp.StatusCode))
	c.updateRate(cred, resp.Header)
	if resp.StatusCode == http.StatusNotModified {
		return &Response{Status: resp.StatusCode, Header: resp.Header, NotModified: true, ETag: resp.Header.Get("ETag"),
			LastModified: resp.Header.Get("Last-Modified"), PollInterval: pollInterval(resp.Header)}, nil
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		out := &Response{Status: resp.StatusCode, Header: resp.Header, Body: data, ETag: resp.Header.Get("ETag"),
			LastModified: resp.Header.Get("Last-Modified"), PollInterval: pollInterval(resp.Header)}
		if next := NextLink(resp.Header.Get("Link")); next != "" {
			if _, err := c.ResolveURL(next); err != nil {
				return nil, &Error{Class: ClassInvalid, Status: resp.StatusCode, Message: "pagination: " + err.Error(), Path: u.Path}
			}
			out.Next = next
		}
		return out, nil
	}
	return nil, c.classify(cred, resp, data, u.Path)
}

func (c *Client) count(cred Credential, status string) {
	if c.metrics != nil {
		c.metrics.GitHubRequests.WithLabelValues(cred.Name(), status).Inc()
	}
}

func statusClass(code int) string {
	switch {
	case code == http.StatusNotModified:
		return "304"
	case code >= 500:
		return "5xx"
	case code >= 400:
		return strconv.Itoa(code)
	default:
		return "2xx"
	}
}

func (c *Client) updateRate(cred Credential, h http.Header) {
	rem, err1 := strconv.Atoi(h.Get("X-RateLimit-Remaining"))
	reset, err2 := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err1 != nil || err2 != nil {
		return
	}
	s := c.state(cred.Name())
	c.mu.Lock()
	s.remaining, s.reset, s.known = rem, time.Unix(reset, 0), true
	c.mu.Unlock()
	if c.metrics != nil {
		c.metrics.GitHubRateRemaining.WithLabelValues(cred.Name()).Set(float64(rem))
	}
}

func (c *Client) block(cred Credential, until time.Time) {
	s := c.state(cred.Name())
	c.mu.Lock()
	if until.After(s.blocked) {
		s.blocked = until
	}
	c.mu.Unlock()
}

// classify implements GitHub's documented rate-limit rules: honour retry-after; else when remaining is 0 wait for
// the reset; else (secondary limit without headers) wait at least a minute.
func (c *Client) classify(cred Credential, resp *http.Response, body []byte, path string) error {
	msg := apiMessage(body)
	e := &Error{Status: resp.StatusCode, Message: msg, Path: path}
	now := c.opts.Now()
	retryAfter := -1 * time.Second
	if v := resp.Header.Get("Retry-After"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			retryAfter = time.Duration(n) * time.Second
		}
	}
	remaining := resp.Header.Get("X-RateLimit-Remaining")
	lower := strings.ToLower(msg)
	switch resp.StatusCode {
	case http.StatusForbidden, http.StatusTooManyRequests:
		switch {
		case retryAfter >= 0:
			e.Class, e.RetryAfter = ClassSecondaryLimited, retryAfter
			if remaining == "0" {
				e.Class = ClassRateLimited
			}
		case remaining == "0":
			reset, _ := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64)
			e.Class, e.RetryAfter = ClassRateLimited, max(time.Unix(reset, 0).Sub(now)+time.Second, time.Second)
		case resp.StatusCode == http.StatusTooManyRequests || strings.Contains(lower, "secondary rate limit") ||
			strings.Contains(lower, "abuse"):
			e.Class, e.RetryAfter = ClassSecondaryLimited, c.opts.SecondaryFloor
		default:
			e.Class = ClassForbidden
		}
	case http.StatusUnauthorized:
		e.Class = ClassUnauthorized
		if c.metrics != nil {
			c.metrics.CredentialFailures.WithLabelValues(cred.Name(), ClassUnauthorized).Inc()
		}
	case http.StatusNotFound:
		e.Class = ClassNotFound
	case http.StatusGone:
		e.Class = ClassGone
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		e.Class = ClassInvalid
	default:
		if resp.StatusCode >= 500 {
			e.Class = ClassServer
			if retryAfter > 0 {
				e.RetryAfter = retryAfter
			}
		} else {
			e.Class = ClassInvalid
		}
	}
	if e.Class == ClassRateLimited || e.Class == ClassSecondaryLimited {
		c.block(cred, now.Add(e.RetryAfter))
		kind := "primary"
		if e.Class == ClassSecondaryLimited {
			kind = "secondary"
		}
		if c.metrics != nil {
			c.metrics.GitHubRateLimited.WithLabelValues(cred.Name(), kind).Inc()
		}
		c.log.Warn("GitHub rate limit", "credential", cred.Name(), "kind", kind,
			observability.FieldRetryAfterSecs, int64(e.RetryAfter/time.Second))
	}
	return e
}

func apiMessage(body []byte) string {
	var m struct {
		Message string `json:"message"`
	}
	if err := jsonUnmarshal(body, &m); err == nil && m.Message != "" {
		return truncate(m.Message, 300)
	}
	return truncate(strings.TrimSpace(string(body)), 120)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func pollInterval(h http.Header) time.Duration {
	if n, err := strconv.Atoi(h.Get("X-Poll-Interval")); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 0
}

var linkRe = regexp.MustCompile(`<([^>]+)>\s*;\s*rel="([^"]+)"`)

// NextLink returns the rel="next" URL of a Link header.
func NextLink(h string) string {
	for _, m := range linkRe.FindAllStringSubmatch(h, -1) {
		for _, rel := range strings.Fields(m[2]) {
			if rel == "next" {
				return m[1]
			}
		}
	}
	return ""
}
