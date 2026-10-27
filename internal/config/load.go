package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/aurimasniekis/go-pulse/pkg/pulse/envelope"
)

// EnvPrefix prefixes operational environment overrides.
const EnvPrefix = "PULSE_GITHUB_"

var (
	accountKeyRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,38}$`)
	loginRe      = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$`)
	domainRe     = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	repoRe       = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}/[A-Za-z0-9._-]{1,100}$`)
)

// Sources that can appear in routing shares.
var shareSources = []string{"webhook", "notifications", "activity", "reconcile", "enrichment", "collector"}

// Load reads, defaults, overrides and validates a configuration file.
func Load(file string) (*Config, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(b, os.Getenv)
}

// Parse decodes configuration bytes. getenv supplies environment overrides (nil disables them).
func Parse(b []byte, getenv func(string) string) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.Version != CurrentVersion {
		return nil, fmt.Errorf("config version %d is not supported (want %d)", c.Version, CurrentVersion)
	}
	c.applyDefaults()
	if getenv != nil {
		if err := c.applyEnv(getenv); err != nil {
			return nil, err
		}
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	setStr(&c.Service.Source, "pulse-github")
	setDur(&c.Service.ShutdownTimeout, 25*time.Second)
	setStr(&c.Log.Level, "info")
	setStr(&c.Log.Format, "json")

	setStr(&c.Storage.Path, "/data/pulse-github.db")
	setSize(&c.Storage.MinFreeBytes, 512<<20)
	setSize(&c.Storage.MaxDBBytes, 20<<30)
	setInt(&c.Storage.MaxInboxPending, 100000)
	setDur(&c.Storage.Retention.Published, 7*24*time.Hour)
	setDur(&c.Storage.Retention.InboxProcessed, 7*24*time.Hour)
	setDur(&c.Storage.Retention.Observations, 35*24*time.Hour)
	setDur(&c.Storage.Retention.SweepInterval, time.Hour)

	setStr(&c.Pulse.SubjectDomain, "github")
	setDur(&c.Pulse.PublishTimeout, 10*time.Second)

	setInt(&c.Outbox.Workers, 4)
	setInt(&c.Outbox.PoisonAttempts, 5)
	setDur(&c.Outbox.BackoffInitial, time.Second)
	setDur(&c.Outbox.BackoffMax, 5*time.Minute)
	setDur(&c.Outbox.PollInterval, time.Second)

	setStr(&c.Webhook.Listen, "0.0.0.0:8080")
	setStr(&c.Webhook.Path, "/webhook")
	setSize(&c.Webhook.MaxBodyBytes, 26<<20)
	setDur(&c.Webhook.ReadTimeout, 30*time.Second)
	setInt(&c.Webhook.ProcessorPoisonAttempts, 5)

	setStr(&c.Admin.Listen, "127.0.0.1:9090")

	setStr(&c.GitHub.APIURL, "https://api.github.com")
	setStr(&c.GitHub.APIVersion, "2022-11-28")
	setDur(&c.GitHub.RequestTimeout, 30*time.Second)
	setInt(&c.GitHub.MaxConcurrentRequests, 8)
	setInt(&c.GitHub.RateLimitReserve, 100)
	setDur(&c.GitHub.App.ReconcileInterval, time.Hour)

	for i := range c.Accounts {
		a := &c.Accounts[i]
		setStr(&a.Key, strings.ToLower(a.Login))
		n := &a.Notifications
		setDur(&n.Interval, 60*time.Second)
		setDur(&n.Overlap, 10*time.Minute)
		setInt(&n.MaxPages, 20)
		setDur(&n.FullSweepInterval, 6*time.Hour)
		setDur(&n.InitialLookback, 7*24*time.Hour)
		setInt(&n.Enrichment.MaxPerHour, 200)
		ac := &a.Activity
		setDur(&ac.Interval, 60*time.Second)
		setInt(&ac.MaxPages, 3)
		for j := range ac.Feeds {
			setStr(&ac.Feeds[j].Credential, CredentialToken)
		}
		r := &a.Reconcile
		setDur(&r.Interval, 6*time.Hour)
		setDur(&r.Lookback, 24*time.Hour)
		setDur(&r.Overlap, time.Hour)
		setInt(&r.MaxRequestsPerRun, 300)
		setDur(&r.MaxDurationPerRun, 10*time.Minute)
		if len(r.Kinds) == 0 {
			r.Kinds = []string{KindRepository, KindIssues, KindPulls, KindReleases}
		}
		setDur(&r.BackfillLookback, 7*24*time.Hour)
		setInt(&r.BackfillMaxRequest, 1000)
	}
}

// applyEnv applies operational overrides. Secrets are never taken from these variables directly.
func (c *Config) applyEnv(getenv func(string) string) error {
	str := map[string]*string{
		"LOG_LEVEL":        &c.Log.Level,
		"LOG_FORMAT":       &c.Log.Format,
		"STORAGE_PATH":     &c.Storage.Path,
		"PULSE_URL":        &c.Pulse.URL,
		"PULSE_CREDS_FILE": &c.Pulse.CredsFile,
		"PULSE_CA_FILE":    &c.Pulse.CAFile,
		"WEBHOOK_LISTEN":   &c.Webhook.Listen,
		"ADMIN_LISTEN":     &c.Admin.Listen,
		"GITHUB_API_URL":   &c.GitHub.APIURL,
	}
	for k, p := range str {
		if v := getenv(EnvPrefix + k); v != "" {
			*p = v
		}
	}
	if v := getenv(EnvPrefix + "SHUTDOWN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%sSHUTDOWN_TIMEOUT: %w", EnvPrefix, err)
		}
		c.Service.ShutdownTimeout = Duration(d)
	}
	return nil
}

// Validate checks the whole configuration and joins every problem found.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		add("log.level %q must be debug, info, warn or error", c.Log.Level)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		add("log.format %q must be json or text", c.Log.Format)
	}
	if c.Service.Source == "" {
		add("service.source is required")
	}
	if c.Service.ShutdownTimeout.D() < time.Second {
		add("service.shutdown_timeout must be at least 1s")
	}
	if c.Storage.Path == "" {
		add("storage.path is required")
	}
	if c.Storage.MaxInboxPending < 1 {
		add("storage.max_inbox_pending must be positive")
	}
	if !domainRe.MatchString(c.Pulse.SubjectDomain) {
		add("pulse.subject_domain %q must match %s", c.Pulse.SubjectDomain, domainRe)
	}
	if c.Pulse.URL == "" {
		add("pulse.url is required")
	}
	if c.Pulse.CredsFile == "" {
		add("pulse.creds_file is required")
	}
	if c.Pulse.PublishTimeout.D() < time.Second {
		add("pulse.publish_timeout must be at least 1s")
	}
	if c.Outbox.Workers < 1 || c.Outbox.Workers > 64 {
		add("outbox.workers must be between 1 and 64")
	}
	if c.Outbox.PoisonAttempts < 1 {
		add("outbox.poison_attempts must be positive")
	}
	if c.Outbox.BackoffInitial.D() <= 0 || c.Outbox.BackoffMax.D() < c.Outbox.BackoffInitial.D() {
		add("outbox backoff must satisfy 0 < backoff_initial <= backoff_max")
	}
	if c.Webhook.Enabled {
		if len(c.Webhook.Secrets) == 0 {
			add("webhook.secrets needs at least one secret reference when webhooks are enabled")
		}
		for i, s := range c.Webhook.Secrets {
			if s.IsZero() {
				add("webhook.secrets[%d] is empty", i)
			}
		}
		if !strings.HasPrefix(c.Webhook.Path, "/") {
			add("webhook.path must start with /")
		}
		if c.Webhook.MaxBodyBytes < 1<<20 {
			add("webhook.max_body_bytes must be at least 1MiB (GitHub sends up to 25MB)")
		}
		if !c.GitHub.App.Enabled {
			add("webhook.enabled requires github.app (installations are resolved through the App API)")
		}
		if err := checkListen(c.Webhook.Listen); err != nil {
			add("webhook.listen: %v", err)
		}
		if c.Webhook.Listen == c.Admin.Listen {
			add("webhook.listen and admin.listen must differ")
		}
	}
	if err := checkListen(c.Admin.Listen); err != nil {
		add("admin.listen: %v", err)
	}
	if c.Admin.Pprof.Enabled && c.Admin.Pprof.Token.IsZero() {
		add("admin.pprof.token is required when pprof is enabled")
	}
	if u, err := url.Parse(c.GitHub.APIURL); err != nil || u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.User != nil {
		add("github.api_url %q must be an https URL without credentials or query", c.GitHub.APIURL)
	}
	if c.GitHub.MaxConcurrentRequests < 1 || c.GitHub.MaxConcurrentRequests > 100 {
		add("github.max_concurrent_requests must be between 1 and 100 (GitHub allows 100 concurrent requests)")
	}
	if c.GitHub.App.Enabled {
		if c.GitHub.App.AppID <= 0 && c.GitHub.App.ClientID == "" {
			add("github.app needs app_id or client_id")
		}
		if c.GitHub.App.PrivateKey.IsZero() {
			add("github.app.private_key is required")
		}
		if c.GitHub.App.ReconcileInterval.D() < time.Minute {
			add("github.app.reconcile_interval must be at least 1m")
		}
	}

	if len(c.Accounts) == 0 {
		add("at least one account is required")
	}
	keys := map[string]bool{}
	logins := map[string]bool{}
	ids := map[int64]bool{}
	for i := range c.Accounts {
		errs = append(errs, c.Accounts[i].validate(c, i)...)
		a := c.Accounts[i]
		if keys[a.Key] {
			add("accounts[%d]: duplicate key %q", i, a.Key)
		}
		keys[a.Key] = true
		l := strings.ToLower(a.Login)
		if logins[l] {
			add("accounts[%d]: login %q is configured twice (contradictory account mapping)", i, a.Login)
		}
		logins[l] = true
		if a.GitHubID != 0 {
			if ids[a.GitHubID] {
				add("accounts[%d]: github_id %d is configured twice", i, a.GitHubID)
			}
			ids[a.GitHubID] = true
		}
	}
	for i, s := range c.Routing.Shares {
		if !keys[s.From] || !keys[s.To] {
			add("routing.shares[%d]: from %q and to %q must be configured account keys", i, s.From, s.To)
		}
		if s.From == s.To {
			add("routing.shares[%d]: from and to are the same account", i)
		}
		if len(s.Sources) == 0 {
			add("routing.shares[%d]: sources must list at least one source", i)
		}
		for _, src := range s.Sources {
			if !slices.Contains(shareSources, src) {
				add("routing.shares[%d]: unknown source %q", i, src)
			}
		}
		errs = append(errs, s.Repositories.validate(fmt.Sprintf("routing.shares[%d].repositories", i))...)
	}
	return errors.Join(errs...)
}

func (a *Account) validate(c *Config, i int) []error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("accounts[%d] (%s): %s", i, a.Key, fmt.Sprintf(format, args...)))
	}
	if !loginRe.MatchString(a.Login) {
		add("login %q is not a valid GitHub login", a.Login)
	}
	if !accountKeyRe.MatchString(a.Key) {
		add("key %q must match %s", a.Key, accountKeyRe)
	} else if err := envelope.ValidateSubject("evt." + c.Pulse.SubjectDomain + "." + a.Key + ".collector.check.none"); err != nil {
		add("key %q does not form a valid Pulse subject: %v", a.Key, err)
	}
	if a.Type != AccountUser && a.Type != AccountOrganization {
		add("type %q must be user or organization", a.Type)
	}
	if a.GitHubID < 0 {
		add("github_id must be positive")
	}
	errs = append(errs, a.Repositories.validate(fmt.Sprintf("accounts[%d].repositories", i))...)

	n := a.Notifications
	if n.Enabled {
		if a.Type != AccountUser {
			add("notifications are only available for user accounts")
		}
		if a.Token.IsZero() {
			add("notifications need a classic personal access token (token)")
		}
		if n.Interval.D() < 10*time.Second {
			add("notifications.interval must be at least 10s")
		}
		if n.Overlap.D() < 0 || n.Overlap.D() > 24*time.Hour {
			add("notifications.overlap must be between 0 and 24h")
		}
		if n.MaxPages < 1 {
			add("notifications.max_pages must be positive")
		}
		if n.FullSweepInterval.D() < time.Minute {
			add("notifications.full_sweep_interval must be at least 1m")
		}
	}
	ac := a.Activity
	if ac.Enabled {
		if ac.Interval.D() < 10*time.Second {
			add("activity.interval must be at least 10s")
		}
		if ac.MaxPages < 1 || ac.MaxPages > 10 {
			add("activity.max_pages must be between 1 and 10 (the Events API returns at most 300 events)")
		}
		if len(ac.Feeds) == 0 {
			add("activity.feeds must list at least one feed")
		}
		seen := map[string]bool{}
		for j, f := range ac.Feeds {
			if err := f.validate(a, c); err != nil {
				add("activity.feeds[%d]: %v", j, err)
			}
			if seen[f.ID()] {
				add("activity.feeds[%d]: duplicate feed %s", j, f.ID())
			}
			seen[f.ID()] = true
		}
	}
	r := a.Reconcile
	if r.Enabled || r.BackfillOnStartup {
		if !c.GitHub.App.Enabled {
			add("reconcile needs github.app (snapshots use installation tokens)")
		}
		if r.Interval.D() < time.Minute {
			add("reconcile.interval must be at least 1m")
		}
		if r.Lookback.D() <= 0 || r.BackfillLookback.D() <= 0 {
			add("reconcile lookbacks must be positive")
		}
		if r.MaxRequestsPerRun < 1 || r.BackfillMaxRequest < 1 {
			add("reconcile request budgets must be positive")
		}
		if r.MaxDurationPerRun.D() < time.Second {
			add("reconcile.max_duration_per_run must be at least 1s")
		}
		for _, k := range r.Kinds {
			switch k {
			case KindRepository, KindIssues, KindPulls, KindReleases, KindWorkflowRuns:
			default:
				add("reconcile.kinds: unknown kind %q", k)
			}
		}
	}
	if a.Webhook.Enabled && !c.Webhook.Enabled {
		add("webhook routing is enabled for the account but webhook.enabled is false")
	}
	return errs
}

// ID is a stable feed identifier used for checkpoints.
func (f FeedConfig) ID() string {
	switch f.Kind {
	case FeedUserOrgEvents, FeedOrgEvents, FeedAuditLog:
		return f.Kind + ":" + strings.ToLower(f.Org)
	case FeedRepoEvents:
		return f.Kind + ":" + strings.ToLower(f.Repo)
	default:
		return f.Kind
	}
}

func (f FeedConfig) validate(a *Account, c *Config) error {
	if f.Credential != CredentialToken && f.Credential != CredentialInstallation {
		return fmt.Errorf("credential %q must be token or installation", f.Credential)
	}
	if f.Credential == CredentialToken && a.Token.IsZero() {
		return errors.New("credential token needs the account token")
	}
	if f.Credential == CredentialInstallation && !c.GitHub.App.Enabled {
		return errors.New("credential installation needs github.app")
	}
	switch f.Kind {
	case FeedUserEvents, FeedReceivedEvents:
		if a.Type != AccountUser || f.Credential != CredentialToken {
			return fmt.Errorf("%s needs a user account and credential token", f.Kind)
		}
	case FeedUserOrgEvents:
		if a.Type != AccountUser || f.Credential != CredentialToken || !loginRe.MatchString(f.Org) {
			return fmt.Errorf("%s needs a user account, credential token and org", f.Kind)
		}
	case FeedOrgEvents:
		if !loginRe.MatchString(f.Org) {
			return fmt.Errorf("%s needs org", f.Kind)
		}
	case FeedRepoEvents:
		if !repoRe.MatchString(f.Repo) {
			return fmt.Errorf("%s needs repo as owner/name", f.Kind)
		}
	case FeedAuditLog:
		if f.Credential != CredentialToken || !loginRe.MatchString(f.Org) {
			return fmt.Errorf("%s needs org and credential token (classic PAT with read:audit_log, GitHub Enterprise Cloud)", f.Kind)
		}
	default:
		return fmt.Errorf("unknown feed kind %q", f.Kind)
	}
	return nil
}

func (r RepoFilter) validate(field string) []error {
	var errs []error
	for _, p := range append(slices.Clone(r.Allow), r.Deny...) {
		if _, err := path.Match(strings.ToLower(p), "x/y"); err != nil || !strings.Contains(p, "/") {
			errs = append(errs, fmt.Errorf("%s: invalid pattern %q (want owner/name glob)", field, p))
		}
	}
	return errs
}

// Allows reports whether a repository "owner/name" passes the filter. Deny wins over allow; an empty allow list allows all.
func (r RepoFilter) Allows(fullName string) bool {
	fn := strings.ToLower(fullName)
	for _, p := range r.Deny {
		if ok, _ := path.Match(strings.ToLower(p), fn); ok {
			return false
		}
	}
	if len(r.Allow) == 0 {
		return true
	}
	for _, p := range r.Allow {
		if ok, _ := path.Match(strings.ToLower(p), fn); ok {
			return true
		}
	}
	return false
}

// AccountByKey returns the account with the routing key.
func (c *Config) AccountByKey(key string) (*Account, bool) {
	for i := range c.Accounts {
		if c.Accounts[i].Key == key {
			return &c.Accounts[i], true
		}
	}
	return nil, false
}

func checkListen(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if port == "" {
		return errors.New("port is required")
	}
	return nil
}

func setStr(p *string, v string) {
	if *p == "" {
		*p = v
	}
}

func setInt(p *int, v int) {
	if *p == 0 {
		*p = v
	}
}

func setDur(p *Duration, v time.Duration) {
	if *p == 0 {
		*p = Duration(v)
	}
}

func setSize(p *ByteSize, v int64) {
	if *p == 0 {
		*p = ByteSize(v)
	}
}
