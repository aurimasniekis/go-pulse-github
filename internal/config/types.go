// Package config loads and validates the versioned pulse-github YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// CurrentVersion is the only configuration schema version this build accepts.
const CurrentVersion = 1

// Config is the root configuration document.
type Config struct {
	Version  int           `yaml:"version"`
	Service  ServiceConfig `yaml:"service"`
	Log      LogConfig     `yaml:"log"`
	Storage  StorageConfig `yaml:"storage"`
	Pulse    PulseConfig   `yaml:"pulse"`
	Outbox   OutboxConfig  `yaml:"outbox"`
	Webhook  WebhookConfig `yaml:"webhook"`
	Admin    AdminConfig   `yaml:"admin"`
	GitHub   GitHubConfig  `yaml:"github"`
	Accounts []Account     `yaml:"accounts"`
	Routing  RoutingConfig `yaml:"routing"`
}

// ServiceConfig holds process-wide settings.
type ServiceConfig struct {
	// Source is the Pulse ce-source of every published event.
	Source          string   `yaml:"source"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

// LogConfig configures log/slog output.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// StorageConfig configures the SQLite state database and its safety limits.
type StorageConfig struct {
	Path            string          `yaml:"path"`
	MinFreeBytes    ByteSize        `yaml:"min_free_bytes"`
	MaxDBBytes      ByteSize        `yaml:"max_db_bytes"`
	MaxInboxPending int             `yaml:"max_inbox_pending"`
	Retention       RetentionConfig `yaml:"retention"`
}

// RetentionConfig bounds how long completed local records are kept.
type RetentionConfig struct {
	Published      Duration `yaml:"published"`
	InboxProcessed Duration `yaml:"inbox_processed"`
	Observations   Duration `yaml:"observations"`
	SweepInterval  Duration `yaml:"sweep_interval"`
}

// PulseConfig configures the connection to an existing Pulse deployment.
type PulseConfig struct {
	URL            string      `yaml:"url"`
	CredsFile      string      `yaml:"creds_file"`
	CAFile         string      `yaml:"ca_file"`
	SubjectDomain  string      `yaml:"subject_domain"`
	PublishTimeout Duration    `yaml:"publish_timeout"`
	Blobs          BlobsConfig `yaml:"blobs"`
	// MaxMessageBytes caps one event message. 0 uses the server's max_payload.
	MaxMessageBytes ByteSize `yaml:"max_message_bytes"`
}

// BlobsConfig enables Pulse blob storage for payloads above the message limit.
type BlobsConfig struct {
	Enabled bool `yaml:"enabled"`
}

// OutboxConfig configures publication workers.
type OutboxConfig struct {
	Workers        int      `yaml:"workers"`
	PoisonAttempts int      `yaml:"poison_attempts"`
	BackoffInitial Duration `yaml:"backoff_initial"`
	BackoffMax     Duration `yaml:"backoff_max"`
	PollInterval   Duration `yaml:"poll_interval"`
}

// WebhookConfig configures the GitHub App webhook receiver.
type WebhookConfig struct {
	Enabled      bool        `yaml:"enabled"`
	Listen       string      `yaml:"listen"`
	Path         string      `yaml:"path"`
	MaxBodyBytes ByteSize    `yaml:"max_body_bytes"`
	ReadTimeout  Duration    `yaml:"read_timeout"`
	Secrets      []SecretRef `yaml:"secrets"`
	// ProcessorPoisonAttempts quarantines a delivery after this many failed normalization attempts.
	ProcessorPoisonAttempts int `yaml:"processor_poison_attempts"`
}

// AdminConfig configures the health, readiness and metrics listener.
type AdminConfig struct {
	Listen string      `yaml:"listen"`
	Pprof  PprofConfig `yaml:"pprof"`
}

// PprofConfig enables token-protected profiling endpoints.
type PprofConfig struct {
	Enabled bool      `yaml:"enabled"`
	Token   SecretRef `yaml:"token"`
}

// GitHubConfig configures GitHub API access.
type GitHubConfig struct {
	APIURL                string    `yaml:"api_url"`
	APIVersion            string    `yaml:"api_version"`
	CAFiles               []string  `yaml:"ca_files"`
	RequestTimeout        Duration  `yaml:"request_timeout"`
	MaxConcurrentRequests int       `yaml:"max_concurrent_requests"`
	RateLimitReserve      int       `yaml:"rate_limit_reserve"`
	App                   AppConfig `yaml:"app"`
}

// AppConfig configures GitHub App authentication.
type AppConfig struct {
	Enabled  bool   `yaml:"enabled"`
	AppID    int64  `yaml:"app_id"`
	ClientID string `yaml:"client_id"`
	// PrivateKey is the PEM private key of the GitHub App.
	PrivateKey SecretRef `yaml:"private_key"`
	// AllowedInstallationIDs restricts accepted installations; empty accepts any installation whose account is configured.
	AllowedInstallationIDs []int64  `yaml:"allowed_installation_ids"`
	ReconcileInterval      Duration `yaml:"reconcile_interval"`
}

// AccountType is a GitHub account type.
type AccountType string

// Account types.
const (
	AccountUser         AccountType = "user"
	AccountOrganization AccountType = "organization"
)

// Account is one monitored GitHub account.
type Account struct {
	// Key is the stable routing identity used in subjects. Defaults to the lowercased login.
	Key      string      `yaml:"key"`
	Login    string      `yaml:"login"`
	Type     AccountType `yaml:"type"`
	GitHubID int64       `yaml:"github_id"`
	// Token is a classic personal access token of this account, used for notifications and personal activity feeds.
	Token         SecretRef           `yaml:"token"`
	Repositories  RepoFilter          `yaml:"repositories"`
	Webhook       AccountWebhook      `yaml:"webhook"`
	Notifications NotificationsConfig `yaml:"notifications"`
	Activity      ActivityConfig      `yaml:"activity"`
	Reconcile     ReconcileConfig     `yaml:"reconcile"`
}

// RepoFilter allows or denies repositories by "owner/name" glob (path.Match syntax, case-insensitive).
type RepoFilter struct {
	Allow []string `yaml:"allow"`
	Deny  []string `yaml:"deny"`
}

// AccountWebhook enables webhook routing to this account.
type AccountWebhook struct {
	Enabled bool `yaml:"enabled"`
}

// NotificationsConfig configures personal notifications polling.
type NotificationsConfig struct {
	Enabled           bool             `yaml:"enabled"`
	Interval          Duration         `yaml:"interval"`
	Overlap           Duration         `yaml:"overlap"`
	MaxPages          int              `yaml:"max_pages"`
	FullSweepInterval Duration         `yaml:"full_sweep_interval"`
	InitialLookback   Duration         `yaml:"initial_lookback"`
	Enrichment        EnrichmentConfig `yaml:"enrichment"`
}

// EnrichmentConfig configures optional, budgeted resource fetches.
type EnrichmentConfig struct {
	Enabled    bool `yaml:"enabled"`
	MaxPerHour int  `yaml:"max_per_hour"`
}

// ActivityConfig configures Events API polling.
type ActivityConfig struct {
	Enabled  bool         `yaml:"enabled"`
	Interval Duration     `yaml:"interval"`
	MaxPages int          `yaml:"max_pages"`
	Feeds    []FeedConfig `yaml:"feeds"`
}

// Feed kinds.
const (
	FeedUserEvents     = "user_events"
	FeedReceivedEvents = "received_events"
	FeedUserOrgEvents  = "user_org_events"
	FeedOrgEvents      = "org_events"
	FeedRepoEvents     = "repo_events"
	FeedAuditLog       = "audit_log"
)

// Feed credentials.
const (
	CredentialToken        = "token"
	CredentialInstallation = "installation"
)

// FeedConfig is one Events API (or audit log) feed.
type FeedConfig struct {
	Kind string `yaml:"kind"`
	// Org is the organization for user_org_events, org_events and audit_log.
	Org string `yaml:"org"`
	// Repo is "owner/name" for repo_events.
	Repo string `yaml:"repo"`
	// Credential selects "token" (the account token, default) or "installation".
	Credential string `yaml:"credential"`
}

// ReconcileConfig configures snapshots and backfill.
type ReconcileConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Interval           Duration `yaml:"interval"`
	Lookback           Duration `yaml:"lookback"`
	Overlap            Duration `yaml:"overlap"`
	MaxRequestsPerRun  int      `yaml:"max_requests_per_run"`
	MaxDurationPerRun  Duration `yaml:"max_duration_per_run"`
	Kinds              []string `yaml:"kinds"`
	BackfillOnStartup  bool     `yaml:"backfill_on_startup"`
	BackfillLookback   Duration `yaml:"backfill_lookback"`
	BackfillMaxRequest int      `yaml:"backfill_max_requests"`
}

// Reconcile resource kinds.
const (
	KindRepository   = "repository"
	KindIssues       = "issues"
	KindPulls        = "pulls"
	KindReleases     = "releases"
	KindWorkflowRuns = "workflow_runs"
)

// RoutingConfig holds explicit cross-account routing.
type RoutingConfig struct {
	Shares []Share `yaml:"shares"`
}

// Share routes observations collected for one account to another account as well.
type Share struct {
	From         string     `yaml:"from"`
	To           string     `yaml:"to"`
	Sources      []string   `yaml:"sources"`
	Repositories RepoFilter `yaml:"repositories"`
}

// SecretRef points at a secret in a mounted file or an environment variable. Literal secrets are rejected.
type SecretRef struct {
	File string `yaml:"file"`
	Env  string `yaml:"env"`
}

// IsZero reports whether the reference is unset.
func (s SecretRef) IsZero() bool { return s.File == "" && s.Env == "" }

// UnmarshalYAML rejects plain scalars so that literal secrets never live in YAML.
func (s *SecretRef) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: secrets must be a reference {file: …} or {env: …}, not a literal value", n.Line)
	}
	type plain SecretRef
	var p plain
	if err := n.Decode(&p); err != nil {
		return err
	}
	*s = SecretRef(p)
	return nil
}

// String describes the reference without revealing the secret.
func (s SecretRef) String() string {
	switch {
	case s.File != "":
		return "file:" + s.File
	case s.Env != "":
		return "env:" + s.Env
	default:
		return "<unset>"
	}
}

// Resolve reads the secret value. Surrounding whitespace is trimmed.
func (s SecretRef) Resolve() ([]byte, error) {
	switch {
	case s.File != "" && s.Env != "":
		return nil, errors.New("secret reference sets both file and env")
	case s.File != "":
		b, err := os.ReadFile(s.File)
		if err != nil {
			return nil, fmt.Errorf("read secret %s: %w", s, err)
		}
		v := []byte(strings.TrimSpace(string(b)))
		if len(v) == 0 {
			return nil, fmt.Errorf("secret %s is empty", s)
		}
		return v, nil
	case s.Env != "":
		v := strings.TrimSpace(os.Getenv(s.Env))
		if v == "" {
			return nil, fmt.Errorf("secret %s is empty or unset", s)
		}
		return []byte(v), nil
	default:
		return nil, errors.New("secret reference is unset")
	}
}

// Duration is a time.Duration that decodes from Go duration strings ("90s", "6h").
type Duration time.Duration

// D returns the time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML parses a Go duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", n.Line, n.Value)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML renders the duration string.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// ByteSize is a byte count that decodes from "512MiB", "1GiB", "4096" and similar.
type ByteSize int64

// UnmarshalYAML parses a byte size.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = ByteSize(v)
	return nil
}

// ParseByteSize parses an integer with an optional KiB/MiB/GiB/TiB (or KB/MB/GB) suffix.
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	}
	mult := int64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			mult = u.mult
			s = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}
	return n * mult, nil
}
