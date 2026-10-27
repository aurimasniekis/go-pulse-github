package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every metric of the service. Labels are bounded: account labels only take configured account keys,
// event categories map unknown GitHub events to "other", and no delivery IDs, URLs, repositories or actors are used.
type Metrics struct {
	Registry *prometheus.Registry

	WebhookRequests      *prometheus.CounterVec
	Observations         *prometheus.CounterVec
	ObservationDuplicate *prometheus.CounterVec
	Publications         *prometheus.CounterVec
	PublishDuration      prometheus.Histogram
	OutboxPending        *prometheus.GaugeVec
	OutboxOldestSeconds  prometheus.Gauge
	InboxPending         prometheus.Gauge
	InboxOldestSeconds   prometheus.Gauge
	InboxUnrouted        prometheus.Gauge
	QuarantineSize       prometheus.Gauge
	Polls                *prometheus.CounterVec
	PollLastSuccess      *prometheus.GaugeVec
	GitHubRequests       *prometheus.CounterVec
	GitHubLatency        *prometheus.HistogramVec
	GitHubRateRemaining  *prometheus.GaugeVec
	GitHubRateLimited    *prometheus.CounterVec
	CredentialFailures   *prometheus.CounterVec
	ReconcileResources   *prometheus.CounterVec
	ReconcileRequests    *prometheus.CounterVec
	CoverageGaps         *prometheus.CounterVec
	CoverageDegraded     *prometheus.GaugeVec
	SQLiteErrors         *prometheus.CounterVec
	StorageDBBytes       prometheus.Gauge
	StorageFreeBytes     prometheus.Gauge
	AdmissionOpen        prometheus.Gauge
	NATSConnected        prometheus.Gauge
	NATSReconnects       prometheus.Counter
	EnrichmentFetches    *prometheus.CounterVec
	BuildInfo            *prometheus.GaugeVec
}

// NewMetrics registers all metrics on a fresh registry (plus Go and process collectors).
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := factory{r}
	m := &Metrics{
		Registry: r,
		WebhookRequests: f.counter("webhook_requests_total", "Webhook HTTP requests by result.",
			"result"),
		Observations: f.counter("observations_total", "New observations stored, by account, source and bounded event category.",
			"account", "source", "category"),
		ObservationDuplicate: f.counter("observation_duplicates_total", "Observations skipped because their dedup key was already stored.",
			"source"),
		Publications: f.counter("publications_total", "Publication attempts by account and result.",
			"account", "result"),
		PublishDuration: f.histogram("publish_duration_seconds", "Time from publish call to JetStream acknowledgement."),
		OutboxPending:   f.gauge("outbox_pending", "Pending publications per account.", "account"),
		OutboxOldestSeconds: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "outbox_oldest_pending_seconds",
			Help: "Age of the oldest pending publication."}),
		InboxPending: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "inbox_pending",
			Help: "Webhook deliveries stored but not yet normalized."}),
		InboxOldestSeconds: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "inbox_oldest_pending_seconds",
			Help: "Age of the oldest pending webhook delivery."}),
		InboxUnrouted: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "inbox_unrouted",
			Help: "Webhook deliveries that matched no configured account."}),
		QuarantineSize: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "quarantine_size",
			Help: "Quarantined inbox deliveries and publications."}),
		Polls:           f.counter("polls_total", "Collector polls by result.", "account", "collector", "result"),
		PollLastSuccess: f.gauge("poll_last_success_timestamp_seconds", "Unix time of the last successful poll.", "account", "collector", "feed"),
		GitHubRequests: f.counter("github_requests_total", "GitHub API requests by credential and status class.",
			"credential", "status"),
		GitHubLatency: f.histogramVec("github_request_duration_seconds", "GitHub API request latency.", "credential"),
		GitHubRateRemaining: f.gauge("github_rate_limit_remaining", "Last reported primary rate-limit remaining requests.",
			"credential"),
		GitHubRateLimited: f.counter("github_rate_limited_total", "Responses that hit a primary or secondary rate limit.",
			"credential", "kind"),
		CredentialFailures: f.counter("credential_failures_total", "Credential refresh or authentication failures.",
			"credential", "class"),
		ReconcileResources: f.counter("reconcile_resources_total", "Reconciled resources by result.", "account", "kind", "result"),
		ReconcileRequests:  f.counter("reconcile_requests_total", "GitHub requests spent on reconciliation and backfill.", "account", "mode"),
		CoverageGaps:       f.counter("coverage_gaps_total", "Detected or suspected collection gaps.", "account", "collector", "kind"),
		CoverageDegraded: f.gauge("coverage_degraded", "1 while a collector cannot collect (auth, permission, access).",
			"account", "collector"),
		SQLiteErrors: f.counter("sqlite_errors_total", "SQLite operation failures.", "op"),
		StorageDBBytes: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "storage_db_bytes",
			Help: "State database size including WAL."}),
		StorageFreeBytes: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "storage_free_bytes",
			Help: "Free bytes on the state volume."}),
		AdmissionOpen: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "webhook_admission_open",
			Help: "1 while webhook deliveries can be durably accepted."}),
		NATSConnected: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "nats_connected",
			Help: "1 while the Pulse NATS connection is up."}),
		NATSReconnects: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "nats_reconnects_total",
			Help: "NATS reconnects."}),
		EnrichmentFetches: f.counter("enrichment_fetches_total", "Enrichment fetches by result.", "account", "result"),
		BuildInfo:         f.gauge("build_info", "Build metadata.", "version", "commit"),
	}
	return m.register()
}

const ns = "pulse_github"

type factory struct{ r *prometheus.Registry }

func (factory) counter(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
}

func (factory) gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help}, labels)
}

func (factory) histogram(name, help string) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help,
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}})
}

func (factory) histogramVec(name, help string, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help,
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30}}, labels)
}

func (m *Metrics) register() *Metrics {
	m.Registry.MustRegister(m.WebhookRequests, m.Observations, m.ObservationDuplicate, m.Publications, m.PublishDuration,
		m.OutboxPending, m.OutboxOldestSeconds, m.InboxPending, m.InboxOldestSeconds, m.InboxUnrouted, m.QuarantineSize,
		m.Polls, m.PollLastSuccess, m.GitHubRequests, m.GitHubLatency, m.GitHubRateRemaining, m.GitHubRateLimited,
		m.CredentialFailures, m.ReconcileResources, m.ReconcileRequests, m.CoverageGaps, m.CoverageDegraded,
		m.SQLiteErrors, m.StorageDBBytes, m.StorageFreeBytes, m.AdmissionOpen, m.NATSConnected, m.NATSReconnects,
		m.EnrichmentFetches, m.BuildInfo)
	return m
}

// Value is a bounded label value: configured-set membership is the caller's responsibility; this only guards against
// empty labels.
func Value(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
