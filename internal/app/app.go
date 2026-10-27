// Package app wires the service together and owns its lifecycle: start-up checks, bounded workers, and graceful
// shutdown on SIGTERM.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/admin"
	"github.com/aurimasniekis/go-pulse-github/internal/buildinfo"
	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors/webhook"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/outbox"
	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/reconcile"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Runner is a long-running component.
type Runner interface {
	Run(ctx context.Context) error
}

type namedRunner struct {
	name string
	r    Runner
}

// Service is the assembled process.
type Service struct {
	Cfg        *config.Config
	Log        *slog.Logger
	Metrics    *observability.Metrics
	DB         *storage.DB
	Router     *routing.Router
	Capturer   *capture.Capturer
	Guard      *admin.Guard
	Publisher  publisher.Publisher
	Dispatcher *outbox.Dispatcher
	GitHub     *githubapi.Client
	App        *githubauth.App
	Tokens     *githubauth.Installations
	AppAPI     *reconcile.AppAPI
	Processor  *webhook.Processor
	Receiver   *webhook.Receiver
	Now        func() time.Time

	pats    map[string]*githubauth.PAT
	runners []namedRunner
	closers []func()
}

// Deps lets tests replace external dependencies.
type Deps struct {
	Publisher  publisher.Publisher
	HTTPClient *http.Client
	Now        func() time.Time
}

// New assembles the service on an open database. The caller holds the process lock.
func New(ctx context.Context, cfg *config.Config, db *storage.DB, log *slog.Logger, m *observability.Metrics, deps Deps) (*Service, error) {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	s := &Service{Cfg: cfg, Log: log, Metrics: m, DB: db, Now: now}
	m.BuildInfo.WithLabelValues(buildinfo.Version, buildinfo.Commit).Set(1)
	s.Router = routing.New(cfg)
	if err := s.syncAccounts(ctx); err != nil {
		return nil, err
	}
	s.Capturer = &capture.Capturer{Domain: cfg.Pulse.SubjectDomain, Metrics: m, Now: now}
	s.Guard = admin.NewGuard(db, admin.GuardConfig{MinFreeBytes: int64(cfg.Storage.MinFreeBytes),
		MaxDBBytes: int64(cfg.Storage.MaxDBBytes), MaxInboxPending: cfg.Storage.MaxInboxPending}, m, log, now)

	hc := deps.HTTPClient
	if hc == nil {
		var err error
		if hc, err = GitHubHTTPClient(cfg.GitHub); err != nil {
			return nil, err
		}
	}
	gh, err := githubapi.New(githubapi.Options{
		BaseURL: cfg.GitHub.APIURL, APIVersion: cfg.GitHub.APIVersion,
		UserAgent:     "pulse-github/" + buildinfo.Version,
		HTTPClient:    hc,
		MaxConcurrent: cfg.GitHub.MaxConcurrentRequests, RateReserve: cfg.GitHub.RateLimitReserve,
		Logger: log, Metrics: m, Now: now,
	})
	if err != nil {
		return nil, err
	}
	s.GitHub = gh
	if cfg.GitHub.App.Enabled {
		pem, err := cfg.GitHub.App.PrivateKey.Resolve()
		if err != nil {
			return nil, fmt.Errorf("github.app.private_key: %w", err)
		}
		key, err := githubauth.ParsePrivateKey(pem)
		if err != nil {
			return nil, fmt.Errorf("github.app.private_key: %w", err)
		}
		s.App = githubauth.NewApp(cfg.GitHub.App.AppID, cfg.GitHub.App.ClientID, key, now)
		s.Tokens = githubauth.NewInstallations(gh, s.App, now)
		s.AppAPI = &reconcile.AppAPI{Client: gh, Cred: s.App.Credential()}
	}

	if deps.Publisher != nil {
		s.Publisher = deps.Publisher
	} else {
		p, err := publisher.Connect(publisher.Options{URL: cfg.Pulse.URL, CredsFile: cfg.Pulse.CredsFile, CAFile: cfg.Pulse.CAFile,
			Source: cfg.Service.Source, Name: cfg.Service.Source, Logger: log, Metrics: m})
		if err != nil {
			return nil, fmt.Errorf("pulse: %w", err)
		}
		s.Publisher = p
		s.closers = append(s.closers, p.Close)
	}
	s.Dispatcher = outbox.New(db, s.Publisher, outbox.Config{
		Workers: cfg.Outbox.Workers, BackoffInitial: cfg.Outbox.BackoffInitial.D(), BackoffMax: cfg.Outbox.BackoffMax.D(),
		PollInterval: cfg.Outbox.PollInterval.D(), PublishTimeout: cfg.Pulse.PublishTimeout.D(),
		BlobsEnabled: cfg.Pulse.Blobs.Enabled, MaxMessageBytes: int64(cfg.Pulse.MaxMessageBytes),
	}, log, m, now)
	s.add("outbox", s.Dispatcher)

	if cfg.Webhook.Enabled {
		secrets := make([][]byte, 0, len(cfg.Webhook.Secrets))
		for i, ref := range cfg.Webhook.Secrets {
			b, err := ref.Resolve()
			if err != nil {
				return nil, fmt.Errorf("webhook.secrets[%d]: %w", i, err)
			}
			secrets = append(secrets, b)
		}
		ver, err := githubauth.NewWebhookVerifier(secrets...)
		if err != nil {
			return nil, err
		}
		s.Processor = &webhook.Processor{
			DB: db, Router: s.Router, Capturer: s.Capturer, AllowedInstIDs: cfg.GitHub.App.AllowedInstallationIDs,
			PoisonAttempts: cfg.Webhook.ProcessorPoisonAttempts, BackoffInitial: cfg.Outbox.BackoffInitial.D(),
			BackoffMax: cfg.Outbox.BackoffMax.D(), Log: log, Metrics: m, Now: now,
			OnAccountBound: s.bindAccount, Published: s.Dispatcher.Notify,
		}
		if s.AppAPI != nil {
			s.Processor.Installations = s.AppAPI
		}
		s.Processor.Init()
		// Routing may have changed since the last run (accounts, shares, filters): give unrouted deliveries another try.
		if n, err := db.RequeueUnrouted(ctx, s.requeueLimit()); err != nil {
			return nil, fmt.Errorf("requeue unrouted deliveries: %w", err)
		} else if n > 0 {
			log.Info("requeued unrouted webhook deliveries", "count", n)
		}
		s.Receiver = &webhook.Receiver{DB: db, Verifier: ver, MaxBody: int64(cfg.Webhook.MaxBodyBytes), Admission: s.Guard,
			Metrics: m, Log: log.With(observability.FieldComponent, "webhook_receiver"), Now: now, Notify: s.Processor.Notify}
		s.add("webhook_processor", s.Processor)
	}
	if err := s.addCollectors(ctx); err != nil {
		return nil, err
	}
	s.add("stats", runnerFunc(s.statsLoop))
	s.add("retention", runnerFunc(s.retentionLoop))
	return s, nil
}

// requeueLimit bounds one requeue of unrouted deliveries to half the inbox admission limit.
func (s *Service) requeueLimit() int { return max(1, s.Cfg.Storage.MaxInboxPending/2) }

type runnerFunc func(ctx context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

func (s *Service) add(name string, r Runner) { s.runners = append(s.runners, namedRunner{name, r}) }

// syncAccounts loads pinned identities and pins configured GitHub IDs, refusing contradictions.
func (s *Service) syncAccounts(ctx context.Context) error {
	recs, err := s.DB.Accounts(ctx)
	if err != nil {
		return err
	}
	for _, r := range recs {
		if _, ok := s.Router.Account(r.Key); !ok || r.GitHubID == 0 {
			continue
		}
		if !s.Router.Bind(r.Key, r.GitHubID, r.Login) {
			return fmt.Errorf("account %q: stored github id %d conflicts with configuration", r.Key, r.GitHubID)
		}
	}
	for _, key := range s.Router.Keys() {
		a, _ := s.Router.Account(key)
		ref := s.Router.Ref(key)
		if _, err := s.DB.SyncAccount(ctx, storage.AccountRecord{Key: key, GitHubID: ref.GitHubID, Login: a.Login,
			Type: string(a.Type)}, s.Now()); err != nil {
			return err
		}
	}
	return nil
}

// bindAccount persists a newly learned account ID (from an authenticated GitHub source).
func (s *Service) bindAccount(ctx context.Context, key string, id int64, login, typ string) error {
	a, _ := s.Router.Account(key)
	if typ == "" {
		typ = string(a.Type)
	}
	renamed, err := s.DB.SyncAccount(ctx, storage.AccountRecord{Key: key, GitHubID: id, Login: login, Type: normalizeType(typ)}, s.Now())
	if err != nil {
		return err
	}
	if renamed != "" {
		s.Log.Info("account login changed; routing key unchanged", observability.FieldAccount, key, "previous_login", renamed,
			"login", login)
	}
	return nil
}

func normalizeType(t string) string {
	switch t {
	case "Organization", "organization":
		return string(config.AccountOrganization)
	default:
		return string(config.AccountUser)
	}
}

// GitHubHTTPClient builds the TLS-verifying HTTP client for the GitHub API (system roots plus github.ca_files).
func GitHubHTTPClient(c config.GitHubConfig) (*http.Client, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	for _, f := range c.CAFiles {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("github.ca_files: %w", err)
		}
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("github.ca_files: %s has no PEM certificates", f)
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	tr.MaxIdleConnsPerHost = 16
	tr.ResponseHeaderTimeout = c.RequestTimeout.D()
	return &http.Client{Transport: tr, Timeout: c.RequestTimeout.D()}, nil
}

// Run serves until ctx is cancelled (SIGTERM), then shuts down in order: close admission, stop accepting webhooks
// (in-flight deliveries finish committing), stop workers (in-flight publishes finish), drain NATS.
func (s *Service) Run(ctx context.Context) error {
	if err := s.Guard.Check(ctx); err != nil {
		s.Log.Error("storage is not ready at start; webhook admission stays closed until it recovers", observability.Err(err))
	}
	workCtx, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	errCh := make(chan error, 4)
	var wg sync.WaitGroup
	for _, nr := range s.runners {
		wg.Add(1)
		go func(nr namedRunner) {
			defer wg.Done()
			if err := nr.r.Run(workCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.Log.Error("component stopped", observability.FieldComponent, nr.name, observability.Err(err))
				errCh <- fmt.Errorf("%s: %w", nr.name, err)
			}
		}(nr)
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = s.Guard.Run(workCtx, 5*time.Second) }()

	var token []byte
	if s.Cfg.Admin.Pprof.Enabled {
		var err error
		if token, err = s.Cfg.Admin.Pprof.Token.Resolve(); err != nil {
			stopWork()
			return fmt.Errorf("admin.pprof.token: %w", err)
		}
	}
	var natsStatus admin.Status
	if p, ok := s.Publisher.(admin.Status); ok {
		natsStatus = p
	}
	adminSrv := &http.Server{Addr: s.Cfg.Admin.Listen, Handler: admin.Handler(s.Guard, s.Metrics, natsStatus, token),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 60 * time.Second}
	servers := []*http.Server{adminSrv}
	var hookSrv *http.Server
	if s.Receiver != nil {
		mux := http.NewServeMux()
		mux.Handle(s.Cfg.Webhook.Path, s.Receiver)
		hookSrv = &http.Server{Addr: s.Cfg.Webhook.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout: s.Cfg.Webhook.ReadTimeout.D(), WriteTimeout: s.Cfg.Webhook.ReadTimeout.D() + 10*time.Second,
			IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10}
		servers = append(servers, hookSrv)
	}
	for _, srv := range servers {
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", srv.Addr)
		if err != nil {
			stopWork()
			wg.Wait()
			return fmt.Errorf("listen %s: %w", srv.Addr, err)
		}
		s.Log.Info("listening", "addr", ln.Addr().String())
		go func(srv *http.Server) {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}(srv)
	}
	s.Log.Info("pulse-github started", "accounts", len(s.Cfg.Accounts), "webhook", s.Receiver != nil,
		"pulse_sdk", buildinfo.PulseSDKVersion())

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	}
	s.Log.Info("shutting down")
	deadline, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.Cfg.Service.ShutdownTimeout.D())
	defer cancel()
	s.Guard.SetShuttingDown()
	if hookSrv != nil {
		if err := hookSrv.Shutdown(deadline); err != nil {
			s.Log.Warn("webhook listener shutdown", observability.Err(err))
		}
	}
	stopWork()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-deadline.Done():
		s.Log.Warn("shutdown timeout reached with work in flight; unfinished work resumes from the database on restart")
	}
	_ = adminSrv.Shutdown(deadline)
	for _, c := range s.closers {
		c()
	}
	s.Log.Info("stopped")
	return runErr
}

func (s *Service) statsLoop(ctx context.Context) error {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		s.updateStats(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (s *Service) updateStats(ctx context.Context) {
	now := s.Now()
	if st, err := s.DB.OutboxStats(ctx); err == nil {
		s.Metrics.OutboxOldestSeconds.Set(age(now, st.OldestPending))
		s.Metrics.QuarantineSize.Set(float64(st.Quarantined))
	}
	if per, err := s.DB.PendingByAccount(ctx); err == nil {
		for _, k := range s.Router.Keys() {
			s.Metrics.OutboxPending.WithLabelValues(k).Set(float64(per[k]))
		}
	}
	if st, err := s.DB.InboxStats(ctx); err == nil {
		s.Metrics.InboxPending.Set(float64(st.Pending))
		s.Metrics.InboxOldestSeconds.Set(age(now, st.OldestPending))
		s.Metrics.InboxUnrouted.Set(float64(st.Unrouted))
	}
	if n, err := s.DB.QuarantineCount(ctx); err == nil {
		s.Metrics.QuarantineSize.Set(float64(n))
	}
	if cps, err := s.DB.Checkpoints(ctx); err == nil {
		for _, cp := range cps {
			if _, ok := s.Router.Account(cp.AccountKey); ok && cp.LastSuccessAt != nil {
				s.Metrics.PollLastSuccess.WithLabelValues(cp.AccountKey, cp.Collector, cp.Feed).Set(float64(cp.LastSuccessAt.Unix()))
			}
		}
	}
}

func age(now time.Time, t *time.Time) float64 {
	if t == nil {
		return 0
	}
	return now.Sub(*t).Seconds()
}

func (s *Service) retentionLoop(ctx context.Context) error {
	t := time.NewTicker(s.Cfg.Storage.Retention.SweepInterval.D())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		r, err := s.DB.Sweep(ctx, s.Now(), storage.RetentionPolicy{Published: s.Cfg.Storage.Retention.Published.D(),
			InboxProcessed: s.Cfg.Storage.Retention.InboxProcessed.D(), Observations: s.Cfg.Storage.Retention.Observations.D()})
		if err != nil {
			if ctx.Err() == nil {
				s.Metrics.SQLiteErrors.WithLabelValues("sweep").Inc()
				s.Log.Error("retention sweep failed", observability.Err(err))
			}
			continue
		}
		s.Log.Info("retention sweep", "outbox_deleted", r.Outbox, "inbox_bodies_dropped", r.InboxBodies,
			"observations_deleted", r.Observations)
	}
}
