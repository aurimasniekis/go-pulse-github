package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Job modes.
const (
	ModeReconciliation = "reconciliation"
	ModeBackfill       = "backfill"
)

// Snapshots runs bounded, resumable snapshot jobs: periodic reconciliation and on-demand or start-up backfill.
//
// A snapshot is the current state of a resource. The first time a resource is seen it is published as
// reconcile.<kind>.snapshot; when its fingerprint differs from the stored one it is published as
// reconcile.<kind>.changed with provenance.inferred = true. Neither carries an occurred_at: GitHub did not report an
// event, and edit histories cannot be reconstructed. A resource that disappears (404) or becomes inaccessible is
// never reported as deleted.
type Snapshots struct {
	DB       *storage.DB
	Router   *routing.Router
	Capturer *capture.Capturer
	GitHub   *githubapi.Client
	Tokens   TokenSource
	// Installation returns the account's active installation.
	Installation func(ctx context.Context, key string) (int64, error)
	Pressure     collectors.Pressure
	Log          *slog.Logger
	Metrics      *observability.Metrics
	Now          func() time.Time
	Published    func()
	// Tick is the idle wait between scheduling rounds.
	Tick time.Duration
}

type jobCursor struct {
	Repos   []string `json:"repos"`
	Repo    int      `json:"repo"`
	Kind    int      `json:"kind"`
	Next    string   `json:"next,omitempty"`
	Skipped []string `json:"skipped,omitempty"`
}

// Run schedules periodic reconciliation, then steps every active job round-robin (one GitHub request per job per
// turn) so one large account cannot starve the others.
func (s *Snapshots) Run(ctx context.Context) error {
	if s.Tick == 0 {
		s.Tick = 5 * time.Second
	}
	for {
		s.schedule(ctx)
		progressed := s.Round(ctx)
		wait := s.Tick
		if progressed {
			wait = 0
		}
		_ = collectors.Sleep(ctx, wait)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// schedule enqueues a reconciliation job for every account whose interval elapsed.
func (s *Snapshots) schedule(ctx context.Context) {
	now := s.Now()
	for _, key := range s.Router.Keys() {
		a, _ := s.Router.Account(key)
		if !a.Reconcile.Enabled {
			continue
		}
		since := now.Add(-a.Reconcile.Lookback.D())
		last, err := s.DB.LastJob(ctx, key, ModeReconciliation)
		if err == nil {
			if last.Status == storage.JobQueued || last.Status == storage.JobRunning || now.Sub(last.CreatedAt) < a.Reconcile.Interval.D() {
				continue
			}
			// Overlap the previous window so changes during the last run are not missed.
			if t := last.CreatedAt.Add(-a.Reconcile.Overlap.D()); t.After(since) {
				since = t
			}
		} else if !errors.Is(err, storage.ErrNotFound) {
			return
		}
		_, _, _ = s.DB.EnqueueJob(ctx, storage.Job{AccountKey: key, Mode: ModeReconciliation, Kinds: a.Reconcile.Kinds,
			Since: since, MaxRequests: a.Reconcile.MaxRequestsPerRun}, now)
	}
}

// EnqueueBackfill queues a bounded backfill for an account.
func EnqueueBackfill(ctx context.Context, db *storage.DB, a *config.Account, since time.Time, kinds []string, maxRequests int, now time.Time) (int64, bool, error) {
	if len(kinds) == 0 {
		kinds = a.Reconcile.Kinds
	}
	if maxRequests <= 0 {
		maxRequests = a.Reconcile.BackfillMaxRequest
	}
	return db.EnqueueJob(ctx, storage.Job{AccountKey: a.Key, Mode: ModeBackfill, Kinds: kinds, Since: since, MaxRequests: maxRequests}, now)
}

// Round advances every active job by one step and reports whether any progressed.
func (s *Snapshots) Round(ctx context.Context) bool {
	if s.Pressure != nil && s.Pressure.Pressure() {
		return false
	}
	jobs, err := s.DB.ActiveJobs(ctx)
	if err != nil {
		return false
	}
	progressed := false
	for i := range jobs {
		if ctx.Err() != nil {
			return false
		}
		if s.step(ctx, &jobs[i]) {
			progressed = true
		}
	}
	return progressed
}

func (s *Snapshots) budgetFor(j *storage.Job) time.Duration {
	if a, ok := s.Router.Account(j.AccountKey); ok {
		return a.Reconcile.MaxDurationPerRun.D()
	}
	return 10 * time.Minute
}

// step performs one request of a job and stores its results and progress in one transaction.
func (s *Snapshots) step(ctx context.Context, j *storage.Job) bool {
	log := s.Log.With(observability.FieldComponent, "reconcile", observability.FieldAccount, j.AccountKey, "job_id", j.ID, "mode", j.Mode)
	now := s.Now()
	a, ok := s.Router.Account(j.AccountKey)
	if !ok {
		_ = s.DB.FinishJob(ctx, j.ID, storage.JobFailed, "account no longer configured", now)
		return false
	}
	var cur jobCursor
	if j.Cursor != "" {
		_ = json.Unmarshal([]byte(j.Cursor), &cur)
	}
	instID, err := s.Installation(ctx, j.AccountKey)
	if err != nil {
		_ = s.DB.FinishJob(ctx, j.ID, storage.JobFailed, err.Error(), now)
		log.Warn("reconciliation needs an App installation", observability.Err(err))
		return false
	}
	cred := s.Tokens.Credential(instID, "installation:"+j.AccountKey)
	if j.Cursor == "" {
		repos, err := s.repos(ctx, instID, a)
		if err != nil {
			_ = s.DB.FinishJob(ctx, j.ID, storage.JobFailed, err.Error(), now)
			return false
		}
		cur.Repos = repos
	}
	// Budgets: requests per job, and wall-clock per run (a running job older than the budget stops).
	if j.RequestsUsed >= j.MaxRequests || (j.Mode == ModeReconciliation && now.Sub(j.CreatedAt) > s.budgetFor(j)) {
		return s.finishBudget(ctx, j, &cur, log)
	}
	if cur.Repo >= len(cur.Repos) {
		_ = s.DB.FinishJob(ctx, j.ID, storage.JobDone, strings.Join(cur.Skipped, "; "), now)
		log.Info("snapshot job finished", "requests", j.RequestsUsed, "repos", len(cur.Repos))
		return false
	}
	repo := cur.Repos[cur.Repo]
	kind := j.Kinds[cur.Kind]
	reqURL := cur.Next
	if reqURL == "" {
		reqURL = firstURL(kind, repo, j.Since)
	}
	resp, err := s.GitHub.Do(ctx, cred, githubapi.Request{URL: reqURL})
	if s.Metrics != nil {
		s.Metrics.ReconcileRequests.WithLabelValues(j.AccountKey, j.Mode).Inc()
	}
	var items []json.RawMessage
	advance := false
	switch {
	case err == nil:
		items, err = itemsOf(kind, resp.Body)
		if err != nil {
			advance = true
			cur.Skipped = append(cur.Skipped, fmt.Sprintf("%s %s: %v", repo, kind, err))
			err = nil
		}
	case githubapi.IsClass(err, githubapi.ClassNotFound), githubapi.IsClass(err, githubapi.ClassForbidden),
		githubapi.IsClass(err, githubapi.ClassGone):
		// Inaccessible or gone: ambiguous, so it is skipped and noted, never reported as deleted.
		cur.Skipped = append(cur.Skipped, fmt.Sprintf("%s %s: %s", repo, kind, githubapi.ClassOf(err)))
		if s.Metrics != nil {
			s.Metrics.ReconcileResources.WithLabelValues(j.AccountKey, kind, "unavailable").Inc()
		}
		advance, err = true, nil
	default:
		if ctx.Err() == nil {
			log.Warn("snapshot request failed; job resumes later", observability.FieldErrorClass, githubapi.ClassOf(err), observability.Err(err))
		}
		return false
	}
	stop := false
	var stored int
	err = s.DB.Tx(ctx, func(tx *storage.Tx) error {
		for _, raw := range items {
			done, n, err := s.storeResource(ctx, tx, j, kind, repo, instID, raw)
			if err != nil {
				return err
			}
			stored += n
			if done {
				stop = true
				break
			}
		}
		switch {
		case advance || stop || resp == nil || resp.Next == "" || kind == config.KindRepository:
			cur.Next = ""
			cur.Kind++
			if cur.Kind >= len(j.Kinds) {
				cur.Kind = 0
				cur.Repo++
			}
		default:
			cur.Next = resp.Next
		}
		b, _ := json.Marshal(cur)
		j.Cursor = string(b)
		j.RequestsUsed++
		return tx.UpdateJobProgress(ctx, j.ID, j.Cursor, j.RequestsUsed, storage.JobRunning, s.Now())
	})
	if err != nil {
		log.Error("snapshot store failed", observability.Err(err))
		return false
	}
	if stored > 0 && s.Published != nil {
		s.Published()
	}
	return true
}

func (s *Snapshots) finishBudget(ctx context.Context, j *storage.Job, cur *jobCursor, log *slog.Logger) bool {
	remaining := len(cur.Repos) - cur.Repo
	detail := fmt.Sprintf("%s job %d stopped by its budget after %d requests; %d of %d repositories not completed",
		j.Mode, j.ID, j.RequestsUsed, remaining, len(cur.Repos))
	_ = s.DB.Tx(ctx, func(tx *storage.Tx) error {
		return s.Capturer.RecordGap(ctx, tx, s.Router, storage.Gap{AccountKey: j.AccountKey, Collector: "reconcile", Feed: j.Mode,
			Kind: capture.GapRequestBudget, WindowStart: &j.Since, Detail: detail})
	})
	_ = s.DB.FinishJob(ctx, j.ID, storage.JobDone, detail, s.Now())
	log.Warn("snapshot job budget exhausted", "detail", detail)
	if s.Published != nil {
		s.Published()
	}
	return false
}

// repos lists the installation's repositories that pass the account's filter, sorted for a stable resume order.
func (s *Snapshots) repos(ctx context.Context, instID int64, a *config.Account) ([]string, error) {
	stored, err := s.DB.InstallationRepos(ctx, instID)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range stored {
		if r.Status == storage.InstallActive && a.Repositories.Allows(r.FullName) {
			out = append(out, r.FullName)
		}
	}
	slices.Sort(out)
	return out, nil
}

func firstURL(kind, repo string, since time.Time) string {
	owner, name, _ := strings.Cut(repo, "/")
	base := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(name)
	ts := since.UTC().Format(time.RFC3339)
	switch kind {
	case config.KindRepository:
		return base
	case config.KindIssues:
		// Includes pull requests (as issues); ascending order makes the next-page cursor stable to resume.
		return base + "/issues?" + url.Values{"state": {"all"}, "sort": {"updated"}, "direction": {"asc"}, "since": {ts}, "per_page": {"100"}}.Encode()
	case config.KindPulls:
		return base + "/pulls?" + url.Values{"state": {"all"}, "sort": {"updated"}, "direction": {"desc"}, "per_page": {"100"}}.Encode()
	case config.KindReleases:
		return base + "/releases?per_page=100"
	case config.KindWorkflowRuns:
		return base + "/actions/runs?" + url.Values{"created": {">=" + since.UTC().Format("2006-01-02")}, "per_page": {"100"}}.Encode()
	}
	return base
}

func itemsOf(kind string, body []byte) ([]json.RawMessage, error) {
	switch kind {
	case config.KindRepository:
		return []json.RawMessage{body}, nil
	case config.KindWorkflowRuns:
		var w struct {
			Runs []json.RawMessage `json:"workflow_runs"`
		}
		err := json.Unmarshal(body, &w)
		return w.Runs, err
	default:
		return githubapi.SplitArray(body)
	}
}

// resourceKind maps a reconcile kind to the published resource kind.
func resourceKind(kind string, raw json.RawMessage) string {
	switch kind {
	case config.KindIssues:
		var o struct {
			PullRequest json.RawMessage `json:"pull_request"`
		}
		_ = json.Unmarshal(raw, &o)
		if len(o.PullRequest) > 0 && string(o.PullRequest) != "null" {
			return "pull_request"
		}
		return "issue"
	case config.KindPulls:
		return "pull_request"
	case config.KindReleases:
		return "release"
	case config.KindWorkflowRuns:
		return "workflow_run"
	default:
		return "repository"
	}
}

// storeResource stores one snapshot. It reports done when a newest-first listing went past the job's window.
func (s *Snapshots) storeResource(ctx context.Context, tx *storage.Tx, j *storage.Job, kind, repo string, instID int64, raw json.RawMessage) (bool, int, error) {
	var o struct {
		ID          json.Number `json:"id"`
		State       string      `json:"state"`
		UpdatedAt   string      `json:"updated_at"`
		CreatedAt   string      `json:"created_at"`
		PublishedAt string      `json:"published_at"`
		Status      string      `json:"status"`
		Conclusion  string      `json:"conclusion"`
		Draft       bool        `json:"draft"`
		Name        string      `json:"name"`
		PushedAt    string      `json:"pushed_at"`
	}
	if json.Unmarshal(raw, &o) != nil || o.ID == "" {
		return false, 0, nil //nolint:nilerr // not a resource object: skipped, not fatal to the job
	}
	if kind == config.KindPulls || kind == config.KindReleases {
		ref := o.UpdatedAt
		if kind == config.KindReleases {
			ref = o.CreatedAt
		}
		if t := events.ParseTime(ref); t != nil && t.Before(j.Since) {
			return true, 0, nil
		}
	}
	rk := resourceKind(kind, raw)
	fp := events.Fingerprint(o.UpdatedAt, o.State, o.Status, o.Conclusion, o.PublishedAt, fmt.Sprint(o.Draft), o.Name, o.PushedAt)
	prev, err := tx.ResourceFingerprint(ctx, j.AccountKey, rk, o.ID.String())
	if err != nil {
		return false, 0, err
	}
	if prev == fp {
		if s.Metrics != nil {
			s.Metrics.ReconcileResources.WithLabelValues(j.AccountKey, rk, "unchanged").Inc()
		}
		return false, 0, nil
	}
	action := "snapshot"
	if prev != "" {
		action = "changed"
	}
	mode := events.ModeReconciliation
	if j.Mode == ModeBackfill {
		mode = events.ModeBackfill
	}
	d := &events.Draft{
		Source: events.SourceReconcile, Event: rk, Action: action,
		DedupKey:   "reconcile:" + j.AccountKey + ":" + rk + ":" + o.ID.String() + ":" + fp,
		ObservedAt: s.Now(), Mode: mode,
		SourceRef:  events.SourceRef{InstallationID: instID},
		GitHub:     events.GitHubRef{Event: rk},
		Repository: &events.Repository{FullName: repo},
		Resource:   events.ResourceFrom(rk, raw),
		Provenance: events.Provenance{Snapshot: true, Inferred: action == "changed", Fingerprint: fp, PreviousFingerprint: prev,
			Qualifiers: []string{"current_state", "no_event_time"}},
		Raw: raw,
	}
	res, err := s.Capturer.Store(ctx, tx, d, s.Router.Route(j.AccountKey, events.MatchInstallation, events.SourceReconcile, repo), 0)
	if err != nil {
		return false, 0, err
	}
	if err := tx.PutResourceFingerprint(ctx, j.AccountKey, rk, o.ID.String(), fp, s.Now()); err != nil {
		return false, 0, err
	}
	if s.Metrics != nil {
		s.Metrics.ReconcileResources.WithLabelValues(j.AccountKey, rk, action).Inc()
	}
	return false, res.NewPublications, nil
}
