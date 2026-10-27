package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// ErrInstallationGone means GitHub no longer knows the installation (uninstalled or inaccessible to this App).
var ErrInstallationGone = errors.New("installation not found")

// InstallationSource fetches installation metadata from the GitHub App API.
type InstallationSource interface {
	Installation(ctx context.Context, id int64) (*storage.Installation, error)
}

// Processor normalizes stored deliveries into observations and publications.
type Processor struct {
	DB             *storage.DB
	Router         *routing.Router
	Capturer       *capture.Capturer
	Installations  InstallationSource
	AllowedInstIDs []int64
	PoisonAttempts int
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	Log            *slog.Logger
	Metrics        *observability.Metrics
	Now            func() time.Time
	// OnAccountBound persists a newly pinned account identity.
	OnAccountBound func(ctx context.Context, key string, id int64, login, typ string) error
	// Published wakes the outbox.
	Published func()

	wake chan struct{}
}

// Notify wakes the processor after a delivery was stored.
func (p *Processor) Notify() {
	if p.wake == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Init prepares the wake channel; call before Run and before handing Notify to the receiver.
func (p *Processor) Init() { p.wake = make(chan struct{}, 1) }

// Run processes deliveries until ctx ends.
func (p *Processor) Run(ctx context.Context) error {
	if p.wake == nil {
		p.Init()
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		for ctx.Err() == nil {
			n, err := p.ProcessDue(ctx, 50)
			if err != nil || n == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-p.wake:
		}
	}
}

// ProcessDue processes up to limit due deliveries and returns how many it handled.
func (p *Processor) ProcessDue(ctx context.Context, limit int) (int, error) {
	rows, err := p.DB.DueInbox(ctx, p.Now(), limit)
	if err != nil {
		if p.Metrics != nil && ctx.Err() == nil {
			p.Metrics.SQLiteErrors.WithLabelValues("due_inbox").Inc()
		}
		return 0, err
	}
	for i := range rows {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		p.process(ctx, &rows[i])
	}
	return len(rows), nil
}

type failure struct {
	class     string
	err       error
	permanent bool
}

func (f *failure) Error() string { return f.class + ": " + f.err.Error() }

func (p *Processor) process(ctx context.Context, row *storage.InboxRow) {
	log := p.Log.With(observability.FieldComponent, "webhook_processor", observability.FieldDelivery, row.DeliveryID,
		observability.FieldGitHubEvent, row.Event)
	err := p.handle(ctx, row, log)
	if err == nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	var f *failure
	if !errors.As(err, &f) {
		f = &failure{class: "storage_error", err: err}
		if p.Metrics != nil {
			p.Metrics.SQLiteErrors.WithLabelValues("process_delivery").Inc()
		}
	}
	attempts := row.Attempts + 1
	quarantine := f.permanent || (f.class == "malformed" && attempts >= p.PoisonAttempts)
	next := p.Now().Add(backoff(attempts, p.BackoffInitial, p.BackoffMax))
	if quarantine {
		log.Error("webhook delivery quarantined", observability.FieldErrorClass, f.class, observability.Err(f.err))
	} else {
		log.Warn("webhook delivery processing failed; will retry", observability.FieldErrorClass, f.class,
			observability.Err(f.err), observability.FieldAttempt, attempts)
	}
	if serr := p.DB.InboxFailed(context.WithoutCancel(ctx), row.ID, next, f.class, f.err.Error(), quarantine); serr != nil {
		log.Error("could not record webhook processing failure", observability.Err(serr))
	}
}

func (p *Processor) handle(ctx context.Context, row *storage.InboxRow, log *slog.Logger) error {
	if row.Body == nil {
		return &failure{class: "body_missing", err: errors.New("delivery body was purged"), permanent: true}
	}
	draft, meta, err := events.NormalizeWebhook(events.WebhookInput{
		DeliveryID: row.DeliveryID, HookID: row.HookID, Event: row.Event, Body: row.Body, ReceivedAt: row.ReceivedAt,
	}, events.ModeLive)
	if err != nil {
		return &failure{class: "malformed", err: err}
	}
	now := p.Now()
	life, err := lifecycle(row.Event, meta.Action, row.Body)
	if err != nil {
		return &failure{class: "malformed", err: err}
	}

	var inst *storage.Installation
	if meta.InstallationID != 0 {
		inst, err = p.installation(ctx, meta.InstallationID, life)
		if err != nil && !errors.Is(err, ErrInstallationGone) {
			return &failure{class: "installation_lookup", err: err}
		}
	}
	// installation_target (account renamed) carries the new login in the top-level account object only; the
	// installation object has just its ID. Apply it to the stored installation before routing.
	if life != nil && life.renamed != nil && inst != nil && inst.AccountGitHubID == life.renamed.ID && life.renamed.Login != "" {
		renamed := *inst
		renamed.AccountLogin = life.renamed.Login
		inst = &renamed
		if life.inst == nil {
			life.inst = &renamed
		}
	}
	var key string
	reason := ""
	switch {
	case meta.InstallationID == 0:
		reason = "no_installation"
	case inst == nil:
		reason = "installation_unknown"
	case len(p.AllowedInstIDs) > 0 && !slices.Contains(p.AllowedInstIDs, inst.ID):
		reason = "installation_not_allowed"
	default:
		key = p.accountFor(ctx, inst, log)
		if key == "" {
			reason = "account_not_configured"
		}
	}
	var targets []routing.Target
	if key != "" {
		targets = p.Router.Route(key, events.MatchInstallation, events.SourceWebhook, meta.RepositoryFullName)
	}
	state := storage.InboxProcessed
	if len(targets) == 0 {
		state = storage.InboxUnrouted
		if reason == "" {
			reason = "filtered"
		}
	}
	var res capture.Result
	err = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		if life != nil {
			if err := life.apply(ctx, tx, inst, now); err != nil {
				return err
			}
		}
		if len(targets) > 0 {
			var err error
			res, err = p.Capturer.Store(ctx, tx, draft, targets, row.ID)
			if err != nil {
				return err
			}
		}
		return tx.MarkInbox(ctx, row.ID, state, now)
	})
	if err != nil {
		return err
	}
	if state == storage.InboxUnrouted {
		log.Info("webhook delivery matched no configured account", "reason", reason,
			observability.FieldInstallation, meta.InstallationID)
		return nil
	}
	log.Debug("webhook delivery captured", observability.FieldObservation, res.ObservationID,
		"publications", res.NewPublications)
	if p.Published != nil {
		p.Published()
	}
	return nil
}

// installation returns stored installation metadata, fetching it from the App API when unknown. Lifecycle events
// carry the installation object themselves, which is used when GitHub no longer returns it (installation.deleted).
func (p *Processor) installation(ctx context.Context, id int64, life *lifecycleChange) (*storage.Installation, error) {
	if life != nil && life.inst != nil {
		cur, err := p.DB.Installation(ctx, id)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		in := *life.inst
		if cur != nil && in.AccountGitHubID == 0 {
			in.AccountGitHubID, in.AccountLogin, in.AccountType = cur.AccountGitHubID, cur.AccountLogin, cur.AccountType
		}
		if in.AccountGitHubID != 0 {
			return &in, nil
		}
	}
	in, err := p.DB.Installation(ctx, id)
	if err == nil {
		return in, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	if p.Installations == nil {
		return nil, ErrInstallationGone
	}
	in, err = p.Installations.Installation(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := p.DB.Tx(ctx, func(tx *storage.Tx) error { return tx.UpsertInstallation(ctx, *in, p.Now()) }); err != nil {
		return nil, err
	}
	return in, nil
}

// accountFor maps an installation's account to a configured account, pinning the GitHub ID on first sight.
func (p *Processor) accountFor(ctx context.Context, inst *storage.Installation, log *slog.Logger) string {
	if key, ok := p.Router.AccountForGitHubID(inst.AccountGitHubID); ok {
		// The account ID is immutable; a different login means the account was renamed on GitHub.
		if inst.AccountLogin != "" && p.Router.Ref(key).Login != inst.AccountLogin {
			if p.OnAccountBound != nil {
				if err := p.OnAccountBound(ctx, key, inst.AccountGitHubID, inst.AccountLogin, inst.AccountType); err != nil {
					log.Error("could not record account rename", observability.FieldAccount, key, observability.Err(err))
					return key
				}
			}
			p.Router.Bind(key, inst.AccountGitHubID, inst.AccountLogin)
		}
		return key
	}
	key, ok := p.Router.UnboundAccountForLogin(inst.AccountLogin)
	if !ok {
		return ""
	}
	if p.OnAccountBound != nil {
		if err := p.OnAccountBound(ctx, key, inst.AccountGitHubID, inst.AccountLogin, inst.AccountType); err != nil {
			log.Error("could not pin account identity", observability.FieldAccount, key, observability.Err(err))
			return ""
		}
	}
	if !p.Router.Bind(key, inst.AccountGitHubID, inst.AccountLogin) {
		return ""
	}
	return key
}

// lifecycleChange is an installation or repository-access change carried by a webhook.
type lifecycleChange struct {
	inst        *storage.Installation
	renamed     *ghAccount
	status      string
	reposAdd    []storage.InstallationRepo
	reposRemove []storage.InstallationRepo
}

type ghInstallation struct {
	ID                  int64           `json:"id"`
	AppID               int64           `json:"app_id"`
	Account             *ghAccount      `json:"account"`
	RepositorySelection string          `json:"repository_selection"`
	Permissions         json.RawMessage `json:"permissions"`
	Events              json.RawMessage `json:"events"`
	SuspendedAt         *time.Time      `json:"suspended_at"`
}

type ghAccount struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
	Type  string `json:"type"`
}

type ghRepoRef struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	Private  bool   `json:"private"`
}

// ToStorage converts an App API or webhook installation object.
func (g *ghInstallation) ToStorage() *storage.Installation {
	in := &storage.Installation{
		ID: g.ID, AppID: g.AppID, RepositorySelection: g.RepositorySelection,
		PermissionsJSON: string(g.Permissions), EventsJSON: string(g.Events), Status: storage.InstallActive,
		SuspendedAt: g.SuspendedAt,
	}
	if g.Account != nil {
		in.AccountGitHubID, in.AccountLogin, in.AccountType = g.Account.ID, g.Account.Login, g.Account.Type
	}
	if g.SuspendedAt != nil {
		in.Status = storage.InstallSuspended
	}
	return in
}

// ParseInstallation decodes a GitHub installation object (App API or webhook payload).
func ParseInstallation(raw []byte) (*storage.Installation, error) {
	var g ghInstallation
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, err
	}
	if g.ID == 0 {
		return nil, errors.New("installation object without id")
	}
	return g.ToStorage(), nil
}

func lifecycle(event, action string, body []byte) (*lifecycleChange, error) {
	if event != "installation" && event != "installation_repositories" && event != "installation_target" {
		return nil, nil
	}
	var p struct {
		Installation        *ghInstallation `json:"installation"`
		RepositoriesAdded   []ghRepoRef     `json:"repositories_added"`
		RepositoriesRemoved []ghRepoRef     `json:"repositories_removed"`
		Repositories        []ghRepoRef     `json:"repositories"`
		Account             *ghAccount      `json:"account"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("decode installation payload: %w", err)
	}
	if p.Installation == nil || p.Installation.ID == 0 {
		return nil, errors.New("installation payload without installation id")
	}
	c := &lifecycleChange{}
	if p.Installation.Account != nil {
		c.inst = p.Installation.ToStorage()
	}
	switch event {
	case "installation":
		switch action {
		case "deleted":
			c.status = storage.InstallRemoved
		case "suspend":
			c.status = storage.InstallSuspended
		case "created", "unsuspend", "new_permissions_accepted":
			c.status = storage.InstallActive
		}
		if action == "created" {
			c.reposAdd = repoRefs(p.Installation.ID, p.Repositories, storage.InstallActive)
		}
	case "installation_repositories":
		c.reposAdd = repoRefs(p.Installation.ID, p.RepositoriesAdded, storage.InstallActive)
		c.reposRemove = repoRefs(p.Installation.ID, p.RepositoriesRemoved, storage.InstallRemoved)
	case "installation_target":
		// Account renamed: the payload's account carries the new login; the immutable ID keeps routing stable.
		c.renamed = p.Account
		if p.Account != nil && c.inst != nil {
			c.inst.AccountGitHubID, c.inst.AccountLogin, c.inst.AccountType = p.Account.ID, p.Account.Login, p.Account.Type
		}
	}
	if c.inst != nil && c.status != "" {
		c.inst.Status = c.status
	}
	return c, nil
}

func repoRefs(instID int64, in []ghRepoRef, status string) []storage.InstallationRepo {
	out := make([]storage.InstallationRepo, 0, len(in))
	for _, r := range in {
		out = append(out, storage.InstallationRepo{InstallationID: instID, RepoID: r.ID, FullName: r.FullName,
			Private: r.Private, Status: status})
	}
	return out
}

func (c *lifecycleChange) apply(ctx context.Context, tx *storage.Tx, current *storage.Installation, now time.Time) error {
	switch {
	case c.inst != nil && c.inst.AccountGitHubID != 0:
		if err := tx.UpsertInstallation(ctx, *c.inst, now); err != nil {
			return err
		}
	case current != nil && c.status != "":
		if err := tx.SetInstallationStatus(ctx, current.ID, c.status, now); err != nil {
			return err
		}
	}
	for _, r := range append(c.reposAdd, c.reposRemove...) {
		if err := tx.SetInstallationRepo(ctx, r, now); err != nil {
			return err
		}
	}
	return nil
}

func backoff(attempt int, initial, maxDelay time.Duration) time.Duration {
	if initial <= 0 {
		initial = time.Second
	}
	if maxDelay < initial {
		maxDelay = 5 * time.Minute
	}
	d := initial
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	return min(d, maxDelay)
}
