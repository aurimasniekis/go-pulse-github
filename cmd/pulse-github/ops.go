package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

func openDB(cmd *cobra.Command, f *rootFlags, readOnly bool) (*storage.DB, *config.Config, error) {
	cfg, err := f.load()
	if err != nil {
		return nil, nil, err
	}
	// Inspection and control commands may run next to the service: SQLite WAL allows concurrent readers and short
	// write transactions, and the service's run lock is not needed for them.
	db, err := storage.Open(cmd.Context(), cfg.Storage.Path, storage.Options{ReadOnly: readOnly})
	if err != nil {
		return nil, nil, err
	}
	return db, cfg, nil
}

func newQuarantineCmd(f *rootFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "quarantine", Short: "Inspect and replay quarantined records"}
	var asJSON bool
	var limit int
	list := &cobra.Command{
		Use:   "list",
		Short: "List quarantined webhook deliveries and publications",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, _, err := openDB(cmd, f, true)
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			items, err := db.ListQuarantine(cmd.Context(), limit)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				for _, it := range items {
					if err := enc.Encode(it); err != nil {
						return err
					}
				}
				return nil
			}
			tw := tabwriter.NewWriter(out, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "KIND\tID\tACCOUNT\tEVENT/SUBJECT\tCLASS\tATTEMPTS\tCREATED\tERROR")
			for _, it := range items {
				what := it.Subject
				if what == "" {
					what = it.Event
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", it.Kind, it.ID, it.Account, what, it.ErrorClass,
					it.Attempts, it.CreatedAt.Format(time.RFC3339), it.Error)
			}
			return tw.Flush()
		},
	}
	list.Flags().BoolVar(&asJSON, "json", false, "one JSON object per line")
	list.Flags().IntVar(&limit, "limit", 500, "maximum records")

	var kind string
	var all bool
	replay := &cobra.Command{
		Use:   "replay [id]",
		Short: "Return a quarantined record (or --all) to processing; publications keep their event ID",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 0) != all {
				return errors.New("give a record id or --all")
			}
			db, _, err := openDB(cmd, f, false)
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			id := ""
			if len(args) == 1 {
				id = args[0]
			}
			n, err := db.ReplayQuarantine(cmd.Context(), kind, id)
			if err != nil {
				return err
			}
			if n == 0 && id != "" {
				return fmt.Errorf("no quarantined record %q", id)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "requeued %d record(s); the running service picks them up within seconds\n", n)
			return nil
		},
	}
	replay.Flags().StringVar(&kind, "kind", "", "inbox or outbox (default: both)")
	replay.Flags().BoolVar(&all, "all", false, "replay every quarantined record")

	var olderThan time.Duration
	var yes bool
	purge := &cobra.Command{
		Use:   "purge",
		Short: "Permanently delete quarantined records older than --older-than",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return errors.New("purging deletes data permanently; pass --yes")
			}
			db, _, err := openDB(cmd, f, false)
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			n, err := db.PurgeQuarantine(cmd.Context(), time.Now().Add(-olderThan))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted %d record(s)\n", n)
			return nil
		},
	}
	purge.Flags().DurationVar(&olderThan, "older-than", 30*24*time.Hour, "minimum age")
	purge.Flags().BoolVar(&yes, "yes", false, "confirm deletion")
	cmd.AddCommand(list, replay, purge)
	return cmd
}

func newStatusCmd(f *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show queue depth, quarantine, checkpoints and installations from the state database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, _, err := openDB(cmd, f, true)
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			ctx := cmd.Context()
			out := cmd.OutOrStdout()
			ob, err := db.OutboxStats(ctx)
			if err != nil {
				return err
			}
			ib, err := db.InboxStats(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "outbox: %d pending (oldest %s), %d quarantined\n", ob.Pending, fmtTime(ob.OldestPending), ob.Quarantined)
			fmt.Fprintf(out, "inbox:  %d pending (oldest %s), %d unrouted, %d quarantined\n", ib.Pending,
				fmtTime(ib.OldestPending), ib.Unrouted, ib.Quarantined)
			accts, _ := db.Accounts(ctx)
			tw := tabwriter.NewWriter(out, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "\nACCOUNT\tGITHUB ID\tLOGIN\tTYPE\tRENAMED FROM")
			for _, a := range accts {
				fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n", a.Key, a.GitHubID, a.Login, a.Type, a.RenamedFrom)
			}
			cps, _ := db.Checkpoints(ctx)
			fmt.Fprintln(tw, "\nACCOUNT\tCOLLECTOR\tFEED\tLAST SUCCESS\tLAST ERROR")
			for _, c := range cps {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", c.AccountKey, c.Collector, c.Feed, fmtTime(c.LastSuccessAt), c.ErrorClass)
			}
			insts, _ := db.Installations(ctx)
			fmt.Fprintln(tw, "\nINSTALLATION\tACCOUNT\tSTATUS\tSELECTION\tLAST RECONCILED")
			for _, in := range insts {
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", in.ID, in.AccountLogin, in.Status, in.RepositorySelection,
					fmtTime(in.LastReconciledAt))
			}
			return tw.Flush()
		},
	}
}

func fmtTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func checkSecrets(cfg *config.Config) error {
	var errs []error
	try := func(what string, ref config.SecretRef) {
		if ref.IsZero() {
			return
		}
		if _, err := ref.Resolve(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", what, err))
		}
	}
	for i, s := range cfg.Webhook.Secrets {
		try(fmt.Sprintf("webhook.secrets[%d]", i), s)
	}
	if cfg.GitHub.App.Enabled {
		try("github.app.private_key", cfg.GitHub.App.PrivateKey)
	}
	if cfg.Admin.Pprof.Enabled {
		try("admin.pprof.token", cfg.Admin.Pprof.Token)
	}
	for _, a := range cfg.Accounts {
		try("accounts."+a.Key+".token", a.Token)
	}
	return errors.Join(errs...)
}
