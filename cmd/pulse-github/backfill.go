package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/reconcile"
)

func newBackfillCmd(f *rootFlags) *cobra.Command {
	var account, kinds string
	var since time.Duration
	var maxRequests int
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Queue a bounded, resumable snapshot backfill for an account (run by the service)",
		Long: "Queues a backfill job in the state database. The running service executes it within its request budget, " +
			"one request per turn across accounts, and resumes it after restarts. Backfilled resources are published as " +
			"reconcile.<kind>.snapshot events (current state, no event time); they are not historical events.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, cfg, err := openDB(cmd, f, false)
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			a, ok := cfg.AccountByKey(account)
			if !ok {
				return fmt.Errorf("unknown account %q", account)
			}
			if !cfg.GitHub.App.Enabled {
				return errors.New("backfill uses App installation tokens: enable github.app")
			}
			var ks []string
			if kinds != "" {
				for _, k := range strings.Split(kinds, ",") {
					switch k = strings.TrimSpace(k); k {
					case config.KindRepository, config.KindIssues, config.KindPulls, config.KindReleases, config.KindWorkflowRuns:
						ks = append(ks, k)
					default:
						return fmt.Errorf("unknown kind %q", k)
					}
				}
			}
			if since <= 0 || since > 366*24*time.Hour {
				return errors.New("--since must be between 1s and 366 days (no unbounded history crawl)")
			}
			now := time.Now()
			id, created, err := reconcile.EnqueueBackfill(cmd.Context(), db, a, now.Add(-since), ks, maxRequests, now)
			if err != nil {
				return err
			}
			if !created {
				fmt.Fprintf(cmd.OutOrStdout(), "a backfill for %s is already queued or running (job %d)\n", a.Key, id)
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "queued backfill job %d for %s since %s\n", id, a.Key, now.Add(-since).UTC().Format(time.RFC3339))
			return nil
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "account key (required)")
	cmd.Flags().DurationVar(&since, "since", 7*24*time.Hour, "how far back to look")
	cmd.Flags().StringVar(&kinds, "kinds", "", "comma-separated kinds (default: the account's reconcile.kinds)")
	cmd.Flags().IntVar(&maxRequests, "max-requests", 0, "request budget (default: reconcile.backfill_max_requests)")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}
