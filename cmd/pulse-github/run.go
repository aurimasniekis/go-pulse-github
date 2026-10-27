package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/aurimasniekis/go-pulse-github/internal/app"
	"github.com/aurimasniekis/go-pulse-github/internal/buildinfo"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

func newRunCmd(f *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the collector service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := f.load()
			if err != nil {
				return err
			}
			log := observability.NewLogger(os.Stdout, cfg.Log.Level, cfg.Log.Format, buildinfo.Version)
			lock, err := storage.AcquireLock(cfg.Storage.Path)
			if err != nil {
				return err
			}
			defer lock.Release() //nolint:errcheck
			ctx := cmd.Context()
			db, err := storage.Open(ctx, cfg.Storage.Path, storage.Options{})
			if err != nil {
				return err
			}
			defer db.Close() //nolint:errcheck
			if err := db.WriteInstance(ctx, "run", buildinfo.Version, time.Now()); err != nil {
				return err
			}
			svc, err := app.New(ctx, cfg, db, log, observability.NewMetrics(), app.Deps{})
			if err != nil {
				return err
			}
			return svc.Run(ctx)
		},
	}
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "pulse-github %s\ncommit: %s\nbuilt: %s\ngo: %s\npulse sdk: %s\n",
				buildinfo.Version, buildinfo.Commit, buildinfo.Date, runtime.Version(), buildinfo.PulseSDKVersion())
		},
	}
}

func newConfigCmd(f *rootFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Configuration commands"}
	var secrets bool
	validate := &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration (and, with --check-secrets, that every secret reference resolves)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := f.load()
			if err != nil {
				return err
			}
			if secrets {
				if err := checkSecrets(cfg); err != nil {
					return err
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "configuration is valid: %d account(s)\n", len(cfg.Accounts))
			return nil
		},
	}
	validate.Flags().BoolVar(&secrets, "check-secrets", false, "also read every referenced secret (values are never printed)")
	cmd.AddCommand(validate)
	return cmd
}

func newHealthcheckCmd(f *rootFlags) *cobra.Command {
	return &cobra.Command{
		Use:    "healthcheck",
		Short:  "Probe the local /readyz endpoint (container HEALTHCHECK)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := f.load()
			if err != nil {
				return err
			}
			_, port, err := net.SplitHostPort(cfg.Admin.Listen)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 4*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+port+"/readyz", nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var body map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&body)
			if resp.StatusCode != http.StatusOK {
				return errors.New("not ready: " + fmt.Sprint(body["storage"]))
			}
			return nil
		},
	}
}
