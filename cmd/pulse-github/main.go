// Command pulse-github collects GitHub activity and publishes it to Pulse.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newRoot().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		var ec exitCode
		if errors.As(err, &ec) {
			os.Exit(int(ec))
		}
		os.Exit(1)
	}
}

type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit status %d", int(e)) }

type rootFlags struct {
	config string
}

func newRoot() *cobra.Command {
	f := &rootFlags{}
	root := &cobra.Command{
		Use:           "pulse-github",
		Short:         "Collect GitHub activity into Pulse (NATS JetStream)",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	def := os.Getenv(config.EnvPrefix + "CONFIG")
	if def == "" {
		def = "/etc/pulse-github/config.yaml"
	}
	root.PersistentFlags().StringVarP(&f.config, "config", "c", def, "configuration file ("+config.EnvPrefix+"CONFIG)")
	root.AddCommand(newRunCmd(f), newVersionCmd(), newConfigCmd(f), newQuarantineCmd(f), newStatusCmd(f),
		newHealthcheckCmd(f), newDoctorCmd(f), newBackfillCmd(f))
	return root
}

func (f *rootFlags) load() (*config.Config, error) { return config.Load(f.config) }
