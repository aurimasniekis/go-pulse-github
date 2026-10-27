package main

import "github.com/spf13/cobra"

func newDoctorCmd(*rootFlags) *cobra.Command   { return &cobra.Command{Use: "doctor", Hidden: true} }
func newBackfillCmd(*rootFlags) *cobra.Command { return &cobra.Command{Use: "backfill", Hidden: true} }
