package main

import "github.com/spf13/cobra"

func newDoctorCmd(*rootFlags) *cobra.Command { return &cobra.Command{Use: "doctor", Hidden: true} }
