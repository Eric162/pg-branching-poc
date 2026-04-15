package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Set via -ldflags at build time
var (
	Version = "dev"
	Commit  = "none"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print pg-branch version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pg-branch %s (%s)\n", Version, Commit)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
