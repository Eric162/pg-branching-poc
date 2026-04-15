package cli

import (
	"github.com/spf13/cobra"
)

var pgURL string

var rootCmd = &cobra.Command{
	Use:   "pg-branch",
	Short: "Local PostgreSQL database branching — branch, diff, merge",
	Long:  "Create isolated database branches from a local Postgres DB, track schema/data changes, and merge them back.",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&pgURL, "pg-url", "",
		"PostgreSQL connection URL (default: from .pg-branch.state.json or PG_BRANCH_URL env)")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(switchCmd)
	rootCmd.AddCommand(connectCmd)
	rootCmd.AddCommand(statusCmd)
}

func Execute() error {
	return rootCmd.Execute()
}

// resolveURL determines the Postgres URL from flags, env, or state file.
func resolveURL() string {
	if pgURL != "" {
		return pgURL
	}
	// Check env
	if u := envOrEmpty("PG_BRANCH_URL"); u != "" {
		return u
	}
	// Try state file
	state, err := loadStateFromCwd()
	if err == nil && state.MainDB != "" {
		return "postgresql://localhost:5432/" + state.MainDB
	}
	return ""
}
