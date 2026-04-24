package cli

import (
	"os"

	"github.com/pg-branch/pg-branch/internal/pg"
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
//
// Resolution order:
//  1. --pg-url flag
//  2. PG_BRANCH_URL env var
//  3. ServerURL saved in the state file at init time, joined with MainDB
//  4. Legacy fallback for state files written before ServerURL was stored:
//     synthesize postgresql://localhost:5432/<MainDB>
func resolveURL() string {
	if pgURL != "" {
		return pgURL
	}
	if u := os.Getenv("PG_BRANCH_URL"); u != "" {
		return u
	}
	state, err := loadStateFromCwd()
	if err != nil || state.MainDB == "" {
		return ""
	}
	if state.ServerURL != "" {
		if u, err := pg.URLForDatabase(state.ServerURL, state.MainDB); err == nil {
			return u
		}
	}
	return "postgresql://localhost:5432/" + state.MainDB
}
