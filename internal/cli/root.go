package cli

import (
	"os"

	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/spf13/cobra"
)

var (
	pgURL         string
	stateFileFlag string
)

var rootCmd = &cobra.Command{
	Use:   "pg-branch",
	Short: "Local PostgreSQL database branching — branch, diff, merge",
	Long:  "Create isolated database branches from a local Postgres DB, track schema/data changes, and merge them back.",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&pgURL, "pg-url", "",
		"PostgreSQL connection URL (default: from active state file or PG_BRANCH_URL env)")
	rootCmd.PersistentFlags().StringVar(&stateFileFlag, "state-file", "",
		"Path to state file to read/write (default: PG_BRANCH_STATE_FILE env, then CWD .pg-branch.state.json, then central $XDG_STATE_HOME/pg-branch/<current>.json)")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(switchCmd)
	rootCmd.AddCommand(connectCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(useCmd)
}

func Execute() error {
	return rootCmd.Execute()
}

// resolveURL determines the Postgres URL from flags, env, or state file.
//
// Resolution order:
//  1. --pg-url flag
//  2. PG_BRANCH_URL env var
//  3. ServerURL saved in the active state file at init time, joined
//     with MainDB. The active state file is itself resolved via
//     resolveStateFile (--state-file → env → CWD legacy → central
//     pointer), so this works whether the user is in a project
//     directory with a per-repo file or on a fresh shell with the
//     XDG-central layout.
//  4. Legacy fallback for state files written before ServerURL was
//     stored: synthesize postgresql://localhost:5432/<MainDB>.
func resolveURL() string {
	if pgURL != "" {
		return pgURL
	}
	if u := os.Getenv("PG_BRANCH_URL"); u != "" {
		return u
	}
	state, err := loadActiveState()
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
