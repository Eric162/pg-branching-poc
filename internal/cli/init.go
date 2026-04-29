package cli

import (
	"fmt"
	"os"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
	"github.com/spf13/cobra"
)

var initCwd bool

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize pg-branch tracking on a database",
	Long: `Install pg-branch's tracking schema on the target database and
write a state file recording the connection.

By default the state file lives at $XDG_STATE_HOME/pg-branch/<db>.json
and the central 'current' pointer is updated so subsequent commands
target this database. Pass --cwd to use the legacy per-project layout
(.pg-branch.state.json in the current directory) instead. --state-file
takes precedence over both modes.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		u := mustResolveURL()

		conn, err := pg.Connect(ctx, u)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		dbName, err := conn.DatabaseName()
		if err != nil {
			return err
		}

		// Check if already initialized
		has, err := tracker.HasTrackingSchema(ctx, conn)
		if err != nil {
			return fmt.Errorf("check tracking schema: %w", err)
		}
		if has {
			fmt.Printf("pg-branch already initialized on %q (re-initializing state file)\n", dbName)
		}

		// Install tracking schema (idempotent — uses IF NOT EXISTS)
		if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
			return err
		}

		// Pick the state-file destination *before* writing so re-running
		// init on the same DB doesn't wipe the branch list or current_branch.
		statePath, mode, err := initStatePath(dbName)
		if err != nil {
			return err
		}
		state, err := config.LoadStateFromFile(statePath)
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}
		state.MainDB = dbName
		serverURL, err := pg.ServerURL(u)
		if err != nil {
			return fmt.Errorf("extract server URL: %w", err)
		}
		state.ServerURL = serverURL
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		// Update the central 'current' pointer so naked commands resolve to
		// this DB. We only do this in central mode — --cwd users opted out
		// and --state-file users may be juggling many files manually.
		if mode == "central" {
			if err := config.WriteCurrent(dbName); err != nil {
				return fmt.Errorf("update current pointer: %w", err)
			}
		}

		fmt.Printf("Initialized pg-branch on database %q\n", dbName)
		fmt.Printf("State saved to %s\n", statePath)
		if mode == "central" {
			fmt.Printf("Current context set to %q (use 'pg-branch use <db>' to switch)\n", dbName)
		}
		return nil
	},
}

// initStatePath chooses where init should write state, returning the
// path and a label for the user-facing summary. --state-file wins,
// then --cwd falls back to the legacy in-project layout, then the
// XDG-central per-DB file is the new default.
func initStatePath(dbName string) (string, string, error) {
	if stateFileFlag != "" {
		return stateFileFlag, "explicit", nil
	}
	if initCwd {
		cwd, err := os.Getwd()
		if err != nil {
			return "", "", err
		}
		return cwd + string(os.PathSeparator) + config.StateFileName, "cwd", nil
	}
	p, err := config.CentralStateFile(dbName)
	if err != nil {
		return "", "", err
	}
	return p, "central", nil
}

func init() {
	initCmd.Flags().BoolVar(&initCwd, "cwd", false, "Write state to .pg-branch.state.json in the current directory (legacy per-project layout) instead of the central XDG state directory")
}
