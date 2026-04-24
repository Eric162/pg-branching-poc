package cli

import (
	"fmt"
	"os"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize pg-branch tracking on a database",
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

		// Load the existing state file (if any) before writing so re-running
		// init on the same DB doesn't wipe the branch list or current_branch.
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		state, err := config.LoadState(cwd)
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}
		state.MainDB = dbName
		state.SetPath(cwd)
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		fmt.Printf("Initialized pg-branch on database %q\n", dbName)
		fmt.Printf("State saved to %s\n", config.StateFileName)
		return nil
	},
}
