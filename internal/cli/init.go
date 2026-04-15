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

		// Install tracking schema
		if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
			return err
		}

		// Save state file
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		state := &config.State{
			MainDB:   dbName,
			Branches: make(map[string]config.BranchState),
		}
		state.SetPath(cwd)
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		fmt.Printf("Initialized pg-branch on database %q\n", dbName)
		fmt.Printf("State saved to %s\n", config.StateFileName)
		return nil
	},
}
