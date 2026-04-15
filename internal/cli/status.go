package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current branch and pending changes",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		if state.CurrentBranch == "" {
			fmt.Printf("On main (%s)\n", state.MainDB)
			fmt.Println("No branch selected.")
			return nil
		}

		bs, _ := state.GetBranch(state.CurrentBranch)
		fmt.Printf("On branch %q (%s)\n", state.CurrentBranch, bs.DBName)
		fmt.Printf("Parent: %s\n", bs.ParentDB)
		fmt.Printf("Created: %s\n", bs.CreatedAt)

		// Connect to branch DB to read DDL log
		conn, err := pg.Connect(ctx, u)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		branchConn, err := conn.ConnectToDatabase(ctx, bs.DBName)
		if err != nil {
			return fmt.Errorf("connect to branch: %w", err)
		}
		defer branchConn.Close()

		entries, err := tracker.ReadDDLLog(ctx, branchConn)
		if err != nil {
			return fmt.Errorf("read DDL log: %w", err)
		}

		if len(entries) == 0 {
			fmt.Println("\nNo schema changes on this branch.")
		} else {
			fmt.Printf("\nSchema changes (%d):\n", len(entries))
			for _, e := range entries {
				fmt.Printf("  %s %s %s\n", e.CommandTag, e.ObjectType, e.ObjectIdentity)
			}
		}

		return nil
	},
}
