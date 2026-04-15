package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/spf13/cobra"
)

var connectCmd = &cobra.Command{
	Use:   "connect [name]",
	Short: "Print connection string for a branch (or current branch)",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		var dbName string
		if len(args) > 0 {
			name := args[0]
			if name == "main" {
				dbName = state.MainDB
			} else {
				bs, exists := state.GetBranch(name)
				if !exists {
					return fmt.Errorf("branch %q not found", name)
				}
				dbName = bs.DBName
			}
		} else if state.CurrentBranch != "" {
			bs, _ := state.GetBranch(state.CurrentBranch)
			dbName = bs.DBName
		} else {
			dbName = state.MainDB
		}

		conn, err := pg.Connect(ctx, u)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		connURL, err := conn.URLForDatabase(dbName)
		if err != nil {
			return err
		}
		fmt.Println(connURL)
		return nil
	},
}
