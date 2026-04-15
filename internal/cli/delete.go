package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a branch and its database",
	Aliases: []string{"rm"},
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		name := args[0]
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		bs, exists := state.GetBranch(name)
		if !exists {
			return fmt.Errorf("branch %q not found", name)
		}

		adminConn, err := connectAdmin(ctx, u)
		if err != nil {
			return fmt.Errorf("connect admin: %w", err)
		}
		defer adminConn.Close()

		fmt.Printf("Deleting branch %q (database: %s)...\n", name, bs.DBName)

		if err := branch.Delete(ctx, adminConn, name, bs.ParentDB, bs.DBName); err != nil {
			return err
		}

		state.RemoveBranch(name)
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		fmt.Printf("Branch %q deleted\n", name)
		return nil
	},
}
