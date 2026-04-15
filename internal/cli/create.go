package cli

import (
	"fmt"
	"time"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/spf13/cobra"
)

var createFrom string

var createCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new database branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		name := args[0]
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		parentDB := createFrom
		if parentDB == "" {
			parentDB = state.MainDB
		}
		if parentDB == "" {
			return fmt.Errorf("no parent database. Run 'pg-branch init' first or use --from")
		}

		// Branch DB name: prefix + branch name
		branchDBName := "pgbr_" + name

		// Check if branch already exists in state
		if _, exists := state.GetBranch(name); exists {
			return fmt.Errorf("branch %q already exists", name)
		}

		// Connect to admin DB for CREATE DATABASE
		adminConn, err := connectAdmin(ctx, u)
		if err != nil {
			return fmt.Errorf("connect admin: %w", err)
		}
		defer adminConn.Close()

		// Check if target DB already exists
		exists, err := adminConn.DatabaseExists(ctx, branchDBName)
		if err != nil {
			return fmt.Errorf("check database exists: %w", err)
		}
		if exists {
			return fmt.Errorf("database %q already exists. Delete it first or choose a different branch name", branchDBName)
		}

		// Verify parent DB exists
		parentExists, err := adminConn.DatabaseExists(ctx, parentDB)
		if err != nil {
			return fmt.Errorf("check parent exists: %w", err)
		}
		if !parentExists {
			return fmt.Errorf("parent database %q does not exist", parentDB)
		}

		fmt.Printf("Creating branch %q from %q...\n", name, parentDB)

		if err := branch.Create(ctx, adminConn, name, parentDB, branchDBName); err != nil {
			return err
		}

		// Update state
		state.AddBranch(name, config.BranchState{
			DBName:    branchDBName,
			ParentDB:  parentDB,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		})
		state.CurrentBranch = name
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		fmt.Printf("Branch %q created (database: %s)\n", name, branchDBName)
		fmt.Printf("Switched to branch %q\n", name)
		return nil
	},
}

func init() {
	createCmd.Flags().StringVar(&createFrom, "from", "", "Parent database to branch from (default: main DB from state)")
}
