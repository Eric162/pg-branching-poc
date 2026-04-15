package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all branches",
	Aliases: []string{"ls"},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		if state.MainDB == "" {
			return fmt.Errorf("not initialized. Run 'pg-branch init' first")
		}

		// Connect to main DB to read branch metadata
		conn, err := pg.Connect(ctx, u)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		mainConn, err := conn.ConnectToDatabase(ctx, state.MainDB)
		if err != nil {
			return fmt.Errorf("connect to main db: %w", err)
		}
		defer mainConn.Close()

		branches, err := branch.List(ctx, mainConn, state)
		if err != nil {
			return err
		}

		if len(branches) == 0 {
			fmt.Println("No branches. Create one with: pg-branch create <name>")
			return nil
		}

		// Print main DB
		marker := " "
		if state.CurrentBranch == "" {
			marker = "*"
		}
		fmt.Printf("  %s main (%s)\n", marker, state.MainDB)

		// Print branches
		for _, b := range branches {
			marker = " "
			if b.IsCurrent {
				marker = "*"
			}
			fmt.Printf("  %s %s (%s) [from %s, %s]\n",
				marker, b.Name, b.DBName, b.ParentDB,
				b.CreatedAt.Format("2006-01-02 15:04"))
		}
		return nil
	},
}
