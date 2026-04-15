package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
	"github.com/spf13/cobra"
)

var logVerbose bool

var logCmd = &cobra.Command{
	Use:   "log [branch]",
	Short: "Show DDL change history for a branch",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		branchName := state.CurrentBranch
		if len(args) > 0 {
			branchName = args[0]
		}
		if branchName == "" {
			return fmt.Errorf("no branch selected. Use 'pg-branch switch <name>' or specify a branch")
		}

		bs, exists := state.GetBranch(branchName)
		if !exists {
			return fmt.Errorf("branch %q not found", branchName)
		}

		conn, err := pg.Connect(ctx, u)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		branchConn, err := conn.ConnectToDatabase(ctx, bs.DBName)
		if err != nil {
			return fmt.Errorf("connect to branch %q: %w", bs.DBName, err)
		}
		defer branchConn.Close()

		entries, err := tracker.ReadDDLLog(ctx, branchConn)
		if err != nil {
			return fmt.Errorf("read DDL log: %w", err)
		}

		if len(entries) == 0 {
			fmt.Printf("No DDL changes on branch %q.\n", branchName)
			return nil
		}

		fmt.Printf("DDL log for branch %q (%d entries):\n\n", branchName, len(entries))
		for _, e := range entries {
			ts := e.EventTime.Format("2006-01-02 15:04:05")
			fmt.Printf("  %s  %s %s\n", ts, e.CommandTag, e.ObjectIdentity)
			if logVerbose && e.Command != "" {
				fmt.Printf("           %s\n", e.Command)
			}
		}
		return nil
	},
}

func init() {
	logCmd.Flags().BoolVarP(&logVerbose, "verbose", "v", false, "Show full SQL commands")
	rootCmd.AddCommand(logCmd)
}
