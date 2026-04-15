package cli

import (
	"fmt"

	"github.com/pg-branch/pg-branch/internal/diff"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff [branch]",
	Short: "Show schema and data changes on a branch vs its parent",
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

		// Load branch-point snapshot from parent
		parentConn, err := conn.ConnectToDatabase(ctx, bs.ParentDB)
		if err != nil {
			return fmt.Errorf("connect to parent: %w", err)
		}
		defer parentConn.Close()

		snapshotData, err := tracker.LoadSnapshot(ctx, parentConn, branchName)
		if err != nil {
			return fmt.Errorf("load snapshot: %w", err)
		}
		branchPointSchema, err := pg.SchemaSnapshotFromJSON(snapshotData)
		if err != nil {
			return fmt.Errorf("parse snapshot: %w", err)
		}

		// Get current branch schema
		branchConn, err := conn.ConnectToDatabase(ctx, bs.DBName)
		if err != nil {
			return fmt.Errorf("connect to branch: %w", err)
		}
		defer branchConn.Close()

		currentSchema, err := branchConn.TakeSchemaSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("snapshot branch: %w", err)
		}

		// Schema diff
		changes := diff.SchemaDiff(branchPointSchema, currentSchema)
		fmt.Println(diff.FormatChanges(changes))

		// Data diff via checksums
		parentChecksums, err := diff.ComputeTableChecksums(ctx, parentConn, stderrProgress("Checksumming parent"))
		if err != nil {
			return fmt.Errorf("parent checksums: %w", err)
		}
		branchChecksums, err := diff.ComputeTableChecksums(ctx, branchConn, stderrProgress("Checksumming branch"))
		if err != nil {
			return fmt.Errorf("branch checksums: %w", err)
		}
		dataChanges := diff.CompareData(parentChecksums, branchChecksums)
		fmt.Println(diff.FormatDataChanges(dataChanges))

		return nil
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)
}
