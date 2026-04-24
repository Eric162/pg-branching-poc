package cli

import (
	"fmt"
	"os"

	"github.com/pg-branch/pg-branch/internal/merge"
	"github.com/spf13/cobra"
)

var (
	mergeInto    string
	mergeApply   bool
	mergeResolve string
	mergeNoLock  bool
	mergeNoData  bool
)

var mergeCmd = &cobra.Command{
	Use:   "merge <branch>",
	Short: "Merge a branch into main (or specified target)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		branchName := args[0]
		u := mustResolveURL()

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		bs, exists := state.GetBranch(branchName)
		if !exists {
			return fmt.Errorf("branch %q not found", branchName)
		}

		targetDB := mergeInto
		if targetDB == "" {
			targetDB = bs.ParentDB
		}
		if targetDB == "" {
			targetDB = state.MainDB
		}

		var resolveMode merge.ResolveMode
		switch mergeResolve {
		case "branch":
			resolveMode = merge.ResolveBranch
		case "main":
			resolveMode = merge.ResolveMain
		case "":
			resolveMode = merge.ResolveNone
		default:
			return fmt.Errorf("invalid --resolve value: %q (use 'branch' or 'main')", mergeResolve)
		}

		adminConn, err := connectAdmin(ctx, u)
		if err != nil {
			return fmt.Errorf("connect admin: %w", err)
		}
		defer adminConn.Close()

		opts := merge.Options{
			BranchName: branchName,
			BranchDB:   bs.DBName,
			MainDB:     targetDB,
			DryRun:     !mergeApply,
			Resolve:    resolveMode,
			Progress:   stderrProgress("Checksumming"),
			NoLock:     mergeNoLock,
			NoData:     mergeNoData,
			Stderr:     os.Stderr,
		}

		result, err := merge.Execute(ctx, adminConn, opts)
		if err != nil {
			if result != nil {
				fmt.Println(result.Summary())
			}
			return err
		}

		fmt.Println(result.Summary())
		return nil
	},
}

func init() {
	mergeCmd.Flags().StringVar(&mergeInto, "into", "", "Target database to merge into (default: parent of branch)")
	mergeCmd.Flags().BoolVar(&mergeApply, "apply", false, "Apply the merge (default: dry-run)")
	mergeCmd.Flags().StringVar(&mergeResolve, "resolve", "", "Conflict resolution: 'branch' or 'main'")
	mergeCmd.Flags().BoolVar(&mergeNoLock, "no-lock", false, "Skip the advisory lock that serialises concurrent merges")
	mergeCmd.Flags().BoolVar(&mergeNoData, "no-data", false, "Skip the row-level data merge (schema changes only)")
	rootCmd.AddCommand(mergeCmd)
}
