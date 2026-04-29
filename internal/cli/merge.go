package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/pg-branch/pg-branch/internal/merge"
	"github.com/spf13/cobra"
)

var (
	mergeInto            string
	mergeApply           bool
	mergeResolve         string
	mergeNoLock          bool
	mergeNoData          bool
	mergeMetaTables      string
	mergeMetaTablesExtra string
)

// parseCommaList splits a comma-separated string into trimmed, non-empty
// entries. Returns nil for the empty string (so cobra "not passed" passes
// through as Options.MetaTables=nil → use defaults).
func parseCommaList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

var mergeCmd = &cobra.Command{
	Use:   "merge <branch>",
	Short: "Merge a branch into main (or specified target)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		branchName := args[0]
		u := mustResolveURL()

		state, err := loadActiveState()
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
			BranchName:      branchName,
			BranchDB:        bs.DBName,
			MainDB:          targetDB,
			DryRun:          !mergeApply,
			Resolve:         resolveMode,
			Progress:        stderrProgress("Checksumming"),
			NoLock:          mergeNoLock,
			NoData:          mergeNoData,
			MetaTables:      parseCommaList(mergeMetaTables),
			MetaTablesExtra: parseCommaList(mergeMetaTablesExtra),
			Stderr:          os.Stderr,
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
	mergeCmd.Flags().BoolVar(&mergeNoData, "no-data", false, "Skip the row-level data merge for ordinary tables. Migration-bookkeeping tables (see --meta-tables) are still merged.")
	mergeCmd.Flags().StringVar(&mergeMetaTables, "meta-tables", "", "Comma-separated bookkeeping tables that always data-merge regardless of --no-data. Replaces the built-in defaults (sequelize_meta, schema_migrations, goose_db_version, flyway_schema_history, knex_migrations). Names without a dot match in any schema; use schema.table to pin.")
	mergeCmd.Flags().StringVar(&mergeMetaTablesExtra, "meta-tables-extra", "", "Comma-separated additional bookkeeping tables to data-merge, on top of the defaults or --meta-tables.")
	rootCmd.AddCommand(mergeCmd)
}
