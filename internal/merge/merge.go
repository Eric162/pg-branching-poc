package merge

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/pg-branch/pg-branch/internal/diff"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

// ResolveMode controls how conflicts are handled.
type ResolveMode string

const (
	ResolveNone   ResolveMode = ""      // fail on conflict
	ResolveBranch ResolveMode = "branch" // branch wins
	ResolveMain   ResolveMode = "main"   // main wins
)

// Options configures a merge operation.
type Options struct {
	BranchName  string
	BranchDB    string
	MainDB      string
	DryRun      bool
	Resolve     ResolveMode
	Progress    diff.ProgressFunc
}

// Execute performs a three-way merge from branch into main.
//
// Algorithm:
//  1. Load branch-point snapshot (schema state when branch was created)
//  2. Get current schema of both main and branch
//  3. Compute schema changes on each side vs branch point
//  4. Detect conflicts (same object modified both sides)
//  5. Build merge operations (DDL to replay, DML to apply)
//  6. If not dry-run, apply within a transaction
func Execute(ctx context.Context, adminConn *pg.Conn, opts Options) (*MergeResult, error) {
	result := &MergeResult{DryRun: opts.DryRun}

	// Connect to both databases
	mainConn, err := adminConn.ConnectToDatabase(ctx, opts.MainDB)
	if err != nil {
		return nil, fmt.Errorf("connect to main: %w", err)
	}
	defer mainConn.Close()

	branchConn, err := adminConn.ConnectToDatabase(ctx, opts.BranchDB)
	if err != nil {
		return nil, fmt.Errorf("connect to branch: %w", err)
	}
	defer branchConn.Close()

	// 1. Load branch-point snapshot
	fmt.Fprintf(os.Stderr, "  Loading branch-point snapshot...\n")
	snapshotData, err := tracker.LoadSnapshot(ctx, mainConn, opts.BranchName)
	if err != nil {
		return nil, fmt.Errorf("load branch-point snapshot: %w", err)
	}
	branchPointSchema, err := pg.SchemaSnapshotFromJSON(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("parse branch-point snapshot: %w", err)
	}

	// 2. Get current schemas
	fmt.Fprintf(os.Stderr, "  Snapshotting main schema...\n")
	mainSchema, err := mainConn.TakeSchemaSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot main schema: %w", err)
	}
	fmt.Fprintf(os.Stderr, "  Snapshotting branch schema...\n")
	branchSchema, err := branchConn.TakeSchemaSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot branch schema: %w", err)
	}

	// 3. Compute changes on each side
	fmt.Fprintf(os.Stderr, "  Computing schema diff...\n")
	mainChanges := diff.SchemaDiff(branchPointSchema, mainSchema)
	branchChanges := diff.SchemaDiff(branchPointSchema, branchSchema)

	// 4. Read DDL log from branch for replay
	ddlLog, err := tracker.ReadDDLLog(ctx, branchConn)
	if err != nil {
		return nil, fmt.Errorf("read DDL log: %w", err)
	}

	// 5. Build schema merge ops
	buildSchemaMergeOps(result, mainChanges, branchChanges, ddlLog, opts.Resolve, branchConn)

	// 6. Data merge — compare checksums
	fmt.Fprintf(os.Stderr, "  Checksumming main...\n")
	mainChecksums, err := diff.ComputeTableChecksums(ctx, mainConn, opts.Progress)
	if err != nil {
		return nil, fmt.Errorf("main checksums: %w", err)
	}
	fmt.Fprintf(os.Stderr, "  Checksumming branch...\n")
	branchChecksums, err := diff.ComputeTableChecksums(ctx, branchConn, opts.Progress)
	if err != nil {
		return nil, fmt.Errorf("branch checksums: %w", err)
	}

	// Get branch-point checksums from main (since main was the template, at branch time
	// main and branch had identical data — so we compare current states)
	buildDataMergeOps(ctx, result, mainConn, branchConn, mainChecksums, branchChecksums, branchPointSchema, opts.Resolve)

	// 7. Apply if not dry-run and no unresolved conflicts
	if !opts.DryRun {
		if result.HasConflicts() && opts.Resolve == ResolveNone {
			return result, fmt.Errorf("merge has %d conflicts. Use --resolve=branch or --resolve=main", len(result.Conflicts))
		}
		if err := applyMerge(ctx, mainConn, result); err != nil {
			return result, fmt.Errorf("apply merge: %w", err)
		}
		result.Applied = true
	}

	return result, nil
}

// buildSchemaMergeOps compares branch and main schema changes to detect conflicts and build ops.
func buildSchemaMergeOps(result *MergeResult, mainChanges, branchChanges []diff.SchemaChange, ddlLog []tracker.DDLEntry, resolve ResolveMode, branchConn *pg.Conn) {
	// Index main changes by object name for conflict detection
	mainChangeMap := make(map[string]diff.SchemaChange)
	for _, mc := range mainChanges {
		mainChangeMap[mc.ObjectName] = mc
	}

	// Track pg_dump SQL for added tables — indexes/constraints are included in the dump
	pgDumpSQL := make(map[string]string) // table name -> full pg_dump DDL

	// Sort: process tables before indexes/constraints so pg_dump results are available
	sortChanges(branchChanges)

	// For each branch change, check if main also changed the same object
	branchObjectsHandled := make(map[string]bool)
	for _, bc := range branchChanges {
		branchObjectsHandled[bc.ObjectName] = true
		mc, mainAlsoChanged := mainChangeMap[bc.ObjectName]

		if !mainAlsoChanged {
			// Skip indexes/constraints already included in a pg_dump for an added table
			if (bc.ObjectKind == "index" || bc.ObjectKind == "constraint") && bc.Type == diff.Added {
				if coveredByPgDump(bc.ObjectName, pgDumpSQL) {
					result.SchemaOps = append(result.SchemaOps, SchemaOp{
						Description: bc.Detail + " (included in table DDL)",
						Status:      "skipped",
					})
					continue
				}
			}

			// Only branch changed this object — safe to apply
			sql := findSQL(bc, ddlLog, branchConn)
			if bc.ObjectKind == "table" && bc.Type == diff.Added && sql != "" {
				pgDumpSQL[bc.ObjectName] = sql
			}
			result.SchemaOps = append(result.SchemaOps, SchemaOp{
				Description: bc.Detail,
				SQL:         sql,
				Status:      "ok",
			})
			continue
		}

		// Both sides changed — conflict
		if bc.Detail == mc.Detail {
			// Same change on both sides — skip (already converged)
			result.SchemaOps = append(result.SchemaOps, SchemaOp{
				Description: bc.Detail + " (same on both sides)",
				Status:      "skipped",
			})
			continue
		}

		// Real conflict
		if resolve == ResolveBranch {
			sql := findSQL(bc, ddlLog, branchConn)
			result.SchemaOps = append(result.SchemaOps, SchemaOp{
				Description: bc.Detail + " (branch wins)",
				SQL:         sql,
				Status:      "ok",
			})
		} else if resolve == ResolveMain {
			result.SchemaOps = append(result.SchemaOps, SchemaOp{
				Description: mc.Detail + " (main wins, skipping branch change)",
				Status:      "skipped",
			})
		} else {
			result.SchemaOps = append(result.SchemaOps, SchemaOp{
				Description: bc.Detail,
				Status:      "conflict",
			})
			result.Conflicts = append(result.Conflicts, Conflict{
				Type:       SchemaConflict,
				ObjectName: bc.ObjectName,
				BranchSide: bc.Detail,
				MainSide:   mc.Detail,
			})
		}
	}
}

// findSQL determines the SQL to replay for a branch schema change.
// For added/removed tables, uses pg_dump to get exact DDL.
// For other changes, falls back to DDL log lookup.
func findSQL(change diff.SchemaChange, ddlLog []tracker.DDLEntry, branchConn *pg.Conn) string {
	if change.ObjectKind == "table" && change.Type == diff.Added {
		// Use pg_dump for added tables — DDL log may not have the CREATE TABLE
		sql, err := pgDumpTable(branchConn.URL(), change.ObjectName)
		if err == nil && sql != "" {
			return sql
		}
	}
	return findDDLForObject(ddlLog, change.ObjectName)
}

// pgDumpTable runs pg_dump --schema-only for a single table and returns the DDL.
func pgDumpTable(dbURL, tableName string) (string, error) {
	// tableName is "schema.table" — pg_dump wants it as-is
	cmd := exec.Command("pg_dump", dbURL, "--schema-only", "--no-owner", "--no-acl", "-t", tableName)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("pg_dump: %w", err)
	}
	// Filter out comments, SET statements, psql meta-commands, and empty lines — keep only DDL
	var ddl strings.Builder
	for _, line := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "SET ") || strings.HasPrefix(trimmed, "SELECT ") || strings.HasPrefix(trimmed, "RESET ") || strings.HasPrefix(trimmed, "\\") {
			continue
		}
		ddl.WriteString(line)
		ddl.WriteString("\n")
	}
	return strings.TrimSpace(ddl.String()), nil
}

// findDDLForObject finds the most relevant DDL command for a schema object from the log.
func findDDLForObject(ddlLog []tracker.DDLEntry, objectName string) string {
	// Search backwards for the most recent DDL affecting this object
	for i := len(ddlLog) - 1; i >= 0; i-- {
		entry := ddlLog[i]
		if entry.ObjectIdentity == objectName || containsObjectRef(entry.Command, objectName) {
			return entry.Command
		}
	}
	return ""
}

func containsObjectRef(command, objectName string) bool {
	// Simple substring match — good enough for most cases.
	// Object names like "public.users" appear in DDL commands.
	parts := splitObjectName(objectName)
	for _, part := range parts {
		if len(part) > 0 && contains(command, part) {
			return true
		}
	}
	return false
}

// coveredByPgDump checks if an index/constraint object name appears in any pg_dump DDL
// we already captured for an added table. The pg_dump output includes CREATE INDEX
// and ALTER TABLE ... ADD CONSTRAINT statements that reference the object by name.
func coveredByPgDump(objectName string, pgDumpSQL map[string]string) bool {
	// Extract the short name (last part after the last dot)
	// e.g. "public.rebate_external_figures_pkey" -> "rebate_external_figures_pkey"
	parts := strings.Split(objectName, ".")
	shortName := parts[len(parts)-1]
	for _, sql := range pgDumpSQL {
		if strings.Contains(sql, shortName) {
			return true
		}
	}
	return false
}

// sortChanges orders schema changes so tables come before indexes and constraints.
func sortChanges(changes []diff.SchemaChange) {
	kindOrder := map[string]int{"table": 0, "column": 1, "index": 2, "constraint": 2}
	sort.SliceStable(changes, func(i, j int) bool {
		return kindOrder[changes[i].ObjectKind] < kindOrder[changes[j].ObjectKind]
	})
}

func splitObjectName(name string) []string {
	// "public.users.email" -> ["public", "users", "email"]
	result := []string{name}
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			result = append(result, name[i+1:])
		}
	}
	return result
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// buildDataMergeOps compares data between branch and main.
// Since branch was created from main via TEMPLATE, the branch-point data = main at that time.
// We detect tables where branch data differs from main data.
func buildDataMergeOps(ctx context.Context, result *MergeResult, mainConn, branchConn *pg.Conn,
	mainChecksums, branchChecksums []diff.TableChecksum,
	branchPointSchema *pg.SchemaSnapshot, resolve ResolveMode) {

	mainMap := make(map[string]diff.TableChecksum)
	for _, mc := range mainChecksums {
		mainMap[mc.Schema+"."+mc.Table] = mc
	}

	for _, bc := range branchChecksums {
		key := bc.Schema + "." + bc.Table
		mc, mainExists := mainMap[key]

		if !mainExists {
			// New table only on branch — data comes with schema op
			continue
		}

		// If branch and main have same checksum, no data change needed
		if bc.Checksum == mc.Checksum {
			continue
		}

		// Data differs — for now we flag this as a data change.
		// Full row-level merge would require PK-based comparison.
		// We record it as a data operation that needs attention.
		result.DataOps = append(result.DataOps, DataOp{
			Table:     key,
			Operation: "SYNC",
			RowKey:    fmt.Sprintf("%d rows (branch) vs %d rows (main)", bc.RowCount, mc.RowCount),
			Status:    "ok",
		})
	}
}

// applyMerge executes the merge operations within a transaction.
func applyMerge(ctx context.Context, mainConn *pg.Conn, result *MergeResult) error {
	tx, err := mainConn.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, op := range result.SchemaOps {
		if op.Status != "ok" || op.SQL == "" {
			continue
		}
		if _, err := tx.Exec(ctx, op.SQL); err != nil {
			return fmt.Errorf("apply schema op %q: %w", op.Description, err)
		}
	}

	return tx.Commit(ctx)
}
