package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

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

// DefaultMetaTables names migration-bookkeeping tables that pg-branch always
// data-merges, even when NoData is true. The risk of NOT propagating these
// is silent schema/meta drift: schema DDL lands on the target while the rows
// tracking which migrations have been applied are left behind, causing the
// migration runner to re-apply migrations whose schema effects are already
// present and fail on duplicate objects.
//
// The default set covers single-table-per-tool conventions:
//   - sequelize_meta         (Sequelize)
//   - schema_migrations      (Rails / ActiveRecord, golang-migrate)
//   - goose_db_version       (goose)
//   - flyway_schema_history  (Flyway)
//   - knex_migrations        (Knex)
//
// Names without a dot match by table name in any schema. Names with a dot
// are matched as schema.table. Set Options.MetaTables to replace the list
// or Options.MetaTablesExtra to add on top.
var DefaultMetaTables = []string{
	"sequelize_meta",
	"schema_migrations",
	"goose_db_version",
	"flyway_schema_history",
	"knex_migrations",
}

// Options configures a merge operation.
type Options struct {
	BranchName string
	BranchDB   string
	MainDB     string
	DryRun     bool
	Resolve    ResolveMode
	Progress   diff.ProgressFunc
	// NoLock skips the Postgres advisory lock that normally serialises merges
	// of the same branch/main pair. Use with care — concurrent merges can
	// interleave DDL and corrupt the merged state.
	NoLock bool
	// NoData skips the row-level data merge for ordinary tables. Migration-
	// bookkeeping tables in the resolved meta-table set (see MetaTables /
	// MetaTablesExtra / DefaultMetaTables) are still data-merged, since
	// dropping their rows while keeping schema DDL is the bug that prompted
	// this flag's redesign. Useful when you want to review most data
	// changes manually but still keep migration state in sync.
	NoData bool
	// MetaTables, when non-nil, replaces DefaultMetaTables as the set of
	// migration-bookkeeping tables that always data-merge regardless of
	// NoData. Pass an empty (but non-nil) slice to disable meta-table
	// protection entirely.
	MetaTables []string
	// MetaTablesExtra is appended to the resolved meta-table set
	// (DefaultMetaTables, or MetaTables if that's set). Use when you want
	// to keep the defaults and add a couple of project-specific
	// bookkeeping tables.
	MetaTablesExtra []string
	// Stderr receives phase-label progress output ("Loading branch-point
	// snapshot...", etc.). Defaults to os.Stderr when nil; set to
	// io.Discard to silence or to a bytes.Buffer in tests.
	Stderr io.Writer
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

	stderr := opts.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	// Connect to both databases
	mainConn, err := adminConn.ConnectToDatabase(ctx, opts.MainDB)
	if err != nil {
		return nil, fmt.Errorf("connect to main: %w", err)
	}
	defer mainConn.Close()

	// Serialise concurrent merges of the same branch/main pair. The lock is
	// session-scoped and held on a dedicated pooled connection for the whole
	// operation; --no-lock opts out for users who know they're the only
	// merger (e.g. scripted demos) and don't want the extra round-trips.
	if !opts.NoLock {
		lockKey := fmt.Sprintf("pgbranch:merge:%s:%s", opts.MainDB, opts.BranchName)
		lock, err := mainConn.TryAdvisoryLock(ctx, lockKey)
		if err != nil {
			return nil, fmt.Errorf("acquire merge lock: %w", err)
		}
		if lock == nil {
			return nil, fmt.Errorf("another merge of %q into %q is in progress (advisory lock held). Wait for it to finish or pass --no-lock to override",
				opts.BranchName, opts.MainDB)
		}
		defer lock.Release(ctx)
	}

	branchConn, err := adminConn.ConnectToDatabase(ctx, opts.BranchDB)
	if err != nil {
		return nil, fmt.Errorf("connect to branch: %w", err)
	}
	defer branchConn.Close()

	// 1. Load branch-point snapshot
	fmt.Fprintf(stderr, "  Loading branch-point snapshot...\n")
	snapshotData, err := tracker.LoadSnapshot(ctx, mainConn, opts.BranchName)
	if err != nil {
		return nil, fmt.Errorf("load branch-point snapshot: %w", err)
	}
	branchPointSchema, err := pg.SchemaSnapshotFromJSON(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("parse branch-point snapshot: %w", err)
	}

	// 2. Get current schemas
	fmt.Fprintf(stderr, "  Snapshotting main schema...\n")
	mainSchema, err := mainConn.TakeSchemaSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot main schema: %w", err)
	}
	fmt.Fprintf(stderr, "  Snapshotting branch schema...\n")
	branchSchema, err := branchConn.TakeSchemaSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot branch schema: %w", err)
	}

	// 3. Compute changes on each side
	fmt.Fprintf(stderr, "  Computing schema diff...\n")
	mainChanges := diff.SchemaDiff(branchPointSchema, mainSchema)
	branchChanges := diff.SchemaDiff(branchPointSchema, branchSchema)

	// 4. Read DDL log from branch for replay
	ddlLog, err := tracker.ReadDDLLog(ctx, branchConn)
	if err != nil {
		return nil, fmt.Errorf("read DDL log: %w", err)
	}

	// 5. Build schema merge ops
	buildSchemaMergeOps(result, mainChanges, branchChanges, ddlLog, opts.Resolve, branchConn)

	// 6. Data merge — compare checksums. NoData=true still processes
	// meta-table rows so migration bookkeeping stays in sync with schema.
	metaSet := resolvedMetaTables(opts)
	if !opts.NoData || len(metaSet) > 0 {
		fmt.Fprintf(stderr, "  Checksumming main...\n")
		mainChecksums, err := diff.ComputeTableChecksums(ctx, mainConn, opts.Progress)
		if err != nil {
			return nil, fmt.Errorf("main checksums: %w", err)
		}
		fmt.Fprintf(stderr, "  Checksumming branch...\n")
		branchChecksums, err := diff.ComputeTableChecksums(ctx, branchConn, opts.Progress)
		if err != nil {
			return nil, fmt.Errorf("branch checksums: %w", err)
		}
		if err := buildDataMergeOps(ctx, result, mainConn, branchConn, mainChecksums, branchChecksums, metaSet, opts.NoData); err != nil {
			return nil, fmt.Errorf("build data merge ops: %w", err)
		}
	}

	// 7. Apply if not dry-run and no unresolved conflicts
	if !opts.DryRun {
		if result.HasConflicts() && opts.Resolve == ResolveNone {
			return result, fmt.Errorf("merge has %d conflicts. Use --resolve=branch or --resolve=main", len(result.Conflicts))
		}
		if err := applyMerge(ctx, mainConn, branchConn, result); err != nil {
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
	for _, bc := range branchChanges {
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
	for _, part := range splitObjectName(objectName) {
		if part != "" && strings.Contains(command, part) {
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

// splitObjectName returns the full name plus every dot-delimited suffix.
// "public.users.email" → ["public.users.email", "email", "users.email"].
func splitObjectName(name string) []string {
	result := []string{name}
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			result = append(result, name[i+1:])
		}
	}
	return result
}

// buildDataMergeOps compares data between branch and main. Since branch was
// created from main via TEMPLATE, branch-point data equals main at that time;
// a current checksum divergence means the branch diverged from main (or main
// diverged from the branch point, or both).
//
// For each divergent table we try to build an executable plan:
//   - If the table has a primary key, emit an INSERT_PK op. applyMerge will
//     stream rows from branch and insert any with PKs not already in main.
//   - If the table has no primary key, emit a SYNC op (reported as
//     [NOT APPLIED] in the summary) so the user knows to sync manually.
//
// Updates and deletes aren't part of this merge strategy — detecting them
// accurately requires a branch-point data snapshot which this tool doesn't
// keep. Surfacing "a table differs" without an automated fix is the safe
// behaviour for those cases.
func buildDataMergeOps(ctx context.Context, result *MergeResult,
	mainConn, branchConn *pg.Conn,
	mainChecksums, branchChecksums []diff.TableChecksum,
	metaSet map[string]struct{}, onlyMeta bool) error {

	mainMap := make(map[string]diff.TableChecksum)
	for _, mc := range mainChecksums {
		mainMap[mc.Schema+"."+mc.Table] = mc
	}

	for _, bc := range branchChecksums {
		// In --no-data mode we still process meta tables so migration
		// bookkeeping stays in sync with schema DDL.
		if onlyMeta && !isMetaTable(bc.Schema, bc.Table, metaSet) {
			continue
		}

		key := bc.Schema + "." + bc.Table
		mc, mainExists := mainMap[key]

		if !mainExists {
			// New table only on branch. The schema phase will CREATE TABLE
			// it on main (via pg_dump --schema-only, which carries no rows),
			// so we need to follow up with an INSERT_PK plan or the table
			// arrives empty.
			if err := emitNewTableDataOp(ctx, result, branchConn, bc); err != nil {
				return err
			}
			continue
		}
		if bc.Checksum == mc.Checksum {
			continue
		}

		rowKey := fmt.Sprintf("%d rows (branch) vs %d rows (main)", bc.RowCount, mc.RowCount)

		pkCols, err := mainConn.PrimaryKeyColumns(ctx, bc.Schema, bc.Table)
		if err != nil {
			return fmt.Errorf("primary key for %s: %w", key, err)
		}
		if len(pkCols) == 0 {
			// No PK on main-side schema (it's the same shape for both here)
			// — fall back to manual review.
			result.DataOps = append(result.DataOps, DataOp{
				Table:     key,
				Operation: "SYNC",
				RowKey:    rowKey + " (no primary key)",
				Status:    "ok",
			})
			continue
		}

		cols, err := branchConn.TableColumnNames(ctx, bc.Schema, bc.Table)
		if err != nil {
			return fmt.Errorf("columns for %s: %w", key, err)
		}
		result.DataOps = append(result.DataOps, DataOp{
			Table:      key,
			Operation:  "INSERT_PK",
			RowKey:     rowKey,
			Status:     "ok",
			Schema:     bc.Schema,
			TableName:  bc.Table,
			PKColumns:  pkCols,
			AllColumns: cols,
		})
	}
	return nil
}

// emitNewTableDataOp builds an INSERT_PK plan for a table that exists only
// on the branch. The schema phase CREATEs it on main; this op streams its
// rows in afterwards. Without it, new-on-branch tables ship empty.
//
// PK and column metadata come from branchConn since main hasn't seen the
// table yet at the point we build ops. applyMerge runs schema ops before
// data ops, so by the time applyPKInsert executes, the table exists on
// main and fetchPKSet returns an empty set — every branch row gets
// inserted.
func emitNewTableDataOp(ctx context.Context, result *MergeResult, branchConn *pg.Conn, bc diff.TableChecksum) error {
	if bc.RowCount == 0 {
		return nil
	}
	key := bc.Schema + "." + bc.Table
	rowKey := fmt.Sprintf("%d row(s) (new table)", bc.RowCount)

	pkCols, err := branchConn.PrimaryKeyColumns(ctx, bc.Schema, bc.Table)
	if err != nil {
		return fmt.Errorf("primary key for %s: %w", key, err)
	}
	if len(pkCols) == 0 {
		result.DataOps = append(result.DataOps, DataOp{
			Table:     key,
			Operation: "SYNC",
			RowKey:    rowKey + " (no primary key)",
			Status:    "ok",
		})
		return nil
	}
	cols, err := branchConn.TableColumnNames(ctx, bc.Schema, bc.Table)
	if err != nil {
		return fmt.Errorf("columns for %s: %w", key, err)
	}
	result.DataOps = append(result.DataOps, DataOp{
		Table:      key,
		Operation:  "INSERT_PK",
		RowKey:     rowKey,
		Status:     "ok",
		Schema:     bc.Schema,
		TableName:  bc.Table,
		PKColumns:  pkCols,
		AllColumns: cols,
	})
	return nil
}

// resolvedMetaTables computes the effective meta-table set keyed for fast
// membership tests by isMetaTable.
func resolvedMetaTables(opts Options) map[string]struct{} {
	base := DefaultMetaTables
	if opts.MetaTables != nil {
		base = opts.MetaTables
	}
	set := make(map[string]struct{}, len(base)+len(opts.MetaTablesExtra))
	for _, n := range base {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	for _, n := range opts.MetaTablesExtra {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	return set
}

// isMetaTable reports whether schema.table matches an entry in set. Names
// without a dot in set match by table name in any schema; dotted names
// match as schema.table. This lets users say "sequelize_meta" without
// pinning it to public.
func isMetaTable(schema, table string, set map[string]struct{}) bool {
	if _, ok := set[table]; ok {
		return true
	}
	if _, ok := set[schema+"."+table]; ok {
		return true
	}
	return false
}

// applyMerge executes the merge operations within a single transaction.
// Schema ops run first (replayed DDL), then data ops (INSERT_PK plans that
// stream rows from branch into main). Any failure rolls back the whole lot.
func applyMerge(ctx context.Context, mainConn, branchConn *pg.Conn, result *MergeResult) error {
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

	for i := range result.DataOps {
		op := &result.DataOps[i]
		if op.Status != "ok" || op.Operation != "INSERT_PK" {
			continue
		}
		inserted, err := applyPKInsert(ctx, tx, branchConn, op)
		if err != nil {
			return fmt.Errorf("apply data op on %s: %w", op.Table, err)
		}
		op.SQL = fmt.Sprintf("inserted %d row(s)", inserted)
	}

	return tx.Commit(ctx)
}

// applyPKInsert streams rows from the branch-side copy of the table and
// INSERTs into main any row whose primary key isn't already present there.
// Runs inside the main-side transaction so a failure rolls back everything
// applyMerge did before this call.
func applyPKInsert(ctx context.Context, mainTx pgx.Tx, branchConn *pg.Conn, op *DataOp) (int, error) {
	qTable := pgx.Identifier{op.Schema, op.TableName}.Sanitize()

	// 1. Fetch primary keys already present on main. Using a Go-side set is
	//    simple and memory-cheap for dev-sized tables — the tool's scope.
	mainPKs, err := fetchPKSet(ctx, mainTx, qTable, op.PKColumns)
	if err != nil {
		return 0, err
	}

	// 2. Stream every row from branch and skip those whose PK is already in
	//    main. The remaining rows get parameterised INSERTs inside the tx.
	quotedCols := make([]string, len(op.AllColumns))
	for i, c := range op.AllColumns {
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}
	selectSQL := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedCols, ","), qTable)

	rows, err := branchConn.Query(ctx, selectSQL)
	if err != nil {
		return 0, fmt.Errorf("select from branch: %w", err)
	}
	defer rows.Close()

	pkIdx := make([]int, len(op.PKColumns))
	for i, pk := range op.PKColumns {
		pkIdx[i] = -1
		for j, c := range op.AllColumns {
			if c == pk {
				pkIdx[i] = j
				break
			}
		}
		if pkIdx[i] == -1 {
			return 0, fmt.Errorf("primary key column %q not found in column list", pk)
		}
	}

	placeholders := make([]string, len(op.AllColumns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		qTable, strings.Join(quotedCols, ","), strings.Join(placeholders, ","))

	inserted := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return inserted, fmt.Errorf("scan branch row: %w", err)
		}
		pkKey, err := pkTupleKey(values, pkIdx)
		if err != nil {
			return inserted, err
		}
		if _, exists := mainPKs[pkKey]; exists {
			continue
		}
		if _, err := mainTx.Exec(ctx, insertSQL, values...); err != nil {
			return inserted, fmt.Errorf("insert into main: %w", err)
		}
		inserted++
	}
	return inserted, rows.Err()
}

// fetchPKSet returns a set of PK-tuple strings for rows in qTable.
func fetchPKSet(ctx context.Context, tx pgx.Tx, qTable string, pkCols []string) (map[string]struct{}, error) {
	quoted := make([]string, len(pkCols))
	for i, c := range pkCols {
		quoted[i] = pgx.Identifier{c}.Sanitize()
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quoted, ","), qTable)

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("select pk from main: %w", err)
	}
	defer rows.Close()

	set := make(map[string]struct{})
	indices := make([]int, len(pkCols))
	for i := range indices {
		indices[i] = i
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan pk row: %w", err)
		}
		key, err := pkTupleKey(values, indices)
		if err != nil {
			return nil, err
		}
		set[key] = struct{}{}
	}
	return set, rows.Err()
}

// pkTupleKey builds a deterministic string key for a PK tuple. JSON encoding
// handles mixed types (int, string, uuid, timestamp) without a handwritten
// separator-based scheme that could collide on certain values.
func pkTupleKey(values []any, indices []int) (string, error) {
	picked := make([]any, len(indices))
	for i, idx := range indices {
		picked[i] = values[idx]
	}
	buf, err := json.Marshal(picked)
	if err != nil {
		return "", fmt.Errorf("encode pk tuple: %w", err)
	}
	return string(buf), nil
}
