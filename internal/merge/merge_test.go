package merge_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/merge"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

func testPGURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("PG_BRANCH_TEST_URL")
	if u == "" {
		u = "postgresql://localhost:5432/postgres"
	}
	return u
}

func setupMergeTest(t *testing.T, ctx context.Context) (*pg.Conn, string) {
	t.Helper()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}

	sourceDB := "pgbranch_merge_source"

	// Clean up
	_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
	_ = adminConn.DropDatabase(ctx, sourceDB)

	// Create source DB with table
	err = adminConn.CreateDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("create source db: %v", err)
	}

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	if err := tracker.InstallTrackingSchema(ctx, srcConn); err != nil {
		t.Fatalf("install tracking schema: %v", err)
	}

	if err := srcConn.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE
		);
		INSERT INTO users (name, email) VALUES
			('Alice', 'alice@example.com'),
			('Bob', 'bob@example.com');
	`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	return adminConn, sourceDB
}

func TestMergeDryRun_SchemaOnlyOnBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Make schema changes on branch only
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}

	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN bio TEXT"); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	if err := branchConn.Exec(ctx, "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	branchConn.Close()

	// Dry-run merge
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("merge dry-run: %v", err)
	}

	if result.Applied {
		t.Error("dry run should not apply")
	}

	if len(result.SchemaOps) == 0 {
		t.Error("expected schema operations")
	}

	if result.HasConflicts() {
		t.Errorf("expected no conflicts, got %d", len(result.Conflicts))
	}

	// Verify main is unchanged (dry run)
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	var hasBio bool
	err = srcConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'bio')").Scan(&hasBio)
	if err != nil {
		t.Fatalf("check bio column: %v", err)
	}
	if hasBio {
		t.Error("main should not have 'bio' column after dry run")
	}
}

func TestMergeApply_SchemaOnlyOnBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Make schema change on branch
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN bio TEXT"); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	branchConn.Close()

	// Apply merge
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     false,
	})
	if err != nil {
		t.Fatalf("merge apply: %v", err)
	}

	if !result.Applied {
		t.Error("merge should be applied")
	}

	// Verify main now has bio column
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	var hasBio bool
	err = srcConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'bio')").Scan(&hasBio)
	if err != nil {
		t.Fatalf("check bio column: %v", err)
	}
	if !hasBio {
		t.Error("main should have 'bio' column after merge apply")
	}
}

func TestMergeConflict_BothSidesModify(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Modify on branch: add bio column
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN bio TEXT"); err != nil {
		t.Fatalf("branch alter: %v", err)
	}
	branchConn.Close()

	// Modify on main: add different column with same concept (age)
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	if err := srcConn.Exec(ctx, "ALTER TABLE users ADD COLUMN age INT"); err != nil {
		t.Fatalf("main alter: %v", err)
	}
	srcConn.Close()

	// Merge — should detect that both sides modified 'users' table but different columns
	// This should NOT conflict because they're different columns.
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if result.HasConflicts() {
		t.Errorf("different columns on the same table should not conflict; got %d conflicts:\n%s",
			len(result.Conflicts), result.Summary())
	}

	// The branch's 'bio' should be scheduled for replay.
	var sawBio bool
	for _, op := range result.SchemaOps {
		if op.Status == "ok" && strings.Contains(op.Description, "bio") {
			sawBio = true
		}
	}
	if !sawBio {
		t.Errorf("expected a schema op for 'bio'; got:\n%s", result.Summary())
	}
}

func TestMergeConflict_SameColumnBothSides(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Both sides add same column with different types
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN status TEXT"); err != nil {
		t.Fatalf("branch alter: %v", err)
	}
	branchConn.Close()

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	if err := srcConn.Exec(ctx, "ALTER TABLE users ADD COLUMN status INT"); err != nil {
		t.Fatalf("main alter: %v", err)
	}
	srcConn.Close()

	// Merge should detect conflict on same column name with different types.
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	// Dry-run doesn't fail on conflicts (--apply would). The merge must return
	// a result and report the collision.
	if err != nil {
		t.Fatalf("dry-run merge should not error on conflicts: %v", err)
	}
	if result == nil {
		t.Fatal("expected a result, got nil")
	}
	if !result.HasConflicts() {
		t.Fatalf("expected a conflict on public.users.status; got none:\n%s", result.Summary())
	}

	var sawStatus bool
	for _, c := range result.Conflicts {
		if c.ObjectName == "public.users.status" && c.Type == merge.SchemaConflict {
			sawStatus = true
		}
	}
	if !sawStatus {
		t.Errorf("expected SchemaConflict on public.users.status; got %+v", result.Conflicts)
	}
}

func TestMergeWithDataChanges(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Add data on branch only
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	if err := branchConn.Exec(ctx,
		"INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')"); err != nil {
		t.Fatalf("insert on branch: %v", err)
	}
	branchConn.Close()

	// Dry-run merge — should detect data changes
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(result.DataOps) == 0 {
		t.Fatal("expected data operations for inserted row")
	}

	var sawUsers bool
	for _, op := range result.DataOps {
		if op.Table == "public.users" {
			sawUsers = true
			if !strings.Contains(op.RowKey, "3 rows") || !strings.Contains(op.RowKey, "2 rows") {
				t.Errorf("expected row-count detail '3 rows (branch) vs 2 rows (main)', got %q", op.RowKey)
			}
			if op.Operation != "INSERT_PK" {
				t.Errorf("expected INSERT_PK plan for PK'd table, got %q", op.Operation)
			}
		}
	}
	if !sawUsers {
		t.Errorf("expected a data op for public.users, got: %+v", result.DataOps)
	}

	// public.users has a primary key, so the plan is executable — the
	// summary should NOT warn about unapplied data.
	if result.PendingDataChanges() {
		t.Errorf("expected PendingDataChanges()=false when PK is available, got true; summary:\n%s", result.Summary())
	}
}

// TestMergeApply_InsertsNewRows exercises the PK-based data merge end-to-end:
// branch adds a new row to a PK'd table, --apply runs the merge, and the new
// row is present on main afterwards. Existing rows in main that aren't on
// branch are left alone.
func TestMergeApply_InsertsNewRows(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	if err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Insert on branch only.
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect branch: %v", err)
	}
	if err := branchConn.Exec(ctx,
		"INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	branchConn.Close()

	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     false,
	})
	if err != nil {
		t.Fatalf("merge apply: %v", err)
	}
	if !result.Applied {
		t.Fatal("merge should be applied")
	}

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()

	var count int
	if err := srcConn.QueryRow(ctx, "SELECT count(*) FROM users WHERE email = 'charlie@example.com'").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected Charlie on main after merge, got count=%d", count)
	}

	// The original rows are still there.
	var total int
	if err := srcConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&total); err != nil {
		t.Fatalf("total count: %v", err)
	}
	if total != 3 {
		t.Errorf("expected 3 users on main (Alice, Bob, Charlie), got %d", total)
	}
}

// TestMergeApply_SkipsRowsAlreadyOnMain covers the deduplication path: a row
// that's present on both sides with the same PK is not re-inserted.
func TestMergeApply_SkipsRowsAlreadyOnMain(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	if err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Add Charlie on both sides with the same PK (serial 3 after the two
	// seed rows on each side). A dry-run reports a divergence because of
	// the column order but no new PKs — apply should insert 0 rows.
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect branch: %v", err)
	}
	if err := branchConn.Exec(ctx,
		"INSERT INTO users (id, name, email) VALUES (10, 'Charlie', 'charlie@example.com')"); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	branchConn.Close()

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	if err := srcConn.Exec(ctx,
		"INSERT INTO users (id, name, email) VALUES (10, 'Charlie', 'charlie@example.com')"); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	srcConn.Close()

	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     false,
	})
	if err != nil {
		t.Fatalf("merge apply: %v", err)
	}

	// Verify main still has just 3 rows (no duplicate-PK conflict).
	srcConn2, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn2.Close()

	var total int
	if err := srcConn2.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 3 {
		t.Errorf("expected 3 rows on main (dedupe kept the existing Charlie), got %d", total)
	}
	_ = result
}

// TestMergeTableWithoutPKFallsBackToManualReview: a row diff on a table that
// has no primary key can't be safely auto-merged, so the op stays [NOT APPLIED].
func TestMergeTableWithoutPKFallsBackToManualReview(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	if err := srcConn.Exec(ctx, `
		CREATE TABLE events (at TIMESTAMPTZ, kind TEXT);
		INSERT INTO events VALUES (now(), 'boot');
	`); err != nil {
		t.Fatalf("create events: %v", err)
	}
	srcConn.Close()

	if err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect branch: %v", err)
	}
	if err := branchConn.Exec(ctx, "INSERT INTO events VALUES (now(), 'click')"); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	branchConn.Close()

	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	var sawEvents bool
	for _, op := range result.DataOps {
		if op.Table == "public.events" {
			sawEvents = true
			if op.Operation != "SYNC" {
				t.Errorf("expected SYNC (no-PK fallback), got %q", op.Operation)
			}
			if !strings.Contains(op.RowKey, "no primary key") {
				t.Errorf("expected no-PK annotation in RowKey, got %q", op.RowKey)
			}
		}
	}
	if !sawEvents {
		t.Errorf("expected a data op for public.events, got: %+v", result.DataOps)
	}
	if !result.PendingDataChanges() {
		t.Error("expected PendingDataChanges()=true for no-PK table")
	}
}

// TestMergeNoDataFlag: --no-data skips the checksum step entirely; no data
// ops are emitted even when rows differ.
func TestMergeNoDataFlag(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	if err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect branch: %v", err)
	}
	if err := branchConn.Exec(ctx,
		"INSERT INTO users (name, email) VALUES ('Charlie', 'c@x')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	branchConn.Close()

	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
		NoData:     true,
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(result.DataOps) != 0 {
		t.Errorf("expected no data ops with --no-data, got %d: %+v", len(result.DataOps), result.DataOps)
	}
}

func TestMergeAdvisoryLockBlocksConcurrent(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	if err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Hold the same advisory lock on a separate connection to simulate a
	// concurrent merge that's still running.
	mainConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to main: %v", err)
	}
	defer mainConn.Close()

	lockKey := "pgbranch:merge:" + sourceDB + ":mergebranch"
	held, err := mainConn.TryAdvisoryLock(ctx, lockKey)
	if err != nil {
		t.Fatalf("pre-acquire lock: %v", err)
	}
	if held == nil {
		t.Fatal("expected to acquire the lock in test setup")
	}
	defer held.Release(ctx)

	// A merge of the same branch/main should now refuse to run.
	_, err = merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
	})
	if err == nil {
		t.Fatal("expected merge to fail while advisory lock is held")
	}
	if !strings.Contains(err.Error(), "in progress") {
		t.Errorf("expected 'in progress' error, got: %v", err)
	}

	// --no-lock should bypass and succeed.
	if _, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
		NoLock:     true,
	}); err != nil {
		t.Errorf("merge with NoLock should bypass the lock, got: %v", err)
	}
}

func TestMergeResolveModeBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupMergeTest(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_mergebranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	err := branch.Create(ctx, adminConn, "mergebranch", sourceDB, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Both sides add same column
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_mergebranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN status TEXT"); err != nil {
		t.Fatalf("branch alter: %v", err)
	}
	branchConn.Close()

	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	if err := srcConn.Exec(ctx, "ALTER TABLE users ADD COLUMN status INT"); err != nil {
		t.Fatalf("main alter: %v", err)
	}
	srcConn.Close()

	// Merge with resolve=branch — conflicts should be resolved
	result, err := merge.Execute(ctx, adminConn, merge.Options{
		BranchName: "mergebranch",
		BranchDB:   "pgbr_mergebranch",
		MainDB:     sourceDB,
		DryRun:     true,
		Resolve:    merge.ResolveBranch,
	})
	if err != nil {
		t.Fatalf("merge with resolve=branch: %v", err)
	}

	if result.HasConflicts() {
		t.Error("expected no conflicts when resolve=branch")
	}
}
