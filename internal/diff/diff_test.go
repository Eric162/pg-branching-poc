package diff_test

import (
	"context"
	"os"
	"testing"

	"github.com/pg-branch/pg-branch/internal/diff"
	"github.com/pg-branch/pg-branch/internal/pg"
)

func testPGURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("PG_BRANCH_TEST_URL")
	if u == "" {
		u = "postgresql://localhost:5432/postgres"
	}
	return u
}

func TestSchemaDiffDetectsAddedTable(t *testing.T) {
	base := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "id", DataType: "integer", OrdinalPos: 1},
			}},
		},
	}
	current := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "id", DataType: "integer", OrdinalPos: 1},
			}},
			{Schema: "public", Name: "posts", Columns: []pg.ColumnInfo{
				{Name: "id", DataType: "integer", OrdinalPos: 1},
				{Name: "title", DataType: "text", OrdinalPos: 2},
			}},
		},
	}

	changes := diff.SchemaDiff(base, current)
	found := false
	for _, c := range changes {
		if c.ObjectKind == "table" && c.ObjectName == "public.posts" && c.Type == diff.Added {
			found = true
		}
	}
	if !found {
		t.Error("expected to detect added table 'public.posts'")
	}
}

func TestSchemaDiffDetectsAddedColumn(t *testing.T) {
	base := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "id", DataType: "integer", OrdinalPos: 1},
				{Name: "name", DataType: "text", OrdinalPos: 2},
			}},
		},
	}
	current := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "id", DataType: "integer", OrdinalPos: 1},
				{Name: "name", DataType: "text", OrdinalPos: 2},
				{Name: "bio", DataType: "text", OrdinalPos: 3, IsNullable: true},
			}},
		},
	}

	changes := diff.SchemaDiff(base, current)
	found := false
	for _, c := range changes {
		if c.ObjectKind == "column" && c.ObjectName == "public.users.bio" && c.Type == diff.Added {
			found = true
		}
	}
	if !found {
		t.Error("expected to detect added column 'public.users.bio'")
	}
}

func TestSchemaDiffDetectsRemovedTable(t *testing.T) {
	base := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users"},
			{Schema: "public", Name: "old_table"},
		},
	}
	current := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users"},
		},
	}

	changes := diff.SchemaDiff(base, current)
	found := false
	for _, c := range changes {
		if c.ObjectKind == "table" && c.ObjectName == "public.old_table" && c.Type == diff.Removed {
			found = true
		}
	}
	if !found {
		t.Error("expected to detect removed table 'public.old_table'")
	}
}

func TestSchemaDiffDetectsModifiedColumn(t *testing.T) {
	base := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "status", DataType: "text"},
			}},
		},
	}
	current := &pg.SchemaSnapshot{
		Tables: []pg.TableInfo{
			{Schema: "public", Name: "users", Columns: []pg.ColumnInfo{
				{Name: "status", DataType: "character varying"},
			}},
		},
	}

	changes := diff.SchemaDiff(base, current)
	found := false
	for _, c := range changes {
		if c.ObjectKind == "column" && c.Type == diff.Modified {
			found = true
		}
	}
	if !found {
		t.Error("expected to detect modified column type")
	}
}

func TestComputeTableChecksums(t *testing.T) {
	ctx := context.Background()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer adminConn.Close()

	testDB := "pgbranch_test_checksums"
	_ = adminConn.DropDatabase(ctx, testDB)
	defer func() { _ = adminConn.DropDatabase(ctx, testDB) }()

	err = adminConn.CreateDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}

	conn, err := adminConn.ConnectToDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}
	defer conn.Close()

	// Create table with data
	err = conn.Exec(ctx, `
		CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT);
		INSERT INTO items (name) VALUES ('apple'), ('banana');
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	checksums, err := diff.ComputeTableChecksums(ctx, conn, nil)
	if err != nil {
		t.Fatalf("compute checksums: %v", err)
	}

	if len(checksums) == 0 {
		t.Fatal("expected at least one checksum")
	}

	found := false
	for _, cs := range checksums {
		if cs.Table == "items" {
			found = true
			if cs.RowCount != 2 {
				t.Errorf("expected 2 rows, got %d", cs.RowCount)
			}
			if cs.Checksum == "" {
				t.Error("expected non-empty checksum")
			}
		}
	}
	if !found {
		t.Error("items table not found in checksums")
	}
}

func TestCompareDataDetectsChanges(t *testing.T) {
	base := []diff.TableChecksum{
		{Schema: "public", Table: "users", Checksum: "abc123", RowCount: 5},
		{Schema: "public", Table: "orders", Checksum: "def456", RowCount: 10},
	}
	current := []diff.TableChecksum{
		{Schema: "public", Table: "users", Checksum: "abc123", RowCount: 5},   // unchanged
		{Schema: "public", Table: "orders", Checksum: "xyz789", RowCount: 12}, // changed
	}

	changes := diff.CompareData(base, current)
	foundChanged := false
	foundUnchanged := false
	for _, c := range changes {
		if c.Table == "orders" && c.HasChange {
			foundChanged = true
		}
		if c.Table == "users" && !c.HasChange {
			foundUnchanged = true
		}
	}
	if !foundChanged {
		t.Error("expected orders to be flagged as changed")
	}
	if !foundUnchanged {
		t.Error("expected users to be flagged as unchanged")
	}
}

// TestChecksumStableAcrossThreshold asserts that the same table data produces
// the same checksum whether the threshold treats it as "small" (single quick
// scan) or "large" (progress-reporting scan). Prior to unifying the two paths,
// the streaming version summed int64 with Go wrap-around while the SQL version
// summed into arbitrary-precision numeric — so identical data could produce
// different digests depending on table size.
func TestChecksumStableAcrossThreshold(t *testing.T) {
	ctx := context.Background()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer adminConn.Close()

	testDB := "pgbranch_test_threshold"
	_ = adminConn.DropDatabase(ctx, testDB)
	defer func() { _ = adminConn.DropDatabase(ctx, testDB) }()

	if err := adminConn.CreateDatabase(ctx, testDB); err != nil {
		t.Fatalf("create test db: %v", err)
	}
	conn, err := adminConn.ConnectToDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}
	defer conn.Close()

	// Seed ~200 rows so both paths have something non-trivial to sum.
	if err := conn.Exec(ctx, `
		CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT, val INT);
		INSERT INTO items (name, val)
		SELECT 'item-' || g::text, (g * 7919) % 1000000
		FROM generate_series(1, 200) g;
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Record and restore the threshold so we don't leak state to other tests.
	origThreshold := diff.LargeTableThresholdForTest()
	defer diff.SetLargeTableThresholdForTest(origThreshold)

	progress := func(_, _ int, _ string) {}

	diff.SetLargeTableThresholdForTest(1_000_000) // small-path
	small, err := diff.ComputeTableChecksums(ctx, conn, progress)
	if err != nil {
		t.Fatalf("small-path checksums: %v", err)
	}

	diff.SetLargeTableThresholdForTest(1) // large-path on everything
	large, err := diff.ComputeTableChecksums(ctx, conn, progress)
	if err != nil {
		t.Fatalf("large-path checksums: %v", err)
	}

	if len(small) != len(large) {
		t.Fatalf("row count differs between paths: small=%d large=%d", len(small), len(large))
	}
	for i := range small {
		if small[i].Table != large[i].Table {
			t.Fatalf("table order differs: %q vs %q", small[i].Table, large[i].Table)
		}
		if small[i].Checksum != large[i].Checksum {
			t.Errorf("checksum for %s.%s diverges between paths:\n  small: %s\n  large: %s",
				small[i].Schema, small[i].Table, small[i].Checksum, large[i].Checksum)
		}
	}
}

// TestChecksumEmptyTable pins the "no rows" sentinel — it must be stable and
// non-empty so downstream comparators don't treat empty tables as missing.
func TestChecksumEmptyTable(t *testing.T) {
	ctx := context.Background()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer adminConn.Close()

	testDB := "pgbranch_test_empty"
	_ = adminConn.DropDatabase(ctx, testDB)
	defer func() { _ = adminConn.DropDatabase(ctx, testDB) }()

	if err := adminConn.CreateDatabase(ctx, testDB); err != nil {
		t.Fatalf("create test db: %v", err)
	}
	conn, err := adminConn.ConnectToDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}
	defer conn.Close()

	if err := conn.Exec(ctx, "CREATE TABLE empty (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	checksums, err := diff.ComputeTableChecksums(ctx, conn, nil)
	if err != nil {
		t.Fatalf("checksums: %v", err)
	}
	if len(checksums) != 1 {
		t.Fatalf("expected 1 checksum, got %d", len(checksums))
	}
	if checksums[0].RowCount != 0 {
		t.Errorf("expected row count 0, got %d", checksums[0].RowCount)
	}
	if checksums[0].Checksum == "" {
		t.Error("empty-table checksum should be a stable sentinel, not empty string")
	}
}

func TestFormatChanges(t *testing.T) {
	changes := []diff.SchemaChange{
		{Type: diff.Added, ObjectKind: "table", ObjectName: "public.posts", Detail: "table public.posts added (2 columns)"},
		{Type: diff.Modified, ObjectKind: "column", ObjectName: "public.users.bio", Detail: "column public.users.bio type changed"},
	}

	output := diff.FormatChanges(changes)
	if output == "" {
		t.Error("expected non-empty output")
	}
}
