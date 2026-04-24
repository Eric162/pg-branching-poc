package tracker_test

import (
	"context"
	"os"
	"testing"

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

func setupTrackerTestDB(t *testing.T, ctx context.Context, dbName string) *pg.Conn {
	t.Helper()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(adminConn.Close)

	_ = adminConn.DropDatabase(ctx, dbName)
	if err := adminConn.CreateDatabase(ctx, dbName); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.DropDatabase(ctx, dbName) })

	conn, err := adminConn.ConnectToDatabase(ctx, dbName)
	if err != nil {
		t.Fatalf("connect to %s: %v", dbName, err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func TestInstallTrackingSchemaIsIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := setupTrackerTestDB(t, ctx, "pgbranch_tracker_idemp")

	if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
		t.Fatalf("first install: %v", err)
	}
	// Seed a row so we can verify a second install doesn't wipe data.
	if err := conn.Exec(ctx,
		`INSERT INTO _pgbranch.branches (name, db_name, parent_db) VALUES ('b1', 'pgbr_b1', 'main')`,
	); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
		t.Fatalf("second install: %v", err)
	}
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM _pgbranch.branches`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("re-install wiped data: want 1 row, got %d", count)
	}
}

func TestHasTrackingSchema(t *testing.T) {
	ctx := context.Background()
	conn := setupTrackerTestDB(t, ctx, "pgbranch_tracker_has")

	has, err := tracker.HasTrackingSchema(ctx, conn)
	if err != nil {
		t.Fatalf("has (pre-install): %v", err)
	}
	if has {
		t.Error("expected false before install")
	}

	if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
		t.Fatalf("install: %v", err)
	}

	has, err = tracker.HasTrackingSchema(ctx, conn)
	if err != nil {
		t.Fatalf("has (post-install): %v", err)
	}
	if !has {
		t.Error("expected true after install")
	}
}

func TestDDLTriggerCapturesCommands(t *testing.T) {
	ctx := context.Background()
	conn := setupTrackerTestDB(t, ctx, "pgbranch_tracker_ddl")

	if err := tracker.InstallDDLTrigger(ctx, conn); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	// Run a mix of DDL commands — each should leave an entry in ddl_log.
	for _, stmt := range []string{
		`CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)`,
		`ALTER TABLE items ADD COLUMN qty INT`,
		`CREATE INDEX idx_items_name ON items (name)`,
	} {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	entries, err := tracker.ReadDDLLog(ctx, conn)
	if err != nil {
		t.Fatalf("read ddl log: %v", err)
	}

	want := map[string]bool{
		"CREATE TABLE": false,
		"ALTER TABLE":  false,
		"CREATE INDEX": false,
	}
	for _, e := range entries {
		if _, ok := want[e.CommandTag]; ok {
			want[e.CommandTag] = true
		}
	}
	for tag, seen := range want {
		if !seen {
			t.Errorf("expected DDL log to capture %q", tag)
		}
	}
}

func TestReadDDLLogOnEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	conn := setupTrackerTestDB(t, ctx, "pgbranch_tracker_empty")

	if err := tracker.InstallDDLTrigger(ctx, conn); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	entries, err := tracker.ReadDDLLog(ctx, conn)
	if err != nil {
		t.Fatalf("read ddl log: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty log on fresh database, got %d entries", len(entries))
	}
}

func TestLoadSnapshotMissingBranch(t *testing.T) {
	ctx := context.Background()
	conn := setupTrackerTestDB(t, ctx, "pgbranch_tracker_snap_miss")

	if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
		t.Fatalf("install: %v", err)
	}

	// Never inserted a row for "ghost" — LoadSnapshot must surface an error
	// rather than silently return empty bytes.
	if _, err := tracker.LoadSnapshot(ctx, conn, "ghost"); err == nil {
		t.Error("expected error for missing branch snapshot, got nil")
	}
}
