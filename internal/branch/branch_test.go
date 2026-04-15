package branch_test

import (
	"context"
	"os"
	"testing"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/config"
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

// setupTestDB creates a temporary source database with tracking schema and a test table.
// Returns the admin connection and source DB name. Caller must clean up.
func setupTestDB(t *testing.T, ctx context.Context) (*pg.Conn, string) {
	t.Helper()
	adminConn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}

	sourceDB := "pgbranch_test_source"

	// Clean up any leftovers
	_ = adminConn.DropDatabase(ctx, sourceDB)
	_ = adminConn.DropDatabase(ctx, "pgbr_testbranch")

	// Create source DB
	err = adminConn.CreateDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("create source db: %v", err)
	}

	// Connect to source and install tracking + create test table
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	if err := tracker.InstallTrackingSchema(ctx, srcConn); err != nil {
		t.Fatalf("install tracking schema: %v", err)
	}

	// Create a test table with data
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

func TestCreateAndDeleteBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupTestDB(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_testbranch")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "testbranch", sourceDB, "pgbr_testbranch")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Verify branch DB exists
	exists, err := adminConn.DatabaseExists(ctx, "pgbr_testbranch")
	if err != nil {
		t.Fatalf("check branch exists: %v", err)
	}
	if !exists {
		t.Fatal("branch database should exist")
	}

	// Verify branch has the test table with data
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_testbranch")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}

	var count int
	err = branchConn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("count users: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 users, got %d", count)
	}
	branchConn.Close()

	// Verify metadata recorded in source DB
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source for metadata check: %v", err)
	}

	var branchName string
	err = srcConn.QueryRow(ctx,
		"SELECT name FROM _pgbranch.branches WHERE name = $1", "testbranch",
	).Scan(&branchName)
	if err != nil {
		t.Fatalf("query branch metadata: %v", err)
	}
	if branchName != "testbranch" {
		t.Errorf("expected 'testbranch', got %q", branchName)
	}
	srcConn.Close()

	// Delete branch
	err = branch.Delete(ctx, adminConn, "testbranch", sourceDB, "pgbr_testbranch")
	if err != nil {
		t.Fatalf("delete branch: %v", err)
	}

	exists, err = adminConn.DatabaseExists(ctx, "pgbr_testbranch")
	if err != nil {
		t.Fatalf("check branch deleted: %v", err)
	}
	if exists {
		t.Fatal("branch database should be deleted")
	}
}

func TestListBranches(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupTestDB(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_br1")
		_ = adminConn.DropDatabase(ctx, "pgbr_br2")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create two branches
	err := branch.Create(ctx, adminConn, "br1", sourceDB, "pgbr_br1")
	if err != nil {
		t.Fatalf("create br1: %v", err)
	}
	err = branch.Create(ctx, adminConn, "br2", sourceDB, "pgbr_br2")
	if err != nil {
		t.Fatalf("create br2: %v", err)
	}

	// List
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	state := &config.State{CurrentBranch: "br1"}
	branches, err := branch.List(ctx, srcConn, state)
	if err != nil {
		t.Fatalf("list branches: %v", err)
	}

	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d", len(branches))
	}

	// br1 should be current
	for _, b := range branches {
		if b.Name == "br1" && !b.IsCurrent {
			t.Error("br1 should be current")
		}
		if b.Name == "br2" && b.IsCurrent {
			t.Error("br2 should not be current")
		}
	}
}

func TestDDLTrackingOnBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupTestDB(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_ddltest")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "ddltest", sourceDB, "pgbr_ddltest")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Connect to branch and make schema changes
	branchConn, err := adminConn.ConnectToDatabase(ctx, "pgbr_ddltest")
	if err != nil {
		t.Fatalf("connect to branch: %v", err)
	}
	defer branchConn.Close()

	// Make DDL changes
	if err := branchConn.Exec(ctx, "ALTER TABLE users ADD COLUMN bio TEXT"); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	if err := branchConn.Exec(ctx, "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT, user_id INT REFERENCES users(id))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Read DDL log
	entries, err := tracker.ReadDDLLog(ctx, branchConn)
	if err != nil {
		t.Fatalf("read DDL log: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("expected DDL entries, got none")
	}

	// Should capture both ALTER TABLE and CREATE TABLE
	foundAlter := false
	foundCreate := false
	for _, e := range entries {
		if e.CommandTag == "ALTER TABLE" {
			foundAlter = true
		}
		if e.CommandTag == "CREATE TABLE" {
			foundCreate = true
		}
	}
	if !foundAlter {
		t.Error("expected ALTER TABLE in DDL log")
	}
	if !foundCreate {
		t.Error("expected CREATE TABLE in DDL log")
	}
}

func TestSchemaSnapshotOnBranch(t *testing.T) {
	ctx := context.Background()
	adminConn, sourceDB := setupTestDB(t, ctx)
	defer adminConn.Close()
	defer func() {
		_ = adminConn.DropDatabase(ctx, "pgbr_snaptest")
		_ = adminConn.DropDatabase(ctx, sourceDB)
	}()

	// Create branch
	err := branch.Create(ctx, adminConn, "snaptest", sourceDB, "pgbr_snaptest")
	if err != nil {
		t.Fatalf("create branch: %v", err)
	}

	// Load snapshot from parent metadata
	srcConn, err := adminConn.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer srcConn.Close()

	data, err := tracker.LoadSnapshot(ctx, srcConn, "snaptest")
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("snapshot data should not be empty")
	}

	// Parse snapshot
	snapshot, err := pg.SchemaSnapshotFromJSON(data)
	if err != nil {
		t.Fatalf("parse snapshot: %v", err)
	}

	// Should contain the users table
	foundUsers := false
	for _, tbl := range snapshot.Tables {
		if tbl.Name == "users" {
			foundUsers = true
			if len(tbl.Columns) < 3 {
				t.Errorf("users table should have at least 3 columns, got %d", len(tbl.Columns))
			}
		}
	}
	if !foundUsers {
		t.Error("users table not found in snapshot")
	}
}
