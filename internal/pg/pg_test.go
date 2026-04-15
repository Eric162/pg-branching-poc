package pg_test

import (
	"context"
	"os"
	"testing"

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

func TestConnectAndPing(t *testing.T) {
	ctx := context.Background()
	conn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
}

func TestDatabaseName(t *testing.T) {
	ctx := context.Background()
	conn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	name, err := conn.DatabaseName()
	if err != nil {
		t.Fatalf("database name: %v", err)
	}
	if name != "postgres" {
		t.Errorf("expected 'postgres', got %q", name)
	}
}

func TestListDatabases(t *testing.T) {
	ctx := context.Background()
	conn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	dbs, err := conn.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	if len(dbs) == 0 {
		t.Error("expected at least one database")
	}
	// 'postgres' should always be present
	found := false
	for _, db := range dbs {
		if db == "postgres" {
			found = true
			break
		}
	}
	if !found {
		t.Error("'postgres' database not found in list")
	}
}

func TestCreateAndDropDatabase(t *testing.T) {
	ctx := context.Background()
	conn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	testDB := "pgbranch_test_create_drop"

	// Cleanup in case previous test run left it
	_ = conn.DropDatabase(ctx, testDB)

	// Create empty database
	err = conn.CreateDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("create database: %v", err)
	}

	// Verify exists
	exists, err := conn.DatabaseExists(ctx, testDB)
	if err != nil {
		t.Fatalf("database exists: %v", err)
	}
	if !exists {
		t.Fatal("database should exist after creation")
	}

	// Drop
	err = conn.DropDatabase(ctx, testDB)
	if err != nil {
		t.Fatalf("drop database: %v", err)
	}

	// Verify gone
	exists, err = conn.DatabaseExists(ctx, testDB)
	if err != nil {
		t.Fatalf("database exists after drop: %v", err)
	}
	if exists {
		t.Fatal("database should not exist after drop")
	}
}

func TestURLForDatabase(t *testing.T) {
	ctx := context.Background()
	conn, err := pg.Connect(ctx, testPGURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	url, err := conn.URLForDatabase("mydb")
	if err != nil {
		t.Fatalf("url for database: %v", err)
	}
	if url == "" {
		t.Error("expected non-empty URL")
	}
}
