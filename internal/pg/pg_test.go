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

func TestURLForDatabasePackageLevel(t *testing.T) {
	cases := []struct {
		name, in, db, want string
	}{
		{"simple", "postgresql://localhost:5432/postgres", "mydb", "postgresql://localhost:5432/mydb"},
		{"with credentials", "postgresql://alice:secret@pg.example.com:6432/old", "new", "postgresql://alice:secret@pg.example.com:6432/new"},
		{"with query params", "postgresql://localhost/postgres?sslmode=require", "mydb", "postgresql://localhost/mydb?sslmode=require"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := pg.URLForDatabase(c.in, c.db)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestServerURL(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"strip db", "postgresql://localhost:5432/postgres", "postgresql://localhost:5432/"},
		{"preserve creds", "postgresql://alice:secret@pg.example.com:6432/old", "postgresql://alice:secret@pg.example.com:6432/"},
		{"preserve query params", "postgresql://localhost/postgres?sslmode=require", "postgresql://localhost/?sslmode=require"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := pg.ServerURL(c.in)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}
