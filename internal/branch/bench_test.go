//go:build bench

package branch_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

// BenchmarkBranchCreate measures how long `CREATE DATABASE ... TEMPLATE` plus
// the event-trigger install + snapshot takes vs parent DB size. Mostly this
// is measuring Postgres, not us — the value is tracking whether the README's
// "sub-second for < 1 GB" claim still holds on typical dev machines.
//
// Run with: go test -tags=bench -bench=. -benchtime=1x -timeout=10m ./internal/branch
func BenchmarkBranchCreate(b *testing.B) {
	ctx := context.Background()
	sizes := []int{0, 10_000, 1_000_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("parent_rows=%d", n), func(b *testing.B) {
			admin, parentDB, cleanup := setupParentDB(b, ctx, n)
			defer cleanup()

			for i := 0; i < b.N; i++ {
				branchDB := fmt.Sprintf("pgbr_bench_%d_%d", n, i)
				_ = admin.DropDatabase(ctx, branchDB)

				b.StartTimer()
				if err := branch.Create(ctx, admin, fmt.Sprintf("bench-%d-%d", n, i), parentDB, branchDB); err != nil {
					b.StopTimer()
					b.Fatalf("branch create: %v", err)
				}
				b.StopTimer()

				_ = admin.DropDatabase(ctx, branchDB)
			}
		})
	}
}

func setupParentDB(b *testing.B, ctx context.Context, rowCount int) (*pg.Conn, string, func()) {
	b.Helper()
	b.StopTimer()

	admin, err := pg.Connect(ctx, benchBranchURL(b))
	if err != nil {
		b.Fatalf("connect admin: %v", err)
	}

	parentDB := fmt.Sprintf("pgbranch_bench_parent_%d", rowCount)
	_ = admin.DropDatabase(ctx, parentDB)

	if err := admin.CreateDatabase(ctx, parentDB); err != nil {
		admin.Close()
		b.Fatalf("create parent: %v", err)
	}

	conn, err := admin.ConnectToDatabase(ctx, parentDB)
	if err != nil {
		_ = admin.DropDatabase(ctx, parentDB)
		admin.Close()
		b.Fatalf("connect parent: %v", err)
	}
	if err := tracker.InstallTrackingSchema(ctx, conn); err != nil {
		conn.Close()
		_ = admin.DropDatabase(ctx, parentDB)
		admin.Close()
		b.Fatalf("install tracker: %v", err)
	}
	if err := conn.Exec(ctx, `
		CREATE TABLE widgets (
			id BIGINT PRIMARY KEY,
			payload TEXT NOT NULL
		)
	`); err != nil {
		conn.Close()
		_ = admin.DropDatabase(ctx, parentDB)
		admin.Close()
		b.Fatalf("create table: %v", err)
	}
	if rowCount > 0 {
		if err := conn.Exec(ctx, fmt.Sprintf(`
			INSERT INTO widgets (id, payload)
			SELECT g, repeat('x', 64) FROM generate_series(1, %d) g
		`, rowCount)); err != nil {
			conn.Close()
			_ = admin.DropDatabase(ctx, parentDB)
			admin.Close()
			b.Fatalf("seed parent: %v", err)
		}
	}
	conn.Close()

	cleanup := func() {
		_ = admin.DropDatabase(ctx, parentDB)
		admin.Close()
	}
	return admin, parentDB, cleanup
}

func benchBranchURL(b *testing.B) string {
	b.Helper()
	if u := os.Getenv("PG_BRANCH_TEST_URL"); u != "" {
		return u
	}
	return "postgresql://localhost:5432/postgres"
}
