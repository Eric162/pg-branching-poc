//go:build bench

package diff_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pg-branch/pg-branch/internal/diff"
	"github.com/pg-branch/pg-branch/internal/pg"
)

// BenchmarkComputeTableChecksums measures full-table checksum cost vs row count.
// Gated behind the `bench` build tag so `go test ./...` doesn't pay for it.
// Run with: go test -tags=bench -bench=. -benchtime=1x ./internal/diff
//
// -benchtime=1x is the right default here: the work is dominated by populating
// the fixture table (seconds at 1M rows), so N>1 just re-seeds without adding
// signal. Re-run the benchmark itself multiple times via -count if you want
// variance estimates.
func BenchmarkComputeTableChecksums(b *testing.B) {
	ctx := context.Background()
	sizes := []int64{1_000, 100_000, 1_000_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			dbName := fmt.Sprintf("pgbranch_bench_diff_%d", n)
			conn, cleanup := setupBenchDB(b, ctx, dbName, n)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := diff.ComputeTableChecksums(ctx, conn, nil); err != nil {
					b.Fatalf("checksum: %v", err)
				}
			}
		})
	}
}

// setupBenchDB creates a dedicated DB with a single `bench_rows` table
// populated with n rows. Returns a connection to the DB and a cleanup func.
func setupBenchDB(b *testing.B, ctx context.Context, dbName string, n int64) (*pg.Conn, func()) {
	b.Helper()
	admin, err := pg.Connect(ctx, benchPGURL(b))
	if err != nil {
		b.Fatalf("connect admin: %v", err)
	}

	_ = admin.DropDatabase(ctx, dbName)
	if err := admin.CreateDatabase(ctx, dbName); err != nil {
		admin.Close()
		b.Fatalf("create db: %v", err)
	}

	conn, err := admin.ConnectToDatabase(ctx, dbName)
	if err != nil {
		_ = admin.DropDatabase(ctx, dbName)
		admin.Close()
		b.Fatalf("connect db: %v", err)
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE bench_rows (
			id BIGINT PRIMARY KEY,
			payload TEXT NOT NULL
		)
	`); err != nil {
		conn.Close()
		_ = admin.DropDatabase(ctx, dbName)
		admin.Close()
		b.Fatalf("create table: %v", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO bench_rows (id, payload)
		SELECT g, 'row-' || g FROM generate_series(1, %d) g
	`, n)); err != nil {
		conn.Close()
		_ = admin.DropDatabase(ctx, dbName)
		admin.Close()
		b.Fatalf("seed rows: %v", err)
	}

	cleanup := func() {
		conn.Close()
		_ = admin.DropDatabase(ctx, dbName)
		admin.Close()
	}
	return conn, cleanup
}

func benchPGURL(b *testing.B) string {
	b.Helper()
	if u := os.Getenv("PG_BRANCH_TEST_URL"); u != "" {
		return u
	}
	return "postgresql://localhost:5432/postgres"
}
