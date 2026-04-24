//go:build bench

package merge_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/pg-branch/pg-branch/internal/branch"
	"github.com/pg-branch/pg-branch/internal/merge"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

// BenchmarkMergeApply_PKInsert measures end-to-end merge time when a branch
// has added N new PK'd rows and main is unchanged. This exercises the
// "common path" — branch-adds-rows — and is where the in-Go PK dedup set
// lives, so scaling here tells us whether the fetchPKSet / map approach
// needs to move to a server-side NOT EXISTS rewrite.
//
// Run with: go test -tags=bench -bench=. -benchtime=1x -timeout=10m ./internal/merge
//
// Each sub-bench re-seeds from scratch, so N>1 multiplies wall-clock without
// adding signal. Use -count to get variance across runs.
func BenchmarkMergeApply_PKInsert(b *testing.B) {
	ctx := context.Background()
	sizes := []int{1_000, 10_000, 100_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("branch_rows=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				runMergeBench(b, ctx, n)
			}
		})
	}
}

func runMergeBench(b *testing.B, ctx context.Context, branchNewRows int) {
	b.Helper()
	b.StopTimer()

	admin, err := pg.Connect(ctx, benchMergeURL(b))
	if err != nil {
		b.Fatalf("connect admin: %v", err)
	}
	defer admin.Close()

	sourceDB := "pgbranch_bench_merge_src"
	branchDB := "pgbr_benchmerge"

	_ = admin.DropDatabase(ctx, branchDB)
	_ = admin.DropDatabase(ctx, sourceDB)
	defer func() {
		_ = admin.DropDatabase(ctx, branchDB)
		_ = admin.DropDatabase(ctx, sourceDB)
	}()

	if err := admin.CreateDatabase(ctx, sourceDB); err != nil {
		b.Fatalf("create source: %v", err)
	}

	src, err := admin.ConnectToDatabase(ctx, sourceDB)
	if err != nil {
		b.Fatalf("connect source: %v", err)
	}
	if err := tracker.InstallTrackingSchema(ctx, src); err != nil {
		b.Fatalf("install tracker: %v", err)
	}
	if err := src.Exec(ctx, `
		CREATE TABLE widgets (
			id BIGINT PRIMARY KEY,
			label TEXT NOT NULL
		)
	`); err != nil {
		b.Fatalf("create table: %v", err)
	}
	// Seed a small main-side baseline so the schema snapshot sees real columns.
	if err := src.Exec(ctx, `INSERT INTO widgets VALUES (1, 'seed')`); err != nil {
		b.Fatalf("seed: %v", err)
	}
	src.Close()

	if err := branch.Create(ctx, admin, "benchmerge", sourceDB, branchDB); err != nil {
		b.Fatalf("branch create: %v", err)
	}

	// Populate branch-side rows with PKs that don't collide with main's seed.
	bconn, err := admin.ConnectToDatabase(ctx, branchDB)
	if err != nil {
		b.Fatalf("connect branch: %v", err)
	}
	if err := bconn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO widgets (id, label)
		SELECT g + 1000, 'branch-' || g FROM generate_series(1, %d) g
	`, branchNewRows)); err != nil {
		bconn.Close()
		b.Fatalf("seed branch rows: %v", err)
	}
	bconn.Close()

	b.StartTimer()
	res, err := merge.Execute(ctx, admin, merge.Options{
		BranchName: "benchmerge",
		BranchDB:   branchDB,
		MainDB:     sourceDB,
		DryRun:     false,
		Stderr:     io.Discard,
	})
	b.StopTimer()

	if err != nil {
		b.Fatalf("merge: %v", err)
	}
	if !res.Applied {
		b.Fatal("merge should be applied")
	}
}

func benchMergeURL(b *testing.B) string {
	b.Helper()
	if u := os.Getenv("PG_BRANCH_TEST_URL"); u != "" {
		return u
	}
	return "postgresql://localhost:5432/postgres"
}
