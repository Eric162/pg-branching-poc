package merge_test

import (
	"strings"
	"testing"

	"github.com/pg-branch/pg-branch/internal/merge"
)

func TestSummaryAppliedNoDataOps(t *testing.T) {
	r := &merge.MergeResult{
		SchemaOps: []merge.SchemaOp{{Description: "table public.x added", SQL: "CREATE TABLE x()", Status: "ok"}},
		Applied:   true,
	}
	out := r.Summary()
	if !strings.Contains(out, "Merge applied successfully.") {
		t.Errorf("expected plain success message, got:\n%s", out)
	}
	if strings.Contains(out, "NOT APPLIED") {
		t.Errorf("should not warn about data when there are no data ops, got:\n%s", out)
	}
}

func TestSummaryAppliedWithUnappliedDataOps(t *testing.T) {
	r := &merge.MergeResult{
		SchemaOps: []merge.SchemaOp{{Description: "column x.y added", SQL: "ALTER TABLE x ADD COLUMN y INT", Status: "ok"}},
		DataOps: []merge.DataOp{{
			Table:     "public.users",
			Operation: "SYNC",
			RowKey:    "3 rows (branch) vs 2 rows (main)",
			// no SQL → not actually applied
		}},
		Applied: true,
	}
	out := r.Summary()
	if !strings.Contains(out, "[NOT APPLIED]") {
		t.Errorf("expected each pending data op to be flagged [NOT APPLIED], got:\n%s", out)
	}
	if !strings.Contains(out, "DETECTED but NOT APPLIED") {
		t.Errorf("expected trailing warning about unapplied data changes, got:\n%s", out)
	}
	if strings.Contains(out, "Merge applied successfully.") {
		t.Errorf("should not claim success when data is unapplied, got:\n%s", out)
	}
}

func TestPendingDataChanges(t *testing.T) {
	cases := []struct {
		name string
		ops  []merge.DataOp
		want bool
	}{
		{name: "empty", ops: nil, want: false},
		{name: "all have SQL", ops: []merge.DataOp{{SQL: "UPDATE ..."}, {SQL: "INSERT ..."}}, want: false},
		{name: "one missing SQL", ops: []merge.DataOp{{SQL: "UPDATE ..."}, {SQL: ""}}, want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &merge.MergeResult{DataOps: c.ops}
			if got := r.PendingDataChanges(); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}
