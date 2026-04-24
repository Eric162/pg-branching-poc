package merge

import (
	"fmt"
	"strings"
)

// ConflictType classifies a merge conflict.
type ConflictType string

const (
	SchemaConflict ConflictType = "schema"
	DataConflict   ConflictType = "data"
)

// Conflict describes a merge conflict between branch and main.
type Conflict struct {
	Type       ConflictType
	ObjectName string // table.column or table for data
	BranchSide string // what branch did
	MainSide   string // what main did
}

// MergeResult captures the outcome of a merge operation.
type MergeResult struct {
	SchemaOps  []SchemaOp // DDL operations to apply
	DataOps    []DataOp   // DML operations to apply
	Conflicts  []Conflict
	DryRun     bool
	Applied    bool
}

// SchemaOp is a DDL operation to apply during merge.
type SchemaOp struct {
	Description string
	SQL         string
	Status      string // "ok", "conflict", "skipped"
}

// DataOp is a DML operation to apply during merge.
type DataOp struct {
	Table       string
	Operation   string // "INSERT_PK" (executable), "SYNC" (manual review only)
	RowKey      string // PK value(s) for display
	SQL         string
	Status      string // "ok", "conflict"
	BranchValue string // for conflict display
	MainValue   string // for conflict display

	// Execution hints — populated for INSERT_PK ops. applyMerge uses these
	// to stream rows from branch into main. Kept out of the human summary.
	Schema     string
	TableName  string
	PKColumns  []string
	AllColumns []string
}

// HasConflicts returns true if any conflicts exist.
func (r *MergeResult) HasConflicts() bool {
	return len(r.Conflicts) > 0
}

// PendingDataChanges returns true if the merge detected data differences
// that can't be applied automatically (no primary key, or operations beyond
// append-only inserts). These show up in the summary as [NOT APPLIED] so
// users don't mistake a partial data merge for a full one.
func (r *MergeResult) PendingDataChanges() bool {
	for _, op := range r.DataOps {
		if op.Operation == "SYNC" {
			return true
		}
	}
	return false
}

// Summary returns a human-readable merge summary.
func (r *MergeResult) Summary() string {
	var b strings.Builder

	if r.DryRun {
		fmt.Fprintln(&b, "=== DRY RUN (no changes applied) ===")
		fmt.Fprintln(&b)
	}

	// Schema operations
	if len(r.SchemaOps) > 0 {
		fmt.Fprintf(&b, "Schema operations (%d):\n", len(r.SchemaOps))
		for _, op := range r.SchemaOps {
			marker := "[OK]"
			if op.Status == "conflict" {
				marker = "[CONFLICT]"
			} else if op.Status == "skipped" {
				marker = "[SKIP]"
			}
			fmt.Fprintf(&b, "  %s %s\n", marker, op.Description)
		}
		fmt.Fprintln(&b)
	}

	// Data operations
	if len(r.DataOps) > 0 {
		fmt.Fprintf(&b, "Data operations (%d):\n", len(r.DataOps))
		for _, op := range r.DataOps {
			marker := "[OK]"
			switch {
			case op.Status == "conflict":
				marker = "[CONFLICT]"
			case op.Operation == "SYNC":
				// No executable plan — flagged for manual review only.
				marker = "[NOT APPLIED]"
			}
			fmt.Fprintf(&b, "  %s %s %s (key: %s)\n", marker, op.Operation, op.Table, op.RowKey)
		}
		fmt.Fprintln(&b)
	}

	if len(r.SchemaOps) == 0 && len(r.DataOps) == 0 {
		fmt.Fprintln(&b, "Nothing to merge.")
	}

	// Conflicts
	if r.HasConflicts() {
		fmt.Fprintf(&b, "CONFLICTS (%d):\n", len(r.Conflicts))
		for _, c := range r.Conflicts {
			fmt.Fprintf(&b, "  %s %s:\n", c.Type, c.ObjectName)
			fmt.Fprintf(&b, "    Branch: %s\n", c.BranchSide)
			fmt.Fprintf(&b, "    Main:   %s\n", c.MainSide)
		}
		fmt.Fprintln(&b)
		fmt.Fprintln(&b, "Resolve with --resolve=branch or --resolve=main")
	}

	if r.Applied {
		if r.PendingDataChanges() {
			fmt.Fprintln(&b, "Schema merge applied. Data changes were DETECTED but NOT APPLIED — review the data ops above and sync manually.")
		} else {
			fmt.Fprintln(&b, "Merge applied successfully.")
		}
	}

	return b.String()
}
