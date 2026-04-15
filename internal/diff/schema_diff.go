package diff

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pg-branch/pg-branch/internal/pg"
)

// ChangeType classifies a schema change.
type ChangeType string

const (
	Added    ChangeType = "added"
	Removed  ChangeType = "removed"
	Modified ChangeType = "modified"
)

// SchemaChange describes a single schema difference.
type SchemaChange struct {
	Type       ChangeType
	ObjectKind string // "table", "column", "index", "constraint"
	ObjectName string // fully qualified name
	Detail     string // human-readable description
}

// SchemaDiff compares two schema snapshots and returns the changes.
func SchemaDiff(base, current *pg.SchemaSnapshot) []SchemaChange {
	var changes []SchemaChange

	changes = append(changes, diffTables(base, current)...)
	changes = append(changes, diffIndexes(base, current)...)
	changes = append(changes, diffConstraints(base, current)...)

	return changes
}

func diffTables(base, current *pg.SchemaSnapshot) []SchemaChange {
	var changes []SchemaChange

	baseMap := tableMap(base.Tables)
	curMap := tableMap(current.Tables)

	// Find added/modified tables
	for key, curTbl := range curMap {
		baseTbl, exists := baseMap[key]
		if !exists {
			changes = append(changes, SchemaChange{
				Type:       Added,
				ObjectKind: "table",
				ObjectName: key,
				Detail:     fmt.Sprintf("table %s added (%d columns)", key, len(curTbl.Columns)),
			})
			continue
		}
		// Compare columns
		changes = append(changes, diffColumns(key, baseTbl.Columns, curTbl.Columns)...)
	}

	// Find removed tables
	for key := range baseMap {
		if _, exists := curMap[key]; !exists {
			changes = append(changes, SchemaChange{
				Type:       Removed,
				ObjectKind: "table",
				ObjectName: key,
				Detail:     fmt.Sprintf("table %s removed", key),
			})
		}
	}

	sort.Slice(changes, func(i, j int) bool {
		return changes[i].ObjectName < changes[j].ObjectName
	})
	return changes
}

func diffColumns(table string, baseCols, curCols []pg.ColumnInfo) []SchemaChange {
	var changes []SchemaChange

	baseMap := columnMap(baseCols)
	curMap := columnMap(curCols)

	for name, curCol := range curMap {
		baseCol, exists := baseMap[name]
		if !exists {
			changes = append(changes, SchemaChange{
				Type:       Added,
				ObjectKind: "column",
				ObjectName: table + "." + name,
				Detail:     fmt.Sprintf("column %s.%s added (%s)", table, name, curCol.DataType),
			})
			continue
		}
		// Check for type changes
		if baseCol.DataType != curCol.DataType {
			changes = append(changes, SchemaChange{
				Type:       Modified,
				ObjectKind: "column",
				ObjectName: table + "." + name,
				Detail:     fmt.Sprintf("column %s.%s type changed: %s -> %s", table, name, baseCol.DataType, curCol.DataType),
			})
		}
		if baseCol.IsNullable != curCol.IsNullable {
			changes = append(changes, SchemaChange{
				Type:       Modified,
				ObjectKind: "column",
				ObjectName: table + "." + name,
				Detail:     fmt.Sprintf("column %s.%s nullable changed: %v -> %v", table, name, baseCol.IsNullable, curCol.IsNullable),
			})
		}
		if ptrStr(baseCol.Default) != ptrStr(curCol.Default) {
			changes = append(changes, SchemaChange{
				Type:       Modified,
				ObjectKind: "column",
				ObjectName: table + "." + name,
				Detail:     fmt.Sprintf("column %s.%s default changed", table, name),
			})
		}
	}

	for name := range baseMap {
		if _, exists := curMap[name]; !exists {
			changes = append(changes, SchemaChange{
				Type:       Removed,
				ObjectKind: "column",
				ObjectName: table + "." + name,
				Detail:     fmt.Sprintf("column %s.%s removed", table, name),
			})
		}
	}

	return changes
}

func diffIndexes(base, current *pg.SchemaSnapshot) []SchemaChange {
	var changes []SchemaChange

	baseMap := indexMap(base.Indexes)
	curMap := indexMap(current.Indexes)

	for key, curIdx := range curMap {
		baseIdx, exists := baseMap[key]
		if !exists {
			changes = append(changes, SchemaChange{
				Type:       Added,
				ObjectKind: "index",
				ObjectName: key,
				Detail:     fmt.Sprintf("index %s added", key),
			})
			continue
		}
		if baseIdx.Definition != curIdx.Definition {
			changes = append(changes, SchemaChange{
				Type:       Modified,
				ObjectKind: "index",
				ObjectName: key,
				Detail:     fmt.Sprintf("index %s definition changed", key),
			})
		}
	}

	for key := range baseMap {
		if _, exists := curMap[key]; !exists {
			changes = append(changes, SchemaChange{
				Type:       Removed,
				ObjectKind: "index",
				ObjectName: key,
				Detail:     fmt.Sprintf("index %s removed", key),
			})
		}
	}

	return changes
}

func diffConstraints(base, current *pg.SchemaSnapshot) []SchemaChange {
	var changes []SchemaChange

	baseMap := constraintMap(base.Constraints)
	curMap := constraintMap(current.Constraints)

	for key, curCon := range curMap {
		baseCon, exists := baseMap[key]
		if !exists {
			changes = append(changes, SchemaChange{
				Type:       Added,
				ObjectKind: "constraint",
				ObjectName: key,
				Detail:     fmt.Sprintf("constraint %s added (%s)", key, curCon.Type),
			})
			continue
		}
		if baseCon.Definition != curCon.Definition {
			changes = append(changes, SchemaChange{
				Type:       Modified,
				ObjectKind: "constraint",
				ObjectName: key,
				Detail:     fmt.Sprintf("constraint %s definition changed", key),
			})
		}
	}

	for key := range baseMap {
		if _, exists := curMap[key]; !exists {
			changes = append(changes, SchemaChange{
				Type:       Removed,
				ObjectKind: "constraint",
				ObjectName: key,
				Detail:     fmt.Sprintf("constraint %s removed", key),
			})
		}
	}

	return changes
}

// Helper functions

func tableMap(tables []pg.TableInfo) map[string]pg.TableInfo {
	m := make(map[string]pg.TableInfo, len(tables))
	for _, t := range tables {
		m[t.Schema+"."+t.Name] = t
	}
	return m
}

func columnMap(cols []pg.ColumnInfo) map[string]pg.ColumnInfo {
	m := make(map[string]pg.ColumnInfo, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}

func indexMap(indexes []pg.IndexInfo) map[string]pg.IndexInfo {
	m := make(map[string]pg.IndexInfo, len(indexes))
	for _, i := range indexes {
		m[i.Schema+"."+i.Name] = i
	}
	return m
}

func constraintMap(constraints []pg.ConstraintInfo) map[string]pg.ConstraintInfo {
	m := make(map[string]pg.ConstraintInfo, len(constraints))
	for _, c := range constraints {
		m[c.Schema+"."+c.Name] = c
	}
	return m
}

func ptrStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// FormatChanges returns a human-readable summary of schema changes.
func FormatChanges(changes []SchemaChange) string {
	if len(changes) == 0 {
		return "No schema changes."
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Schema changes (%d):\n", len(changes))
	for _, c := range changes {
		symbol := " "
		switch c.Type {
		case Added:
			symbol = "+"
		case Removed:
			symbol = "-"
		case Modified:
			symbol = "~"
		}
		fmt.Fprintf(&b, "  %s %s\n", symbol, c.Detail)
	}
	return b.String()
}
