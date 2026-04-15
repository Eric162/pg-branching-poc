package diff

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-branch/pg-branch/internal/pg"
)

// TableChecksum represents the checksum of a table's data.
type TableChecksum struct {
	Schema   string
	Table    string
	Checksum string
	RowCount int64
}

// DataChange describes a data difference for one table.
type DataChange struct {
	Schema    string
	Table     string
	BaseRows  int64
	CurrRows  int64
	HasChange bool // checksum differs
}

// ComputeTableChecksums returns a checksum and row count for each user table.
func ComputeTableChecksums(ctx context.Context, conn *pg.Conn) ([]TableChecksum, error) {
	tables, err := conn.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	var checksums []TableChecksum
	for _, t := range tables {
		cs, err := computeSingleChecksum(ctx, conn, t.Schema, t.Name)
		if err != nil {
			return nil, fmt.Errorf("checksum %s.%s: %w", t.Schema, t.Name, err)
		}
		checksums = append(checksums, *cs)
	}
	return checksums, nil
}

func computeSingleChecksum(ctx context.Context, conn *pg.Conn, schema, table string) (*TableChecksum, error) {
	fqn := fmt.Sprintf("%s.%s",
		pgQuoteIdent(schema),
		pgQuoteIdent(table),
	)

	// Get row count
	var rowCount int64
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", fqn)).Scan(&rowCount)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Compute MD5 of all rows cast to text, ordered by ctid as fallback ordering.
	// For tables with a PK we'd ideally ORDER BY PK, but ctid works for checksums.
	var checksum *string
	err = conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT md5(COALESCE(string_agg(t::text, ''), '')) FROM (SELECT * FROM %s ORDER BY ctid) t",
		fqn,
	)).Scan(&checksum)
	if err != nil {
		return nil, fmt.Errorf("compute checksum: %w", err)
	}

	cs := ""
	if checksum != nil {
		cs = *checksum
	}

	return &TableChecksum{
		Schema:   schema,
		Table:    table,
		Checksum: cs,
		RowCount: rowCount,
	}, nil
}

// CompareData compares table checksums between two databases.
func CompareData(base, current []TableChecksum) []DataChange {
	baseMap := checksumMap(base)
	curMap := checksumMap(current)

	var changes []DataChange

	// Tables in current
	for key, cur := range curMap {
		b, exists := baseMap[key]
		if !exists {
			// New table (schema diff handles this)
			changes = append(changes, DataChange{
				Schema:    cur.Schema,
				Table:     cur.Table,
				CurrRows:  cur.RowCount,
				HasChange: cur.RowCount > 0,
			})
			continue
		}
		changes = append(changes, DataChange{
			Schema:    cur.Schema,
			Table:     cur.Table,
			BaseRows:  b.RowCount,
			CurrRows:  cur.RowCount,
			HasChange: b.Checksum != cur.Checksum,
		})
	}

	// Tables removed
	for key, b := range baseMap {
		if _, exists := curMap[key]; !exists {
			changes = append(changes, DataChange{
				Schema:    b.Schema,
				Table:     b.Table,
				BaseRows:  b.RowCount,
				HasChange: true,
			})
		}
	}

	return changes
}

// FormatDataChanges returns human-readable data change summary.
func FormatDataChanges(changes []DataChange) string {
	changed := 0
	for _, c := range changes {
		if c.HasChange {
			changed++
		}
	}

	if changed == 0 {
		return "No data changes."
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Data changes (%d tables modified):\n", changed)
	for _, c := range changes {
		if !c.HasChange {
			continue
		}
		rowDiff := c.CurrRows - c.BaseRows
		sign := "+"
		if rowDiff < 0 {
			sign = ""
		}
		fmt.Fprintf(&b, "  ~ %s.%s (%d -> %d rows, %s%d)\n",
			c.Schema, c.Table, c.BaseRows, c.CurrRows, sign, rowDiff)
	}
	return b.String()
}

func checksumMap(checksums []TableChecksum) map[string]TableChecksum {
	m := make(map[string]TableChecksum, len(checksums))
	for _, cs := range checksums {
		m[cs.Schema+"."+cs.Table] = cs
	}
	return m
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
