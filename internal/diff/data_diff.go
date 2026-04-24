package diff

import (
	"context"
	"crypto/md5"
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/pg-branch/pg-branch/internal/pg"
)

// largeTableThreshold is the row count above which per-row progress is reported.
// Exposed as a var (not const) so tests can force the streaming path on small tables.
var largeTableThreshold int64 = 50000

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

// ProgressFunc is called during long operations with current/total progress and a description.
type ProgressFunc func(current, total int, detail string)

// ComputeTableChecksums returns a checksum and row count for each user table.
// Pass nil for progress if no reporting is needed.
func ComputeTableChecksums(ctx context.Context, conn *pg.Conn, progress ProgressFunc) ([]TableChecksum, error) {
	tables, err := conn.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	total := len(tables)
	var checksums []TableChecksum
	for i, t := range tables {
		if progress != nil {
			progress(i+1, total, fmt.Sprintf("%s.%s", t.Schema, t.Name))
		}
		cs, err := computeSingleChecksum(ctx, conn, t.Schema, t.Name, i+1, total, progress)
		if err != nil {
			return nil, fmt.Errorf("checksum %s.%s: %w", t.Schema, t.Name, err)
		}
		checksums = append(checksums, *cs)
	}
	return checksums, nil
}

func computeSingleChecksum(ctx context.Context, conn *pg.Conn, schema, table string, tableIdx, tableTotal int, progress ProgressFunc) (*TableChecksum, error) {
	fqn := pgx.Identifier{schema, table}.Sanitize()

	// Get row count
	var rowCount int64
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", fqn)).Scan(&rowCount)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	cs, err := computeChecksum(ctx, conn, fqn, schema, table, rowCount, tableIdx, tableTotal, progress)
	if err != nil {
		return nil, err
	}

	return &TableChecksum{
		Schema:   schema,
		Table:    table,
		Checksum: cs,
		RowCount: rowCount,
	}, nil
}

// computeChecksum streams per-row 64-bit hashes and combines them with an
// arbitrary-precision sum, then hashes the resulting decimal string. The sum
// is done in Go with math/big so the result matches what Postgres' SUM(bigint)
// (which returns numeric) would compute for the same rows — this keeps the
// checksum stable regardless of table size, and a divergence between "small"
// and "large" tables can't creep back in.
//
// Progress is reported per row for tables above largeTableThreshold; smaller
// tables only report once at completion.
func computeChecksum(ctx context.Context, conn *pg.Conn, fqn, schema, table string, rowCount int64, tableIdx, tableTotal int, progress ProgressFunc) (string, error) {
	if rowCount == 0 {
		// Empty table — stable sentinel matches md5("0") so that an empty table
		// always hashes to the same string regardless of which path we took.
		return fmt.Sprintf("%x", md5.Sum([]byte("0"))), nil
	}

	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT ('x' || substr(md5(t::text), 1, 16))::bit(64)::bigint FROM %s t", fqn,
	))
	if err != nil {
		return "", fmt.Errorf("compute checksum: %w", err)
	}
	defer rows.Close()

	sum := new(big.Int)
	tmp := new(big.Int)
	var processed int64

	reportLarge := rowCount >= largeTableThreshold && progress != nil
	reportInterval := rowCount / 100
	if reportInterval < 1000 {
		reportInterval = 1000
	}

	for rows.Next() {
		var hashVal int64
		if err := rows.Scan(&hashVal); err != nil {
			return "", fmt.Errorf("scan row hash: %w", err)
		}
		tmp.SetInt64(hashVal)
		sum.Add(sum, tmp)
		processed++
		if reportLarge && processed%reportInterval == 0 {
			progress(tableIdx, tableTotal, fmt.Sprintf("%s.%s (%s/%s rows)",
				schema, table, formatCount(processed), formatCount(rowCount)))
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate rows: %w", err)
	}

	return fmt.Sprintf("%x", md5.Sum([]byte(sum.String()))), nil
}

// formatCount formats a number with comma separators.
func formatCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	offset := len(s) % 3
	if offset > 0 {
		b.WriteString(s[:offset])
	}
	for i := offset; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
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
