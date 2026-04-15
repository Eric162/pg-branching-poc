package pg

import (
	"context"
	"encoding/json"
	"fmt"
)

// TableInfo describes a table in the database.
type TableInfo struct {
	Schema  string       `json:"schema"`
	Name    string       `json:"name"`
	Columns []ColumnInfo `json:"columns"`
}

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name         string  `json:"name"`
	DataType     string  `json:"data_type"`
	IsNullable   bool    `json:"is_nullable"`
	Default      *string `json:"column_default,omitempty"`
	OrdinalPos   int     `json:"ordinal_position"`
	CharMaxLen   *int    `json:"character_maximum_length,omitempty"`
	NumPrecision *int    `json:"numeric_precision,omitempty"`
}

// IndexInfo describes an index.
type IndexInfo struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Name       string `json:"name"`
	Definition string `json:"definition"`
	IsUnique   bool   `json:"is_unique"`
	IsPrimary  bool   `json:"is_primary"`
}

// ConstraintInfo describes a table constraint.
type ConstraintInfo struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Name       string `json:"name"`
	Type       string `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	Definition string `json:"definition"`
}

// SchemaSnapshot is a complete point-in-time capture of the database schema.
type SchemaSnapshot struct {
	Tables      []TableInfo      `json:"tables"`
	Indexes     []IndexInfo      `json:"indexes"`
	Constraints []ConstraintInfo `json:"constraints"`
}

// TakeSchemaSnapshot captures the current schema state of user tables.
func (c *Conn) TakeSchemaSnapshot(ctx context.Context) (*SchemaSnapshot, error) {
	tables, err := c.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	indexes, err := c.ListIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	constraints, err := c.ListConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("list constraints: %w", err)
	}
	return &SchemaSnapshot{
		Tables:      tables,
		Indexes:     indexes,
		Constraints: constraints,
	}, nil
}

// ToJSON serializes the snapshot to JSON.
func (s *SchemaSnapshot) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// SchemaSnapshotFromJSON deserializes a snapshot from JSON.
func SchemaSnapshotFromJSON(data []byte) (*SchemaSnapshot, error) {
	var s SchemaSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ListTables returns all user tables with column info.
// Excludes _pgbranch schema and system schemas.
func (c *Conn) ListTables(ctx context.Context) ([]TableInfo, error) {
	rows, err := c.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', '_pgbranch')
		  AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		cols, err := c.listColumns(ctx, t.Schema, t.Name)
		if err != nil {
			return nil, fmt.Errorf("columns for %s.%s: %w", t.Schema, t.Name, err)
		}
		t.Columns = cols
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (c *Conn) listColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	rows, err := c.Query(ctx, `
		SELECT column_name, data_type, is_nullable, column_default,
		       ordinal_position, character_maximum_length, numeric_precision
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.DataType, &nullable,
			&col.Default, &col.OrdinalPos, &col.CharMaxLen, &col.NumPrecision); err != nil {
			return nil, err
		}
		col.IsNullable = nullable == "YES"
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// ListIndexes returns all user indexes.
func (c *Conn) ListIndexes(ctx context.Context) ([]IndexInfo, error) {
	rows, err := c.Query(ctx, `
		SELECT schemaname, tablename, indexname, indexdef,
		       (SELECT indisunique FROM pg_index WHERE indexrelid = (schemaname || '.' || indexname)::regclass),
		       (SELECT indisprimary FROM pg_index WHERE indexrelid = (schemaname || '.' || indexname)::regclass)
		FROM pg_indexes
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema', '_pgbranch')
		ORDER BY schemaname, tablename, indexname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		if err := rows.Scan(&idx.Schema, &idx.Table, &idx.Name, &idx.Definition,
			&idx.IsUnique, &idx.IsPrimary); err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// ListConstraints returns all user table constraints.
func (c *Conn) ListConstraints(ctx context.Context) ([]ConstraintInfo, error) {
	rows, err := c.Query(ctx, `
		SELECT tc.table_schema, tc.table_name, tc.constraint_name,
		       tc.constraint_type,
		       pg_get_constraintdef(pgc.oid) AS definition
		FROM information_schema.table_constraints tc
		JOIN pg_constraint pgc ON pgc.conname = tc.constraint_name
		JOIN pg_namespace nsp ON nsp.nspname = tc.constraint_schema AND nsp.oid = pgc.connamespace
		WHERE tc.table_schema NOT IN ('pg_catalog', 'information_schema', '_pgbranch')
		ORDER BY tc.table_schema, tc.table_name, tc.constraint_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []ConstraintInfo
	for rows.Next() {
		var c ConstraintInfo
		if err := rows.Scan(&c.Schema, &c.Table, &c.Name, &c.Type, &c.Definition); err != nil {
			return nil, err
		}
		constraints = append(constraints, c)
	}
	return constraints, rows.Err()
}
