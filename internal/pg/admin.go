package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateDatabase creates a new empty database.
func (c *Conn) CreateDatabase(ctx context.Context, dbName string) error {
	sql := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
	return c.Exec(ctx, sql)
}

// CreateDatabaseFromTemplate creates a new database using the source as a template.
// Requires no active connections to the source database (other than this one, which
// connects via the 'postgres' maintenance DB).
func (c *Conn) CreateDatabaseFromTemplate(ctx context.Context, newDB, templateDB string) error {
	// Must use the postgres/maintenance DB for CREATE DATABASE.
	// SQL identifiers can't be parameterized, so we quote them.
	sql := fmt.Sprintf(
		"CREATE DATABASE %s TEMPLATE %s",
		pgx.Identifier{newDB}.Sanitize(),
		pgx.Identifier{templateDB}.Sanitize(),
	)
	return c.Exec(ctx, sql)
}

// DropDatabase drops a database. Terminates active connections first.
func (c *Conn) DropDatabase(ctx context.Context, dbName string) error {
	if err := c.TerminateConnections(ctx, dbName); err != nil {
		return fmt.Errorf("terminate connections before drop: %w", err)
	}
	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", pgx.Identifier{dbName}.Sanitize())
	return c.Exec(ctx, sql)
}

// TerminateConnections terminates all connections to the given database
// except the current one.
func (c *Conn) TerminateConnections(ctx context.Context, dbName string) error {
	sql := `SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid != pg_backend_pid()`
	_, err := c.pool.Exec(ctx, sql, dbName)
	return err
}

// DatabaseExists checks if a database exists.
func (c *Conn) DatabaseExists(ctx context.Context, dbName string) (bool, error) {
	var exists bool
	err := c.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
		dbName,
	).Scan(&exists)
	return exists, err
}

// ListDatabases returns all non-template database names.
func (c *Conn) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.Query(ctx,
		"SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
