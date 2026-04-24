package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// AdvisoryLock represents a held session-level advisory lock. The lock is
// pinned to a single pooled connection; releasing it returns the connection
// to the pool. pg_advisory_lock is per-session, so acquiring and releasing
// via arbitrary pool checkouts would be unsafe — callers must keep the
// handle alive for the duration of the protected operation.
type AdvisoryLock struct {
	conn    *pgxpool.Conn
	key     string
	held    bool
	release func()
}

// TryAdvisoryLock attempts to acquire a session-level advisory lock keyed by
// the given string. Returns a held AdvisoryLock on success. If another
// session already holds the lock, returns (nil, nil) with no error — callers
// decide whether that's fatal or a soft miss.
//
// hashtextextended is 64-bit (PG 11+) and its output fits pg_try_advisory_lock(bigint).
func (c *Conn) TryAdvisoryLock(ctx context.Context, key string) (*AdvisoryLock, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for advisory lock: %w", err)
	}

	var got bool
	err = conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key,
	).Scan(&got)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("try advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, nil
	}

	return &AdvisoryLock{
		conn:    conn,
		key:     key,
		held:    true,
		release: conn.Release,
	}, nil
}

// Release drops the advisory lock and returns the pinned connection to the
// pool. Safe to call multiple times; a nil receiver is a no-op so callers
// can `defer lock.Release(ctx)` without a nil check.
func (l *AdvisoryLock) Release(ctx context.Context) {
	if l == nil || !l.held {
		return
	}
	l.held = false
	// Best-effort: if unlock fails, the session-end cleanup on connection
	// close will still release the lock. We swallow the error rather than
	// pretend the caller can do something useful with it.
	_, _ = l.conn.Exec(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", l.key)
	l.release()
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
