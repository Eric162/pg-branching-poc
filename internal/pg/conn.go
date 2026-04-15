package pg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Conn wraps a pgxpool.Pool with convenience methods for pg-branch operations.
type Conn struct {
	pool *pgxpool.Pool
	url  string
}

// Connect creates a new connection pool to the given Postgres URL.
func Connect(ctx context.Context, pgURL string) (*Conn, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Conn{pool: pool, url: pgURL}, nil
}

// Close shuts down the connection pool.
func (c *Conn) Close() {
	c.pool.Close()
}

// Pool returns the underlying pgxpool.Pool.
func (c *Conn) Pool() *pgxpool.Pool {
	return c.pool
}

// URL returns the connection URL.
func (c *Conn) URL() string {
	return c.url
}

// Query executes a query and returns rows.
func (c *Conn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

// QueryRow executes a query that returns at most one row.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.pool.QueryRow(ctx, sql, args...)
}

// Exec executes a query that doesn't return rows.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := c.pool.Exec(ctx, sql, args...)
	return err
}

// URLForDatabase returns a connection URL for a different database on the same server.
func (c *Conn) URLForDatabase(dbName string) (string, error) {
	u, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("parse connection URL: %w", err)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

// ConnectToDatabase opens a new connection to a different database on the same server.
func (c *Conn) ConnectToDatabase(ctx context.Context, dbName string) (*Conn, error) {
	dbURL, err := c.URLForDatabase(dbName)
	if err != nil {
		return nil, err
	}
	return Connect(ctx, dbURL)
}

// DatabaseName extracts the database name from the connection URL.
func (c *Conn) DatabaseName() (string, error) {
	u, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("parse connection URL: %w", err)
	}
	name := u.Path
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	return name, nil
}
