package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
)

func envOrEmpty(key string) string {
	return os.Getenv(key)
}

func loadStateFromCwd() (*config.State, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return config.LoadState(cwd)
}

// connectAdmin connects to the 'postgres' maintenance database on the same server.
func connectAdmin(ctx context.Context, pgURL string) (*pg.Conn, error) {
	conn, err := pg.Connect(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	adminURL, err := conn.URLForDatabase("postgres")
	conn.Close()
	if err != nil {
		return nil, err
	}
	return pg.Connect(ctx, adminURL)
}

// stderrProgress returns a ProgressFunc that prints a carriage-return progress line to stderr.
func stderrProgress(label string) func(current, total int, detail string) {
	return func(current, total int, detail string) {
		fmt.Fprintf(os.Stderr, "\r\033[2K    [%d/%d] %s", current, total, detail)
		if current == total {
			// Move to next line so phase labels don't overwrite
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
}

// mustResolveURL resolves the URL or exits with an error.
func mustResolveURL() string {
	u := resolveURL()
	if u == "" {
		fmt.Fprintln(os.Stderr, "error: no PostgreSQL URL. Use --pg-url, PG_BRANCH_URL env, or run 'pg-branch init' first.")
		os.Exit(1)
	}
	return u
}
