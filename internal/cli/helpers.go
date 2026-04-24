package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
)

func loadStateFromCwd() (*config.State, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return config.LoadState(cwd)
}

// adminDBName returns the maintenance database to connect to for
// CREATE/DROP DATABASE. Defaults to "postgres" — the conventional
// PostgreSQL maintenance DB — but honours PG_BRANCH_ADMIN_DB so users on
// servers that don't expose "postgres" (some managed providers, or setups
// where the app role has no access to it) can point pg-branch at a DB
// their role can actually connect to. Example: PG_BRANCH_ADMIN_DB=defaultdb.
func adminDBName() string {
	if name := os.Getenv("PG_BRANCH_ADMIN_DB"); name != "" {
		return name
	}
	return "postgres"
}

// connectAdmin connects to the maintenance database on the same server as
// the given URL. The DB name comes from adminDBName().
func connectAdmin(ctx context.Context, pgURL string) (*pg.Conn, error) {
	adminURL, err := pg.URLForDatabase(pgURL, adminDBName())
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
