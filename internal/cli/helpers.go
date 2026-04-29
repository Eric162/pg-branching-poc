package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
)

// loadStateFromCwd is preserved for tests and a few callers that
// explicitly want the legacy CWD location. Most code should call
// loadActiveState instead so it picks up --state-file / env / central.
func loadStateFromCwd() (*config.State, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return config.LoadState(cwd)
}

// resolveStateFile returns the path of the state file the CLI should
// read for the current invocation. Resolution order:
//  1. --state-file flag
//  2. PG_BRANCH_STATE_FILE env var
//  3. CWD .pg-branch.state.json (legacy per-project state, kept for
//     backward compatibility — pre-existing setups Just Work)
//  4. Central pointer: $XDG_STATE_HOME/pg-branch/current → that DB's
//     state file under the same directory
//
// Returns ("", nil) when no source resolves — callers (init, use)
// supply their own fallback so they can write a fresh state file.
func resolveStateFile() (string, error) {
	if stateFileFlag != "" {
		return stateFileFlag, nil
	}
	if env := os.Getenv("PG_BRANCH_STATE_FILE"); env != "" {
		return env, nil
	}
	cwd, err := os.Getwd()
	if err == nil {
		legacy := filepath.Join(cwd, config.StateFileName)
		if _, statErr := os.Stat(legacy); statErr == nil {
			return legacy, nil
		}
	}
	current, err := config.ReadCurrent()
	if err != nil {
		return "", err
	}
	if current == "" {
		return "", nil
	}
	return config.CentralStateFile(current)
}

// loadActiveState resolves the active state file and loads it. Returns
// a friendly error when no source resolves so users know exactly how to
// recover (run init, set env, pass --state-file).
func loadActiveState() (*config.State, error) {
	path, err := resolveStateFile()
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("no pg-branch state found. Run 'pg-branch init --pg-url=...' to create one, or pass --state-file / set PG_BRANCH_STATE_FILE")
	}
	return config.LoadStateFromFile(path)
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
