# pg-branch (proof of concept)

Local PostgreSQL database branching. Create isolated database copies, track schema and data changes, merge them back.

Think `git branch` for your local Postgres.

> **Note:** This is a proof of concept and is not actively maintained. Feel free to fork it. — [@Eric162](https://github.com/Eric162)

## Install

```bash
# From source
make build        # produces ./pg-branch
make install      # installs to $GOPATH/bin

# Or directly
go install ./cmd/pg-branch
```

Requires Go 1.22+ and a running PostgreSQL instance.

## Quick Start

```bash
# Point pg-branch at your dev database
pg-branch init --pg-url="postgresql://localhost:5432/myapp_dev"

# Create a branch — clones the DB via TEMPLATE
pg-branch create feature-auth

# Work on the branch (your app connects to pgbr_feature-auth)
pg-branch connect
# postgresql://localhost:5432/pgbr_feature-auth

# See what changed
pg-branch status     # DDL change summary
pg-branch diff       # full schema + data diff
pg-branch log -v     # DDL history with SQL

# Merge back into main
pg-branch merge feature-auth            # dry-run (default)
pg-branch merge feature-auth --apply    # apply changes

# Clean up
pg-branch delete feature-auth
```

## Commands

| Command | Description |
|---------|-------------|
| `init --pg-url=URL` | Install tracking schema on a database |
| `create <name> [--from=DB]` | Create a branch database via TEMPLATE |
| `list` | List all branches |
| `switch <name>` | Set current branch |
| `status` | Show current branch and pending DDL changes |
| `diff [branch]` | Schema and data diff vs parent |
| `log [-v] [branch]` | DDL change history |
| `merge <branch> [--apply] [--resolve=branch\|main]` | Three-way merge into parent |
| `connect [name]` | Print connection URL |
| `delete <name>` | Drop branch database |
| `version` | Print version |

## How It Works

### Branching

`pg-branch create` uses `CREATE DATABASE ... TEMPLATE` to clone the parent database. This is a server-side file copy — fast for dev-sized databases (sub-second for < 1 GB).

After cloning, pg-branch:
1. Installs a DDL event trigger on the branch to capture all schema changes
2. Takes a schema snapshot at the branch point (stored as JSON in the parent)

### Change Tracking

**Schema changes** are captured automatically by a PostgreSQL event trigger (`ddl_command_end`). Every `CREATE TABLE`, `ALTER TABLE`, `DROP INDEX`, etc. is logged with the full SQL command.

**Data changes** are detected via table checksums — MD5 of all rows, compared between branch and parent.

### Merging

Merge uses a three-way algorithm:

```
branch-point snapshot
       |
   +---+---+
   |       |
 main    branch
(current) (current)
   |       |
   +---+---+
       |
   merged result
```

1. Compare branch schema vs branch-point snapshot → branch changes
2. Compare main schema vs branch-point snapshot → main changes
3. For each branch change:
   - If main didn't touch the same object → safe to apply (replay DDL)
   - If both sides made the same change → skip (already converged)
   - If both sides made different changes → **conflict**

Merge is **dry-run by default**. Pass `--apply` to execute. All changes run in a transaction — rollback on error.

### Conflict Resolution

```bash
# See conflicts
pg-branch merge feature-auth

# Branch wins
pg-branch merge feature-auth --apply --resolve=branch

# Main wins (skip branch changes that conflict)
pg-branch merge feature-auth --apply --resolve=main
```

## Configuration

pg-branch stores state in `.pg-branch.state.json` in your working directory:

```json
{
  "current_branch": "feature-auth",
  "main_db": "myapp_dev",
  "branches": {
    "feature-auth": {
      "db_name": "pgbr_feature-auth",
      "parent_db": "myapp_dev",
      "created_at": "2026-04-15T10:00:00Z"
    }
  }
}
```

Connection URL resolution order:
1. `--pg-url` flag
2. `PG_BRANCH_URL` environment variable
3. `.pg-branch.state.json` in current directory

## Limitations

- `CREATE DATABASE ... TEMPLATE` requires no active connections to the source database. pg-branch terminates other connections automatically, but this can interrupt running queries.
- DDL event triggers don't capture changes made via `pg_restore` or direct file manipulation.
- Data merge detects which tables changed (via checksums) but doesn't yet do row-level merge — it reports changes for manual review.
- Branch databases live on the same Postgres server. No remote or cross-server branching.

## Development

```bash
make build     # build binary
make test      # run tests (requires local Postgres on port 5432)
make test-v    # verbose test output
make clean     # remove binary
```

Tests run against a local PostgreSQL instance. Set `PG_BRANCH_TEST_URL` to override the default connection (`postgresql://localhost:5432/postgres`).

## License

MIT
