#!/usr/bin/env bash
set -euo pipefail

# Integration test for pg-branch CLI
# Requires: pg-branch on PATH, local Postgres on port 5432

PG_URL="postgresql://localhost:5432/postgres"
MAIN_DB="pgbr_inttest_main"
BRANCH_A="feature-a"
BRANCH_B="feature-b"
BRANCH_A_DB="pgbr_feature-a"
BRANCH_B_DB="pgbr_feature-b"
PASS=0
FAIL=0
WORKDIR=""

cleanup() {
    psql -h localhost -d postgres -q -c "DROP DATABASE IF EXISTS \"$BRANCH_A_DB\"" 2>/dev/null || true
    psql -h localhost -d postgres -q -c "DROP DATABASE IF EXISTS \"$BRANCH_B_DB\"" 2>/dev/null || true
    psql -h localhost -d postgres -q -c "DROP DATABASE IF EXISTS \"$MAIN_DB\"" 2>/dev/null || true
    if [ -n "$WORKDIR" ] && [ -d "$WORKDIR" ]; then
        rm -rf "$WORKDIR"
    fi
}
trap cleanup EXIT

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected: '$expected', got: '$actual')"
        FAIL=$((FAIL + 1))
    fi
}

assert_contains() {
    local desc="$1" needle="$2" haystack="$3"
    if echo "$haystack" | grep -q "$needle"; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected to contain: '$needle')"
        FAIL=$((FAIL + 1))
    fi
}

assert_not_contains() {
    local desc="$1" needle="$2" haystack="$3"
    if ! echo "$haystack" | grep -q "$needle"; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (should not contain: '$needle')"
        FAIL=$((FAIL + 1))
    fi
}

# --- Setup ---
echo "=== pg-branch integration test ==="
echo ""

cleanup  # ensure clean state
psql -h localhost -d postgres -q -c "CREATE DATABASE \"$MAIN_DB\""
WORKDIR=$(mktemp -d)
cd "$WORKDIR"

# Seed main DB with schema and data
psql -h localhost -d "$MAIN_DB" -q <<SQL
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), total NUMERIC);
INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com');
INSERT INTO orders (user_id, total) VALUES (1, 99.99), (2, 49.99);
SQL

# --- Test: init ---
echo "--- init ---"
OUT=$(pg-branch init --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "init succeeds" "Initialized" "$OUT"
assert_eq "state file created" "true" "$([ -f .pg-branch.state.json ] && echo true || echo false)"

# --- Test: create branch A ---
echo "--- create branch A ---"
OUT=$(pg-branch create "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "create succeeds" "created" "$OUT"
assert_contains "auto-switches" "Switched" "$OUT"

# Verify data cloned
ROW_COUNT=$(psql -h localhost -d "$BRANCH_A_DB" -tAc "SELECT count(*) FROM users")
assert_eq "branch has 2 users" "2" "$ROW_COUNT"

# --- Test: list ---
echo "--- list ---"
OUT=$(pg-branch list --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "list shows branch" "$BRANCH_A" "$OUT"
assert_contains "branch is current" "*" "$OUT"

# --- Test: switch ---
echo "--- switch ---"
OUT=$(pg-branch switch main 2>&1)
assert_contains "switch to main" "main" "$OUT"

OUT=$(pg-branch switch "$BRANCH_A" 2>&1)
assert_contains "switch back to branch" "$BRANCH_A" "$OUT"

# --- Test: make schema changes on branch ---
echo "--- schema changes on branch ---"
psql -h localhost -d "$BRANCH_A_DB" -q <<SQL
ALTER TABLE users ADD COLUMN bio TEXT;
CREATE INDEX idx_users_email ON users(email);
SQL

OUT=$(pg-branch status --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "status shows changes" "ALTER TABLE" "$OUT"

# --- Test: make data changes on branch ---
echo "--- data changes on branch ---"
psql -h localhost -d "$BRANCH_A_DB" -q -c "INSERT INTO users (name, email, bio) VALUES ('Charlie', 'charlie@test.com', 'New user')"

# --- Test: log ---
echo "--- log ---"
OUT=$(pg-branch log --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "log shows ALTER TABLE" "ALTER TABLE" "$OUT"
assert_contains "log shows CREATE INDEX" "CREATE INDEX" "$OUT"

OUT=$(pg-branch log -v --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "verbose log shows SQL" "bio" "$OUT"

# --- Test: diff ---
echo "--- diff ---"
OUT=$(pg-branch diff --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "diff shows added column" "bio" "$OUT"
assert_contains "diff shows data change" "Data changes" "$OUT"

# --- Test: connect ---
echo "--- connect ---"
OUT=$(pg-branch connect --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "connect shows branch URL" "$BRANCH_A_DB" "$OUT"

OUT=$(pg-branch connect main --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "connect main shows main URL" "$MAIN_DB" "$OUT"

# --- Test: merge dry-run ---
echo "--- merge dry-run ---"
OUT=$(pg-branch merge "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "dry-run header" "DRY RUN" "$OUT"
assert_contains "dry-run shows schema op" "bio" "$OUT"

# Verify main unchanged after dry-run
HAS_BIO=$(psql -h localhost -d "$MAIN_DB" -tAc "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='bio')")
assert_eq "main unchanged after dry-run" "f" "$HAS_BIO"

# --- Test: merge apply ---
echo "--- merge apply ---"
OUT=$(pg-branch merge "$BRANCH_A" --apply --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "merge applied" "applied" "$OUT"

HAS_BIO=$(psql -h localhost -d "$MAIN_DB" -tAc "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='bio')")
assert_eq "main has bio column after merge" "t" "$HAS_BIO"

# --- Test: create second branch and conflict ---
echo "--- conflict detection ---"
pg-branch create "$BRANCH_B" --pg-url="postgresql://localhost:5432/$MAIN_DB" >/dev/null 2>&1

# Add same column on branch with different type
psql -h localhost -d "$BRANCH_B_DB" -q -c "ALTER TABLE orders ADD COLUMN status TEXT"
# Add same column on main with different type
psql -h localhost -d "$MAIN_DB" -q -c "ALTER TABLE orders ADD COLUMN status INT"

OUT=$(pg-branch merge "$BRANCH_B" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1 || true)
assert_contains "conflict detected" "CONFLICT" "$OUT"

# --- Test: resolve conflict ---
echo "--- conflict resolution ---"
OUT=$(pg-branch merge "$BRANCH_B" --resolve=main --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "resolve=main skips branch" "main wins" "$OUT"
assert_not_contains "no conflicts after resolve" "CONFLICTS" "$OUT"

# --- Test: delete ---
echo "--- delete ---"
OUT=$(pg-branch delete "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "delete succeeds" "deleted" "$OUT"

DB_EXISTS=$(psql -h localhost -d postgres -tAc "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='$BRANCH_A_DB')")
assert_eq "branch DB dropped" "f" "$DB_EXISTS"

OUT=$(pg-branch delete "$BRANCH_B" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1)
assert_contains "delete B succeeds" "deleted" "$OUT"

# --- Test: error cases ---
echo "--- error handling ---"
OUT=$(pg-branch create "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1 && pg-branch create "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1 || true)
assert_contains "duplicate branch error" "already exists" "$OUT"
pg-branch delete "$BRANCH_A" --pg-url="postgresql://localhost:5432/$MAIN_DB" >/dev/null 2>&1 || true

OUT=$(pg-branch switch nonexistent 2>&1 || true)
assert_contains "switch nonexistent error" "not found" "$OUT"

OUT=$(pg-branch delete nonexistent --pg-url="postgresql://localhost:5432/$MAIN_DB" 2>&1 || true)
assert_contains "delete nonexistent error" "not found" "$OUT"

# --- Summary ---
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
