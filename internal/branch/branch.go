package branch

import (
	"context"
	"fmt"
	"time"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/pg-branch/pg-branch/internal/pg"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

// BranchInfo combines DB-stored metadata with state file info.
type BranchInfo struct {
	Name      string
	DBName    string
	ParentDB  string
	CreatedAt time.Time
	IsCurrent bool
}

// Create creates a new branch database from a parent using TEMPLATE.
// 1. Terminates connections to parent
// 2. CREATE DATABASE ... TEMPLATE
// 3. Installs DDL trigger on new DB
// 4. Takes schema snapshot and stores it
// 5. Records metadata in parent's _pgbranch.branches
func Create(ctx context.Context, adminConn *pg.Conn, name, parentDB, branchDBName string) error {
	// Terminate connections to parent so TEMPLATE works
	if err := adminConn.TerminateConnections(ctx, parentDB); err != nil {
		return fmt.Errorf("terminate connections to %s: %w", parentDB, err)
	}

	// Create branch database from template
	if err := adminConn.CreateDatabaseFromTemplate(ctx, branchDBName, parentDB); err != nil {
		return fmt.Errorf("create branch database: %w", err)
	}

	// Connect to the new branch DB
	branchConn, err := adminConn.ConnectToDatabase(ctx, branchDBName)
	if err != nil {
		return fmt.Errorf("connect to branch db: %w", err)
	}
	defer branchConn.Close()

	// Install DDL tracking trigger
	if err := tracker.InstallDDLTrigger(ctx, branchConn); err != nil {
		return fmt.Errorf("install DDL trigger: %w", err)
	}

	// Take schema snapshot at branch point
	snapshot, err := branchConn.TakeSchemaSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("take schema snapshot: %w", err)
	}
	snapshotJSON, err := snapshot.ToJSON()
	if err != nil {
		return fmt.Errorf("serialize snapshot: %w", err)
	}

	// Connect to parent to store metadata
	parentConn, err := adminConn.ConnectToDatabase(ctx, parentDB)
	if err != nil {
		return fmt.Errorf("connect to parent db: %w", err)
	}
	defer parentConn.Close()

	// Record branch metadata
	err = parentConn.Exec(ctx,
		`INSERT INTO _pgbranch.branches (name, db_name, parent_db, snapshot_data)
		 VALUES ($1, $2, $3, $4)`,
		name, branchDBName, parentDB, snapshotJSON,
	)
	if err != nil {
		return fmt.Errorf("record branch metadata: %w", err)
	}

	return nil
}

// Delete removes a branch database and its metadata.
func Delete(ctx context.Context, adminConn *pg.Conn, name, parentDB, branchDBName string) error {
	// Drop the branch database
	if err := adminConn.DropDatabase(ctx, branchDBName); err != nil {
		return fmt.Errorf("drop branch database %s: %w", branchDBName, err)
	}

	// Remove metadata from parent
	parentConn, err := adminConn.ConnectToDatabase(ctx, parentDB)
	if err != nil {
		return fmt.Errorf("connect to parent db: %w", err)
	}
	defer parentConn.Close()

	err = parentConn.Exec(ctx,
		"DELETE FROM _pgbranch.branches WHERE name = $1", name)
	if err != nil {
		return fmt.Errorf("remove branch metadata: %w", err)
	}

	return nil
}

// List returns all branches recorded in the parent database.
func List(ctx context.Context, parentConn *pg.Conn, state *config.State) ([]BranchInfo, error) {
	rows, err := parentConn.Query(ctx,
		`SELECT name, db_name, parent_db, created_at
		 FROM _pgbranch.branches ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}
	defer rows.Close()

	var branches []BranchInfo
	for rows.Next() {
		var b BranchInfo
		if err := rows.Scan(&b.Name, &b.DBName, &b.ParentDB, &b.CreatedAt); err != nil {
			return nil, err
		}
		b.IsCurrent = b.Name == state.CurrentBranch
		branches = append(branches, b)
	}
	return branches, rows.Err()
}
