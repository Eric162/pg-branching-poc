package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/pg-branch/pg-branch/internal/pg"
)

// DDLEntry represents a captured DDL change.
type DDLEntry struct {
	ID             int       `json:"id"`
	EventTime      time.Time `json:"event_time"`
	CommandTag     string    `json:"command_tag"`
	ObjectType     string    `json:"object_type"`
	SchemaName     string    `json:"schema_name"`
	ObjectIdentity string    `json:"object_identity"`
	Command        string    `json:"command"`
}

// InstallTrackingSchema creates the _pgbranch schema and tables on the given database.
// This is called during `pg-branch init` on the main database.
func InstallTrackingSchema(ctx context.Context, conn *pg.Conn) error {
	sql := `
		CREATE SCHEMA IF NOT EXISTS _pgbranch;

		CREATE TABLE IF NOT EXISTS _pgbranch.branches (
			name TEXT PRIMARY KEY,
			db_name TEXT NOT NULL,
			parent_db TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT now(),
			snapshot_data JSONB
		);
	`
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("install tracking schema: %w", err)
	}
	return nil
}

// InstallDDLTrigger sets up DDL change tracking on a branch database.
// Called after creating a branch DB from template.
func InstallDDLTrigger(ctx context.Context, conn *pg.Conn) error {
	sql := `
		CREATE SCHEMA IF NOT EXISTS _pgbranch;

		CREATE TABLE IF NOT EXISTS _pgbranch.ddl_log (
			id SERIAL PRIMARY KEY,
			event_time TIMESTAMPTZ DEFAULT now(),
			command_tag TEXT,
			object_type TEXT,
			schema_name TEXT,
			object_identity TEXT,
			command TEXT
		);

		CREATE OR REPLACE FUNCTION _pgbranch.log_ddl()
		RETURNS event_trigger AS $$
		DECLARE
			obj record;
		BEGIN
			FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
			LOOP
				INSERT INTO _pgbranch.ddl_log(command_tag, object_type, schema_name, object_identity, command)
				VALUES (obj.command_tag, obj.object_type, obj.schema_name, obj.object_identity, current_query());
			END LOOP;
		END;
		$$ LANGUAGE plpgsql;

		DROP EVENT TRIGGER IF EXISTS pgbranch_ddl_tracker;
		CREATE EVENT TRIGGER pgbranch_ddl_tracker ON ddl_command_end
			EXECUTE FUNCTION _pgbranch.log_ddl();
	`
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("install DDL trigger: %w", err)
	}
	return nil
}

// ReadDDLLog returns all DDL entries captured on a branch database.
func ReadDDLLog(ctx context.Context, conn *pg.Conn) ([]DDLEntry, error) {
	rows, err := conn.Query(ctx, `
		SELECT id, event_time, command_tag, object_type,
		       COALESCE(schema_name, ''), COALESCE(object_identity, ''), COALESCE(command, '')
		FROM _pgbranch.ddl_log
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("read DDL log: %w", err)
	}
	defer rows.Close()

	var entries []DDLEntry
	for rows.Next() {
		var e DDLEntry
		if err := rows.Scan(&e.ID, &e.EventTime, &e.CommandTag, &e.ObjectType,
			&e.SchemaName, &e.ObjectIdentity, &e.Command); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SaveSnapshot stores a schema snapshot for a branch in the main DB's tracking table.
func SaveSnapshot(ctx context.Context, conn *pg.Conn, branchName string, snapshotJSON []byte) error {
	sql := `UPDATE _pgbranch.branches SET snapshot_data = $1 WHERE name = $2`
	return conn.Exec(ctx, sql, snapshotJSON, branchName)
}

// LoadSnapshot retrieves the schema snapshot for a branch.
func LoadSnapshot(ctx context.Context, conn *pg.Conn, branchName string) ([]byte, error) {
	var data []byte
	err := conn.QueryRow(ctx,
		"SELECT snapshot_data FROM _pgbranch.branches WHERE name = $1",
		branchName,
	).Scan(&data)
	if err != nil {
		return nil, fmt.Errorf("load snapshot for %s: %w", branchName, err)
	}
	return data, nil
}

// HasTrackingSchema checks if _pgbranch schema exists on a database.
func HasTrackingSchema(ctx context.Context, conn *pg.Conn) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '_pgbranch')",
	).Scan(&exists)
	return exists, err
}
