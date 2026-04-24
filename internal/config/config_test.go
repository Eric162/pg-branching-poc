package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pg-branch/pg-branch/internal/config"
)

func TestStateRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create state
	state := &config.State{
		MainDB:        "myapp_dev",
		CurrentBranch: "feature-x",
		Branches:      make(map[string]config.BranchState),
	}
	state.SetPath(dir)
	state.AddBranch("feature-x", config.BranchState{
		DBName:    "pgbr_feature_x",
		ParentDB:  "myapp_dev",
		CreatedAt: "2026-04-15T10:00:00Z",
	})

	// Save
	if err := state.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify file exists
	path := filepath.Join(dir, config.StateFileName)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not found: %v", err)
	}

	// Load back
	loaded, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded.MainDB != "myapp_dev" {
		t.Errorf("main_db: got %q, want %q", loaded.MainDB, "myapp_dev")
	}
	if loaded.CurrentBranch != "feature-x" {
		t.Errorf("current_branch: got %q, want %q", loaded.CurrentBranch, "feature-x")
	}

	bs, ok := loaded.GetBranch("feature-x")
	if !ok {
		t.Fatal("branch 'feature-x' not found")
	}
	if bs.DBName != "pgbr_feature_x" {
		t.Errorf("db_name: got %q, want %q", bs.DBName, "pgbr_feature_x")
	}
}

func TestLoadStateMissingFile(t *testing.T) {
	dir := t.TempDir()
	state, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if state.MainDB != "" {
		t.Error("expected empty main_db for missing state file")
	}
	if len(state.Branches) != 0 {
		t.Error("expected empty branches for missing state file")
	}
}

// TestLoadStateThenOverwriteMainDB models the init-reinit flow: loading an
// existing state file and updating only MainDB must preserve existing branches
// and the current_branch marker. Init previously replaced the file with a
// fresh empty State, silently dropping branch entries.
func TestLoadStateThenOverwriteMainDB(t *testing.T) {
	dir := t.TempDir()

	// Seed state with a branch and a current branch marker.
	seed := &config.State{
		MainDB:        "myapp_dev",
		CurrentBranch: "feature-x",
		Branches:      make(map[string]config.BranchState),
	}
	seed.SetPath(dir)
	seed.AddBranch("feature-x", config.BranchState{
		DBName:    "pgbr_feature_x",
		ParentDB:  "myapp_dev",
		CreatedAt: "2026-04-15T10:00:00Z",
	})
	if err := seed.Save(); err != nil {
		t.Fatalf("save seed: %v", err)
	}

	// Simulate re-running `init` against the same main DB.
	loaded, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	loaded.MainDB = "myapp_dev"
	loaded.SetPath(dir)
	if err := loaded.Save(); err != nil {
		t.Fatalf("resave state: %v", err)
	}

	reloaded, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if _, ok := reloaded.GetBranch("feature-x"); !ok {
		t.Error("re-init dropped the existing branch")
	}
	if reloaded.CurrentBranch != "feature-x" {
		t.Errorf("re-init cleared current_branch: got %q", reloaded.CurrentBranch)
	}
}

// TestStateRoundTripPreservesServerURL ensures ServerURL survives marshal
// round-trips so non-default Postgres servers (different host/port/user) can
// be reconnected across invocations without the old localhost:5432 guess.
func TestStateRoundTripPreservesServerURL(t *testing.T) {
	dir := t.TempDir()
	s := &config.State{
		MainDB:    "myapp_dev",
		ServerURL: "postgresql://alice:secret@pg.example.com:6432/",
		Branches:  make(map[string]config.BranchState),
	}
	s.SetPath(dir)
	if err := s.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.ServerURL != s.ServerURL {
		t.Errorf("ServerURL: got %q, want %q", loaded.ServerURL, s.ServerURL)
	}
}

func TestRemoveBranch(t *testing.T) {
	state := &config.State{
		CurrentBranch: "feature-x",
		Branches:      make(map[string]config.BranchState),
	}
	state.AddBranch("feature-x", config.BranchState{DBName: "pgbr_feature_x"})

	state.RemoveBranch("feature-x")
	if _, ok := state.GetBranch("feature-x"); ok {
		t.Error("branch should be removed")
	}
	if state.CurrentBranch != "" {
		t.Error("current branch should be cleared when removed branch was current")
	}
}
