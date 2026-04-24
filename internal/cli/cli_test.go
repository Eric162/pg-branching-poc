package cli_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pg-branch/pg-branch/internal/cli"
	"github.com/pg-branch/pg-branch/internal/config"
)

// runCLI invokes the shared root command with the given args and returns the
// combined stdout+stderr output plus any error. The RunE handlers currently
// write user-facing strings via fmt.Printf (not cmd.Out), so we redirect
// os.Stdout and os.Stderr during the call to capture both.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cli.ResetFlagsForTest()

	root := cli.RootForTest()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)

	prevSilenceUsage, prevSilenceErrors := root.SilenceUsage, root.SilenceErrors
	root.SilenceUsage = true
	root.SilenceErrors = true
	t.Cleanup(func() {
		root.SilenceUsage = prevSilenceUsage
		root.SilenceErrors = prevSilenceErrors
	})

	// Redirect OS-level stdout/stderr into a pipe that feeds the same buffer.
	origStdout, origStderr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	os.Stderr = w

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	execErr := root.Execute()

	w.Close()
	os.Stdout, os.Stderr = origStdout, origStderr
	<-done
	r.Close()

	return buf.String(), execErr
}

// writeState puts a .pg-branch.state.json in dir with fields copied from the
// given template. The template is a plain struct literal — using NewState +
// field assignment inline at every call site would make the tests much
// noisier — so this helper translates the "declarative fixture" style into
// a properly-initialised State with a save path.
func writeState(t *testing.T, dir string, template *config.State) {
	t.Helper()
	s := config.NewState(dir)
	s.MainDB = template.MainDB
	s.CurrentBranch = template.CurrentBranch
	s.ServerURL = template.ServerURL
	if template.Branches != nil {
		for k, v := range template.Branches {
			s.Branches[k] = v
		}
	}
	if err := s.Save(); err != nil {
		t.Fatalf("write state: %v", err)
	}
}

func TestVersionCommand(t *testing.T) {
	out, err := runCLI(t, "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.Contains(out, "pg-branch") {
		t.Errorf("expected output to mention pg-branch, got %q", out)
	}
}

func TestSwitchBranchNotFound(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:   "myapp_dev",
		Branches: map[string]config.BranchState{},
	})

	out, err := runCLI(t, "switch", "nonexistent")
	if err == nil {
		t.Fatalf("expected error, got output: %s", out)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestSwitchToMainClearsCurrent(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:        "myapp_dev",
		CurrentBranch: "feature-x",
		Branches: map[string]config.BranchState{
			"feature-x": {DBName: "pgbr_feature-x", ParentDB: "myapp_dev", CreatedAt: time.Now().Format(time.RFC3339)},
		},
	})

	out, err := runCLI(t, "switch", "main")
	if err != nil {
		t.Fatalf("switch main: %v", err)
	}
	if !strings.Contains(out, "main") {
		t.Errorf("expected 'main' in output, got %q", out)
	}

	reloaded, err := config.LoadState(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.CurrentBranch != "" {
		t.Errorf("expected current_branch cleared, got %q", reloaded.CurrentBranch)
	}
}

func TestDeleteMainRefused(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:   "myapp_dev",
		Branches: map[string]config.BranchState{},
	})

	_, err := runCLI(t, "delete", "main", "--pg-url=postgresql://localhost:5432/myapp_dev")
	if err == nil {
		t.Fatal("expected error for deleting main")
	}
	if !strings.Contains(err.Error(), "cannot delete main") {
		t.Errorf("expected 'cannot delete main', got: %v", err)
	}
}

func TestDeleteBranchNotFound(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:   "myapp_dev",
		Branches: map[string]config.BranchState{},
	})

	_, err := runCLI(t, "delete", "ghost", "--pg-url=postgresql://localhost:5432/myapp_dev")
	if err == nil {
		t.Fatal("expected error for nonexistent branch")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found', got: %v", err)
	}
}

func TestMergeInvalidResolve(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB: "myapp_dev",
		Branches: map[string]config.BranchState{
			"feature-x": {DBName: "pgbr_feature-x", ParentDB: "myapp_dev", CreatedAt: time.Now().Format(time.RFC3339)},
		},
	})

	_, err := runCLI(t, "merge", "feature-x", "--resolve=bogus", "--pg-url=postgresql://localhost:5432/myapp_dev")
	if err == nil {
		t.Fatal("expected error for invalid --resolve")
	}
	if !strings.Contains(err.Error(), "invalid --resolve value") {
		t.Errorf("expected 'invalid --resolve value', got: %v", err)
	}
}

func TestMergeBranchNotFound(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:   "myapp_dev",
		Branches: map[string]config.BranchState{},
	})

	_, err := runCLI(t, "merge", "ghost", "--pg-url=postgresql://localhost:5432/myapp_dev")
	if err == nil {
		t.Fatal("expected error for nonexistent branch")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found', got: %v", err)
	}
}

func TestNoURLResolvable(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	// No state file written, no --pg-url, no PG_BRANCH_URL env set.
	os.Unsetenv("PG_BRANCH_URL")

	// list calls mustResolveURL which exits with os.Exit(1) on failure; we
	// can't observe that from tests without intercepting. Use switch
	// instead — it only consults state.CurrentBranch and doesn't demand a
	// URL, so we exercise the state-loading path cleanly.
	out, err := runCLI(t, "switch", "main")
	if err != nil {
		t.Fatalf("switch without URL should still work: %v (out=%s)", err, out)
	}
}

func TestStatusNoCurrentBranch(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeState(t, dir, &config.State{
		MainDB:    "myapp_dev",
		ServerURL: "postgresql://localhost:5432/",
		Branches:  map[string]config.BranchState{},
	})

	out, err := runCLI(t, "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out, "main") {
		t.Errorf("expected 'main' in output, got %q", out)
	}
	if !strings.Contains(out, "No branch selected") {
		t.Errorf("expected 'No branch selected' in output, got %q", out)
	}
}

func TestAdminDBNameDefault(t *testing.T) {
	t.Setenv("PG_BRANCH_ADMIN_DB", "")
	if got := cli.AdminDBNameForTest(); got != "postgres" {
		t.Errorf("default admin DB: got %q, want %q", got, "postgres")
	}
}

func TestAdminDBNameFromEnv(t *testing.T) {
	t.Setenv("PG_BRANCH_ADMIN_DB", "defaultdb")
	if got := cli.AdminDBNameForTest(); got != "defaultdb" {
		t.Errorf("env-provided admin DB: got %q, want %q", got, "defaultdb")
	}
}

func TestStateFileWritten(t *testing.T) {
	// Sanity: NewState uses the directory the test passes in, which tests
	// couple to t.Chdir. A save should land there, not somewhere stale from
	// a previous test.
	dir := t.TempDir()
	t.Chdir(dir)

	writeState(t, dir, &config.State{MainDB: "app", Branches: map[string]config.BranchState{}})
	if _, err := os.Stat(filepath.Join(dir, config.StateFileName)); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
}
