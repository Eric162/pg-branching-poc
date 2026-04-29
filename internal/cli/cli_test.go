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

// TestSwitchDoesNotRequireURL: switch only consults state.CurrentBranch,
// not the Postgres connection URL, so it works without --pg-url or
// PG_BRANCH_URL as long as a state file exists. The legacy CWD layout
// is the easiest way to set up state without poking at XDG paths in a
// test, and it exercises the legacy-resolution branch of resolveStateFile.
func TestSwitchDoesNotRequireURL(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	os.Unsetenv("PG_BRANCH_URL")
	writeState(t, dir, &config.State{
		MainDB:   "myapp_dev",
		Branches: map[string]config.BranchState{},
	})

	out, err := runCLI(t, "switch", "main")
	if err != nil {
		t.Fatalf("switch without URL should still work: %v (out=%s)", err, out)
	}
}

// TestNakedCommandWithNoStateErrors: with no state file in CWD, no env
// override, and no central pointer, commands should fail with a clear
// "run init" message rather than silently producing an empty state and
// pretending to succeed (which is what the old loadStateFromCwd did).
func TestNakedCommandWithNoStateErrors(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	os.Unsetenv("PG_BRANCH_URL")
	t.Setenv("PG_BRANCH_STATE_FILE", "")
	t.Setenv("XDG_STATE_HOME", t.TempDir()) // empty, no `current` pointer

	_, err := runCLI(t, "switch", "main")
	if err == nil {
		t.Fatal("expected error when no state can be resolved")
	}
	if !strings.Contains(err.Error(), "init") {
		t.Errorf("expected error to mention 'init', got: %v", err)
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

// isolateXDG points XDG_STATE_HOME at a fresh temp dir for the test's
// duration. Without this, tests touching central state would scribble
// in the developer's real ~/.local/state/pg-branch directory.
func isolateXDG(t *testing.T) string {
	t.Helper()
	xdg := t.TempDir()
	t.Setenv("XDG_STATE_HOME", xdg)
	return xdg
}

// TestUseListsKnownContexts: 'pg-branch use' with no arg prints the
// current pointer (or 'No current context set.') and the list of
// central state files. This is the discovery path users hit when they
// land on a new shell and want to see what's available.
func TestUseListsKnownContexts(t *testing.T) {
	xdg := isolateXDG(t)
	t.Chdir(t.TempDir())
	t.Setenv("PG_BRANCH_STATE_FILE", "")
	os.Unsetenv("PG_BRANCH_URL")

	// Seed two central state files. Use config helpers directly rather
	// than the runCLI path, so we don't depend on a live Postgres for
	// init.
	for _, name := range []string{"alpha", "beta"} {
		p := filepath.Join(xdg, "pg-branch", name+".json")
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		s := config.NewStateAtFile(p)
		s.MainDB = name
		if err := s.Save(); err != nil {
			t.Fatalf("save %s: %v", name, err)
		}
	}

	out, err := runCLI(t, "use")
	if err != nil {
		t.Fatalf("use: %v (out=%s)", err, out)
	}
	for _, want := range []string{"No current context set.", "alpha", "beta"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got: %s", want, out)
		}
	}
}

// TestUseSwitchesCurrentPointer: 'pg-branch use <name>' updates the
// central 'current' file, and a subsequent state-loading command reads
// from that DB's central state file.
func TestUseSwitchesCurrentPointer(t *testing.T) {
	xdg := isolateXDG(t)
	t.Chdir(t.TempDir())
	t.Setenv("PG_BRANCH_STATE_FILE", "")
	os.Unsetenv("PG_BRANCH_URL")

	// Seed a central state file with a branch entry; the test will
	// 'use' it and then assert that 'switch' on that branch resolves
	// without further configuration.
	p := filepath.Join(xdg, "pg-branch", "alpha.json")
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	s := config.NewStateAtFile(p)
	s.MainDB = "alpha"
	s.AddBranch("feature-a", config.BranchState{
		DBName:    "pgbr_feature-a",
		ParentDB:  "alpha",
		CreatedAt: "2026-04-28T00:00:00Z",
	})
	if err := s.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	out, err := runCLI(t, "use", "alpha")
	if err != nil {
		t.Fatalf("use alpha: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "alpha") {
		t.Errorf("expected confirmation mentioning alpha, got: %s", out)
	}

	// 'switch feature-a' should now find the branch via the central pointer.
	out, err = runCLI(t, "switch", "feature-a")
	if err != nil {
		t.Fatalf("switch feature-a after use: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "feature-a") {
		t.Errorf("expected switch confirmation, got: %s", out)
	}
}

// TestUseRejectsUnknownContext: 'use' won't write the pointer to a DB
// that has no central state file — that would just defer the failure to
// the next command with a worse error message.
func TestUseRejectsUnknownContext(t *testing.T) {
	isolateXDG(t)
	t.Chdir(t.TempDir())

	_, err := runCLI(t, "use", "ghost")
	if err == nil {
		t.Fatal("expected error for unknown context")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error should name the missing context, got: %v", err)
	}
}

// TestStateFileFlagOverridesCentral: --state-file beats both the CWD
// legacy file and the central pointer. Useful for one-off operations
// against an arbitrary state file (a backup, or a colleague's export)
// without touching the user's normal context.
func TestStateFileFlagOverridesCentral(t *testing.T) {
	xdg := isolateXDG(t)
	t.Chdir(t.TempDir())
	os.Unsetenv("PG_BRANCH_URL")

	// Central context says "alpha"
	pAlpha := filepath.Join(xdg, "pg-branch", "alpha.json")
	if err := os.MkdirAll(filepath.Dir(pAlpha), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	sAlpha := config.NewStateAtFile(pAlpha)
	sAlpha.MainDB = "alpha"
	sAlpha.AddBranch("alpha-feat", config.BranchState{DBName: "pgbr_alpha-feat", ParentDB: "alpha"})
	if err := sAlpha.Save(); err != nil {
		t.Fatalf("save alpha: %v", err)
	}
	if err := config.WriteCurrent("alpha"); err != nil {
		t.Fatalf("write current: %v", err)
	}

	// Out-of-band state file mentions a different branch
	other := filepath.Join(t.TempDir(), "manual-state.json")
	sOther := config.NewStateAtFile(other)
	sOther.MainDB = "elsewhere"
	sOther.AddBranch("manual-branch", config.BranchState{DBName: "pgbr_manual-branch", ParentDB: "elsewhere"})
	if err := sOther.Save(); err != nil {
		t.Fatalf("save other: %v", err)
	}

	out, err := runCLI(t, "switch", "manual-branch", "--state-file", other)
	if err != nil {
		t.Fatalf("switch via --state-file: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "manual-branch") {
		t.Errorf("expected switch to manual-branch, got: %s", out)
	}

	// And alpha's central state file should be unchanged.
	reloaded, err := config.LoadStateFromFile(pAlpha)
	if err != nil {
		t.Fatalf("reload alpha: %v", err)
	}
	if reloaded.CurrentBranch != "" {
		t.Errorf("alpha state should be untouched; got CurrentBranch=%q", reloaded.CurrentBranch)
	}
}

// TestCWDStateBeatsCentralPointer: a project-local .pg-branch.state.json
// takes precedence over the central pointer. This preserves the legacy
// per-repo workflow for users who haven't migrated, and lets a project
// pin a specific context simply by checking in (or never deleting) its
// state file.
func TestCWDStateBeatsCentralPointer(t *testing.T) {
	xdg := isolateXDG(t)
	dir := t.TempDir()
	t.Chdir(dir)
	os.Unsetenv("PG_BRANCH_URL")
	t.Setenv("PG_BRANCH_STATE_FILE", "")

	// Central says "alpha"
	pAlpha := filepath.Join(xdg, "pg-branch", "alpha.json")
	if err := os.MkdirAll(filepath.Dir(pAlpha), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	sAlpha := config.NewStateAtFile(pAlpha)
	sAlpha.MainDB = "alpha"
	if err := sAlpha.Save(); err != nil {
		t.Fatalf("save alpha: %v", err)
	}
	if err := config.WriteCurrent("alpha"); err != nil {
		t.Fatalf("write current: %v", err)
	}

	// CWD state mentions a different DB and a branch
	writeState(t, dir, &config.State{
		MainDB: "project_local",
		Branches: map[string]config.BranchState{
			"local-feat": {DBName: "pgbr_local-feat", ParentDB: "project_local"},
		},
	})

	out, err := runCLI(t, "switch", "local-feat")
	if err != nil {
		t.Fatalf("switch (CWD): %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "local-feat") {
		t.Errorf("CWD state should win; got: %s", out)
	}
}
