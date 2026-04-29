package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	StateFileName = ".pg-branch.state.json"

	// BranchDBPrefix is prepended to the user-supplied branch name to form
	// the actual Postgres database name (e.g. "my-feature" → "pgbr_my-feature").
	// Callers must use this rather than hard-coding the prefix so the
	// create / delete / connect / status paths stay in sync.
	BranchDBPrefix = "pgbr_"

	// CentralAppDir is the per-user directory under XDG_STATE_HOME (or
	// ~/.local/state on Unix-likes when XDG_STATE_HOME is unset) where
	// pg-branch keeps one state file per parent database plus a `current`
	// pointer to the active context.
	CentralAppDir = "pg-branch"

	// CurrentPointerFileName lives inside CentralAppDir and contains the
	// MainDB name of the active context. The CLI consults it when no
	// explicit --state-file / PG_BRANCH_STATE_FILE / cwd state file is
	// provided. Plain text rather than JSON so users can inspect or edit
	// it without ceremony.
	CurrentPointerFileName = "current"
)

// BranchState tracks metadata for a single branch.
type BranchState struct {
	DBName    string `json:"db_name"`
	ParentDB  string `json:"parent_db"`
	CreatedAt string `json:"created_at"`
}

// State represents the .pg-branch.state.json file.
type State struct {
	CurrentBranch string `json:"current_branch"`
	MainDB        string `json:"main_db"`
	// ServerURL stores the connection URL captured at init time with the
	// database path stripped (e.g. "postgresql://user:pw@host:5432/"). It's
	// used to reconstruct URLs for the main DB and for any branch DB on the
	// same server without guessing host/port/user. Empty for state files
	// that predate this field — callers should fall back to a sensible
	// localhost default in that case.
	ServerURL string                 `json:"server_url,omitempty"`
	Branches  map[string]BranchState `json:"branches"`
	path      string
}

// NewState constructs an empty State targeting dir for future Save() calls.
// Prefer this over a &State{} literal so the save path is always populated —
// a State built with a bare literal will panic or write to a wrong location
// on Save. Use LoadState when a state file may already exist on disk.
func NewState(dir string) *State {
	return &State{
		Branches: make(map[string]BranchState),
		path:     filepath.Join(dir, StateFileName),
	}
}

// LoadState reads state from disk. Returns an empty state targeting dir if
// the file doesn't exist, so callers can unconditionally modify and Save().
func LoadState(dir string) (*State, error) {
	p := filepath.Join(dir, StateFileName)
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return &State{
			Branches: make(map[string]BranchState),
			path:     p,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse state file: %w", err)
	}
	if s.Branches == nil {
		s.Branches = make(map[string]BranchState)
	}
	s.path = p
	return &s, nil
}

// Save writes state to disk. Panics if the state wasn't built with NewState
// or LoadState — that's a programming error caught loudly rather than a
// silent empty-path write.
func (s *State) Save() error {
	if s.path == "" {
		panic("config.State.Save called on a State with no path — construct via NewState or LoadState")
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return os.WriteFile(s.path, data, 0644)
}

// NewStateAtFile constructs an empty State whose Save() writes to path
// directly (no StateFileName join). Use this when the caller already
// knows the full file path — e.g. central per-DB state files in the
// XDG state directory, or files supplied via --state-file.
func NewStateAtFile(path string) *State {
	return &State{
		Branches: make(map[string]BranchState),
		path:     path,
	}
}

// LoadStateFromFile reads state from a specific file. Returns an empty
// state targeting path if the file doesn't exist (so init can blindly
// load-then-save without race-prone existence checks).
func LoadStateFromFile(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &State{
			Branches: make(map[string]BranchState),
			path:     path,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse state file: %w", err)
	}
	if s.Branches == nil {
		s.Branches = make(map[string]BranchState)
	}
	s.path = path
	return &s, nil
}

// Path returns the on-disk location this State will save to. Useful for
// callers that need to surface the path in user-facing messages without
// reaching into private fields.
func (s *State) Path() string {
	return s.path
}

// CentralStateDir returns the per-user directory where pg-branch keeps
// central state, creating it if missing. Honours $XDG_STATE_HOME, falls
// back to $HOME/.local/state per the XDG Base Directory Specification.
//
// On macOS we deliberately use the XDG layout rather than
// ~/Library/Application Support — pg-branch is a developer tool whose
// users overwhelmingly think in dotfiles terms, and matching Linux keeps
// scripts portable.
func CentralStateDir() (string, error) {
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("locate home dir: %w", err)
		}
		base = filepath.Join(home, ".local", "state")
	}
	dir := filepath.Join(base, CentralAppDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("create state dir: %w", err)
	}
	return dir, nil
}

// CentralStateFile returns the central per-DB state file path for
// mainDB, creating the parent directory if needed. The file itself is
// not touched — callers Load/Save explicitly.
func CentralStateFile(mainDB string) (string, error) {
	if mainDB == "" {
		return "", fmt.Errorf("mainDB required to resolve central state file")
	}
	dir, err := CentralStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, mainDB+".json"), nil
}

// CurrentPointerPath returns the path to the `current` pointer file.
func CurrentPointerPath() (string, error) {
	dir, err := CentralStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, CurrentPointerFileName), nil
}

// ReadCurrent returns the MainDB name recorded in the central `current`
// pointer file, or "" if the file doesn't exist. Trailing whitespace is
// trimmed so manually-edited pointers with stray newlines still resolve.
func ReadCurrent() (string, error) {
	p, err := CurrentPointerPath()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read current pointer: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// WriteCurrent atomically replaces the central `current` pointer with
// mainDB. Atomicity matters because two terminals running `pg-branch
// use` simultaneously shouldn't be able to leave a half-written pointer
// that other commands then misread.
func WriteCurrent(mainDB string) error {
	p, err := CurrentPointerPath()
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(mainDB+"\n"), 0644); err != nil {
		return fmt.Errorf("write current pointer: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("install current pointer: %w", err)
	}
	return nil
}

// ListCentralStateDBs returns the MainDB names known in the central
// state directory (one per *.json file). Order is filesystem order.
// Used by the CLI to list contexts and by `use` for validation.
func ListCentralStateDBs() ([]string, error) {
	dir, err := CentralStateDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read state dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		names = append(names, strings.TrimSuffix(name, ".json"))
	}
	return names, nil
}

// AddBranch records a new branch in state.
func (s *State) AddBranch(name string, bs BranchState) {
	s.Branches[name] = bs
}

// RemoveBranch removes a branch from state.
func (s *State) RemoveBranch(name string) {
	delete(s.Branches, name)
	if s.CurrentBranch == name {
		s.CurrentBranch = ""
	}
}

// GetBranch returns branch state by name.
func (s *State) GetBranch(name string) (BranchState, bool) {
	bs, ok := s.Branches[name]
	return bs, ok
}
