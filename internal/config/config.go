package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	StateFileName = ".pg-branch.state.json"

	// BranchDBPrefix is prepended to the user-supplied branch name to form
	// the actual Postgres database name (e.g. "my-feature" → "pgbr_my-feature").
	// Callers must use this rather than hard-coding the prefix so the
	// create / delete / connect / status paths stay in sync.
	BranchDBPrefix = "pgbr_"
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
