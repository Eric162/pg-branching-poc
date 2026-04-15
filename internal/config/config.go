package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	ConfigFileName = ".pg-branch.yaml"
	StateFileName  = ".pg-branch.state.json"
)

// Config represents the .pg-branch.yaml configuration.
type Config struct {
	PGURL        string `json:"pg_url"`
	BranchPrefix string `json:"branch_prefix"`
}

// DefaultConfig returns config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		PGURL:        "postgresql://localhost:5432/postgres",
		BranchPrefix: "pgbr_",
	}
}

// BranchState tracks metadata for a single branch.
type BranchState struct {
	DBName    string `json:"db_name"`
	ParentDB  string `json:"parent_db"`
	CreatedAt string `json:"created_at"`
}

// State represents the .pg-branch.state.json file.
type State struct {
	CurrentBranch string                 `json:"current_branch"`
	MainDB        string                 `json:"main_db"`
	Branches      map[string]BranchState `json:"branches"`
	path          string
}

// LoadState reads state from disk. Returns empty state if file doesn't exist.
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

// Save writes state to disk.
func (s *State) Save() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return os.WriteFile(s.path, data, 0644)
}

// SetPath sets the file path for saving.
func (s *State) SetPath(dir string) {
	s.path = filepath.Join(dir, StateFileName)
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
