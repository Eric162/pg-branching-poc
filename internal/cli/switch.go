package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var switchCmd = &cobra.Command{
	Use:   "switch <name>",
	Short: "Switch current branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]

		state, err := loadStateFromCwd()
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}

		// "main" is a special name
		if name == "main" {
			state.CurrentBranch = ""
			if err := state.Save(); err != nil {
				return fmt.Errorf("save state: %w", err)
			}
			fmt.Printf("Switched to main (%s)\n", state.MainDB)
			return nil
		}

		if _, exists := state.GetBranch(name); !exists {
			return fmt.Errorf("branch %q not found. Run 'pg-branch list' to see available branches", name)
		}

		state.CurrentBranch = name
		if err := state.Save(); err != nil {
			return fmt.Errorf("save state: %w", err)
		}

		fmt.Printf("Switched to branch %q\n", name)
		return nil
	},
}
