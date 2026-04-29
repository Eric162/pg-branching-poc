package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pg-branch/pg-branch/internal/config"
	"github.com/spf13/cobra"
)

var useCmd = &cobra.Command{
	Use:   "use [<main-db>]",
	Short: "Switch the central 'current' pointer to another initialized database",
	Long: `Update $XDG_STATE_HOME/pg-branch/current so subsequent naked
commands operate on <main-db>. With no argument, prints the current
pointer and the list of known databases.

Each <main-db> must already have a state file at
$XDG_STATE_HOME/pg-branch/<main-db>.json — created by 'pg-branch init'
on that database. Use does not migrate legacy in-project state files;
re-init the DB without --cwd to publish it centrally.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		known, err := config.ListCentralStateDBs()
		if err != nil {
			return fmt.Errorf("list central state: %w", err)
		}

		if len(args) == 0 {
			current, err := config.ReadCurrent()
			if err != nil {
				return err
			}
			if current == "" {
				fmt.Println("No current context set.")
			} else {
				fmt.Printf("Current: %s\n", current)
			}
			if len(known) == 0 {
				fmt.Println("No central state files. Run 'pg-branch init --pg-url=...' to create one.")
				return nil
			}
			sort.Strings(known)
			fmt.Println("Available:")
			for _, n := range known {
				marker := "  "
				if n == current {
					marker = "* "
				}
				fmt.Printf("%s%s\n", marker, n)
			}
			return nil
		}

		target := args[0]
		if !contains(known, target) {
			hint := ""
			if len(known) > 0 {
				sort.Strings(known)
				hint = fmt.Sprintf(" Known: %s.", strings.Join(known, ", "))
			}
			return fmt.Errorf("no central state file for %q.%s Run 'pg-branch init --pg-url=...' on that database first", target, hint)
		}
		if err := config.WriteCurrent(target); err != nil {
			return err
		}
		fmt.Printf("Switched current context to %q\n", target)
		return nil
	},
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
