package cli

import "github.com/spf13/cobra"

// RootForTest returns the shared rootCmd so tests can drive it with SetArgs /
// SetOut / SetErr without duplicating the registration logic in each subcommand's
// init(). Call ResetFlagsForTest before each test to clear flag state left by
// the previous one — cobra keeps parsed flag values on the package-level vars.
func RootForTest() *cobra.Command { return rootCmd }

// ResetFlagsForTest clears every flag variable defined in this package so
// tests observe a clean slate. Add new entries here whenever a command gains
// a flag, otherwise that flag will leak across tests.
func ResetFlagsForTest() {
	pgURL = ""
	createFrom = ""
	mergeInto = ""
	mergeApply = false
	mergeResolve = ""
	mergeNoLock = false
	mergeNoData = false
	logVerbose = false
}
