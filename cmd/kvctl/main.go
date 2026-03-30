package main

import (
	"os"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/commands"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
)

func main() {
	// Setup colors
	output.SetupColors()

	// Execute root command
	if err := commands.RootCmd.Execute(); err != nil {
		output.ErrorColor.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
