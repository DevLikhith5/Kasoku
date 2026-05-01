package commands

import (
	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/validate"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete a key",
	Long:  "Delete a key from the database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		key := args[0]

		// Validate key
		if err := validate.Key(key); err != nil {
			output.PrintError("Invalid key: %v\n", err)
			return err
		}

		if err := engineInst.Delete(key); err != nil {
			output.PrintError("Delete failed: %v\n", err)
			return err
		}

		output.PrintSuccess("OK\n")
		if verbose {
			output.PrintInfo("  Deleted '%s'\n", key)
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(deleteCmd)
}
