package commands

import (
	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/validate"
	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Store a key-value pair",
	Long:  "Store a key-value pair in the database",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		key := args[0]
		value := args[1]

		// Validate key
		if err := validate.Key(key); err != nil {
			output.PrintError("Invalid key: %v\n", err)
			return err
		}

		// Validate value
		if err := validate.Value(value); err != nil {
			output.PrintError("Invalid value: %v\n", err)
			return err
		}

		// Put the value
		if err := engineInst.Put(key, []byte(value)); err != nil {
			output.PrintError("Put failed: %v\n", err)
			return err
		}

		output.PrintSuccess("OK\n")
		if verbose {
			output.PrintInfo("  Stored '%s' (%d bytes)\n", key, len(value))
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(putCmd)
}
