package commands

import (
	"fmt"
	"sort"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var keysCmd = &cobra.Command{
	Use:   "keys",
	Short: "List all keys",
	Long:  "List all keys in the database",
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		keys, err := engineInst.Keys()
		if err != nil {
			output.PrintError("Failed to get keys: %v\n", err)
			return err
		}

		if len(keys) == 0 {
			output.PrintInfo("No keys in database\n")
			return nil
		}

		// Sort keys
		sort.Strings(keys)

		// Output
		switch outputFmt {
		case "json":
			output.PrettyPrint(keys)

		default: // text
			for _, key := range keys {
				output.PrintKey(key)
				fmt.Println()
			}
		}

		output.PrintInfo("\nTotal: %d keys\n", len(keys))
		return nil
	},
}

func init() {
	RootCmd.AddCommand(keysCmd)
}
