package commands

import (
	"fmt"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Retrieve a value by key",
	Long:  "Retrieve a value by key from the database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		key := args[0]
		entry, err := engineInst.Get(key)
		if err != nil {
			output.PrintError("Key not found: %s\n", key)
			return err
		}

		// Output based on format
		switch outputFmt {
		case "json":
			data := map[string]interface{}{
				"key":       key,
				"value":     string(entry.Value),
				"version":   entry.Version,
				"timestamp": entry.TimeStamp,
			}
			output.PrettyPrint(data)

		case "table":
			table := output.NewTable()
			table.Header([]string{"Key", "Value", "Version", "Timestamp"})
			table.Append([]string{
				key,
				string(entry.Value),
				fmt.Sprintf("%d", entry.Version),
				entry.TimeStamp.Format("2006-01-02 15:04:05"),
			})
			table.Render()

		default: // text
			output.PrintKey(fmt.Sprintf("Key:     %s\n", key))
			output.PrintKey(fmt.Sprintf("Value:   %s\n", string(entry.Value)))
			output.PrintKey(fmt.Sprintf("Version:  %d\n", entry.Version))
			output.PrintKey(fmt.Sprintf("Time:     %s\n", entry.TimeStamp.Format("2006-01-02 15:04:05")))
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(getCmd)
}
