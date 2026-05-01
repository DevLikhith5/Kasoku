package commands

import (
	"fmt"
	"sort"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var scanCmd = &cobra.Command{
	Use:   "scan <prefix>",
	Short: "Scan keys by prefix",
	Long:  "Scan and retrieve all keys with a given prefix",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		prefix := args[0]
		entries, err := engineInst.Scan(prefix)
		if err != nil {
			output.PrintError("Scan failed: %v\n", err)
			return err
		}

		if len(entries) == 0 {
			output.PrintInfo("No keys found with prefix: %s\n", prefix)
			return nil
		}

		// Sort entries by key
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Key < entries[j].Key
		})

		// Output based on format
		switch outputFmt {
		case "json":
			var data []map[string]interface{}
			for _, entry := range entries {
				data = append(data, map[string]interface{}{
					"key":       entry.Key,
					"value":     string(entry.Value),
					"version":   entry.Version,
					"timestamp": entry.TimeStamp,
				})
			}
			output.PrettyPrint(data)

		case "table":
			table := output.NewTable()
			table.Header([]string{"Key", "Value", "Version", "Size"})
			for _, entry := range entries {
				table.Append([]string{
					entry.Key,
					output.Truncate(string(entry.Value), 50),
					fmt.Sprintf("%d", entry.Version),
					fmt.Sprintf("%d bytes", len(entry.Value)),
				})
			}
			table.Render()

		default: // text
			for _, entry := range entries {
				output.PrintKey(entry.Key)
				fmt.Print(" = ")
				output.PrintValue(string(entry.Value))
				fmt.Println()
			}
		}

		output.PrintInfo("\nScanned %d keys\n", len(entries))
		return nil
	},
}

func init() {
	RootCmd.AddCommand(scanCmd)
}
