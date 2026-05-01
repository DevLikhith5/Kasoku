package commands

import (
	"encoding/json"
	"os"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var (
	importFile string
	exportFile string
)

var importCmd = &cobra.Command{
	Use:   "import <file>",
	Short: "Import data from JSON file",
	Long:  "Import key-value pairs from a JSON file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		// Read file
		data, err := os.ReadFile(filename)
		if err != nil {
			output.PrintError("Failed to read file: %v\n", err)
			return err
		}

		// Parse JSON
		var entries map[string]string
		if err := json.Unmarshal(data, &entries); err != nil {
			output.PrintError("Failed to parse JSON: %v\n", err)
			return err
		}

		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		// Import entries
		count := 0
		for key, value := range entries {
			if err := engineInst.Put(key, []byte(value)); err != nil {
				output.PrintError("Failed to put %s: %v\n", key, err)
				continue
			}
			count++
		}

		output.PrintSuccess("Imported %d entries from %s\n", count, filename)
		return nil
	},
}

var exportCmd = &cobra.Command{
	Use:   "export <file>",
	Short: "Export data to JSON file",
	Long:  "Export all key-value pairs to a JSON file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]

		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		// Get all entries
		entries, err := engineInst.Scan("")
		if err != nil {
			output.PrintError("Failed to scan database: %v\n", err)
			return err
		}

		// Convert to map
		data := make(map[string]string)
		for _, entry := range entries {
			data[entry.Key] = string(entry.Value)
		}

		// Write to file
		jsonData, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			output.PrintError("Failed to marshal JSON: %v\n", err)
			return err
		}

		if err := os.WriteFile(filename, jsonData, 0600); err != nil {
			output.PrintError("Failed to write file: %v\n", err)
			return err
		}

		output.PrintSuccess("Exported %d entries to %s\n", len(entries), filename)
		return nil
	},
}

func init() {
	importCmd.Flags().StringVarP(&importFile, "file", "f", "", "JSON file to import")
	exportCmd.Flags().StringVarP(&exportFile, "file", "f", "", "JSON file to export to")
	RootCmd.AddCommand(importCmd)
	RootCmd.AddCommand(exportCmd)
}
