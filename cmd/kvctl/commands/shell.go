package commands

import (
	"fmt"
	"strings"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Interactive shell",
	Long:  "Start an interactive shell for running commands",
	RunE: func(cmd *cobra.Command, args []string) error {
		output.PrintInfo("Starting interactive shell...\n")
		output.PrintInfo("Type 'help' for available commands, 'exit' to quit\n\n")

		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		prompt := promptui.Prompt{
			Label: "kvctl",
			Templates: &promptui.PromptTemplates{
				Prompt:  "{{ . }} ",
				Valid:   "{{ . | green }} ",
				Invalid: "{{ . | cyan }} ",
				Success: "{{ . | green }} ",
			},
			Validate: func(input string) error {
				if strings.TrimSpace(input) == "" {
					return fmt.Errorf("command cannot be empty")
				}
				return nil
			},
		}

		for {
			result, err := prompt.Run()
			if err != nil {
				if err.Error() == "^C" || err.Error() == "^D" {
					fmt.Println()
					output.PrintInfo("Exiting shell\n")
					return nil
				}
				continue
			}

			if err := processCommand(result, engineInst); err != nil {
				if err.Error() == "exit" {
					return nil
				}
				output.PrintError("Error: %v\n", err)
			}
		}
	},
}

func processCommand(input string, engineInst engine.KVEngine) error {
	parts := parseArgs(input)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "help":
		printHelp()

	case "exit", "quit":
		output.PrintInfo("Exiting shell\n")
		return fmt.Errorf("exit")

	case "put":
		if len(args) != 2 {
			output.PrintError("Usage: put <key> <value>\n")
			return nil
		}
		if err := engineInst.Put(args[0], []byte(args[1])); err != nil {
			output.PrintError("Error: %v\n", err)
			return nil
		}
		output.PrintSuccess("OK\n")

	case "get":
		if len(args) != 1 {
			output.PrintError("Usage: get <key>\n")
			return nil
		}
		entry, err := engineInst.Get(args[0])
		if err != nil {
			output.PrintError("Key not found: %s\n", args[0])
			return nil
		}
		output.PrintKey(fmt.Sprintf("Key:     %s\n", args[0]))
		output.PrintKey(fmt.Sprintf("Value:   %s\n", string(entry.Value)))
		output.PrintKey(fmt.Sprintf("Version:  %d\n", entry.Version))
		output.PrintKey(fmt.Sprintf("Time:     %s\n", entry.TimeStamp.Format("2006-01-02 15:04:05")))

	case "delete":
		if len(args) != 1 {
			output.PrintError("Usage: delete <key>\n")
			return nil
		}
		if err := engineInst.Delete(args[0]); err != nil {
			output.PrintError("Error: %v\n", err)
			return nil
		}
		output.PrintSuccess("OK\n")

	case "scan":
		if len(args) != 1 {
			output.PrintError("Usage: scan <prefix>\n")
			return nil
		}
		entries, err := engineInst.Scan(args[0])
		if err != nil {
			output.PrintError("Error: %v\n", err)
			return nil
		}
		if len(entries) == 0 {
			output.PrintInfo("No keys found\n")
			return nil
		}
		for _, entry := range entries {
			fmt.Printf("%s = %s\n", entry.Key, string(entry.Value))
		}

	case "keys":
		keys, err := engineInst.Keys()
		if err != nil {
			output.PrintError("Error: %v\n", err)
			return nil
		}
		if len(keys) == 0 {
			output.PrintInfo("No keys found\n")
			return nil
		}
		for _, k := range keys {
			fmt.Println(k)
		}

	case "stats":
		stats := engineInst.Stats()
		fmt.Printf("Key Count:  %d\n", stats.KeyCount)
		fmt.Printf("Disk Bytes: %s\n", output.FormatBytes(stats.DiskBytes))
		fmt.Printf("Mem Bytes:  %s\n", output.FormatBytes(stats.MemBytes))
		fmt.Printf("Bloom FP:   %.4f\n", stats.BloomFPRate)

	case "flush":
		if err := engineInst.Flush(); err != nil {
			output.PrintError("Error: %v\n", err)
			return nil
		}
		output.PrintSuccess("Memtable flushed successfully\n")

	case "":
		return nil

	default:
		output.PrintError("Unknown command: %s. Type 'help' for available commands.\n", cmd)
	}

	return nil
}

func printHelp() {
	help := `Available Commands:
  put <key> <value>    Store a key-value pair
  get <key>            Retrieve a value by key
  delete <key>         Delete a key
  scan <prefix>        Scan keys by prefix
  keys                 List all keys
  stats                Show database statistics
  flush                Force flush memtable to disk
  help                 Show this help message
  exit                 Exit the shell

Examples:
  kvctl> put user:1 "Alice"
  kvctl> get user:1
  kvctl> scan user:
  kvctl> stats
`
	fmt.Println(help)
}

// parseArgs splits a shell command line respecting quoted strings.
// Example: put key "value with spaces" -> ["put", "key", "value with spaces"]
func parseArgs(input string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	escapeNext := false

	for _, r := range input {
		switch {
		case escapeNext:
			current.WriteRune(r)
			escapeNext = false
		case r == '\\' && !inQuotes:
			escapeNext = true
		case r == '"':
			inQuotes = !inQuotes
		case r == ' ' && !inQuotes:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

func init() {
	RootCmd.AddCommand(shellCmd)
}
