package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DevLikhith5/kasoku/internal/config"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/fatih/color"
	"github.com/manifoldco/promptui"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	dataDir   string
	outputFmt string
	verbose   bool
	walSyncMs int // WAL sync interval in milliseconds
	cfg       *config.Config

	// Colors
	successColor = color.New(color.FgGreen, color.Bold)
	errorColor   = color.New(color.FgRed, color.Bold)
	infoColor    = color.New(color.FgCyan, color.Bold)
	warnColor    = color.New(color.FgYellow, color.Bold)
	keyColor     = color.New(color.FgBlue, color.Bold)
	valueColor   = color.New(color.FgWhite)
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		errorColor.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "kvctl",
	Short: "Kasoku KV Store CLI",
	Long: `Kasoku - High Performance Key-Value Store

A modern LSM-tree based storage engine with:
  • Write-Ahead Logging (WAL)
  • Bloom Filters
  • Automatic Compaction
  • Crash Recovery`,
	Example: `  kvctl put user:1 "Alice"
  kvctl get user:1
  kvctl scan user:
  kvctl shell`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Store a key-value pair",
	Long:  "Store a key-value pair in the database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		key := args[0]
		value := args[1]

		if err := validateKey(key); err != nil {
			errorColor.Printf("Invalid key: %v\n", err)
			return
		}

		if err := engine.Put(key, []byte(value)); err != nil {
			errorColor.Printf("Put failed: %v\n", err)
			return
		}

		successColor.Printf("OK\n")
		if verbose {
			fmt.Printf("  Stored '%s' (%d bytes)\n", keyColor.Sprint(key), len(value))
		}
	},
}

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Retrieve a value by key",
	Long:  "Retrieve a value by key from the database",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		key := args[0]
		entry, err := engine.Get(key)

		if err == storage.ErrKeyNotFound {
			warnColor.Printf("Key '%s' not found\n", key)
			return
		}
		if err != nil {
			errorColor.Printf("Get failed: %v\n", err)
			return
		}

		if entry.Tombstone {
			warnColor.Printf("Key '%s' was deleted\n", key)
			return
		}

		switch outputFmt {
		case "json":
			output := map[string]interface{}{
				"key":       entry.Key,
				"value":     string(entry.Value),
				"version":   entry.Version,
				"timestamp": entry.TimeStamp.Format(time.RFC3339),
			}
			prettyPrint(output)
		case "table":
			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{"Field", "Value"})
			table.Append([]string{"Key", entry.Key})
			table.Append([]string{"Value", string(entry.Value)})
			table.Append([]string{"Version", fmt.Sprintf("%d", entry.Version)})
			table.Append([]string{"Timestamp", entry.TimeStamp.Format("2006-01-02 15:04:05")})
			table.Render()
		default:
			keyColor.Print("Key:     ")
			fmt.Println(entry.Key)
			valueColor.Print("Value:   ")
			fmt.Println(string(entry.Value))
			fmt.Printf("Version:  %d\n", entry.Version)
			fmt.Printf("Time:     %s\n", entry.TimeStamp.Format("2006-01-02 15:04:05"))
		}
	},
}

var deleteCmd = &cobra.Command{
	Use:     "delete <key>",
	Aliases: []string{"del", "rm"},
	Short:   "Delete a key",
	Long:    "Delete a key from the database",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		key := args[0]

		if err := engine.Delete(key); err != nil {
			if err == storage.ErrKeyNotFound {
				warnColor.Printf("Key '%s' not found\n", key)
				return
			}
			errorColor.Printf("Delete failed: %v\n", err)
			return
		}

		successColor.Printf("Deleted '%s'\n", key)
	},
}

var scanCmd = &cobra.Command{
	Use:   "scan <prefix>",
	Short: "Scan entries by prefix",
	Long:  "Scan and display all entries with the given prefix",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		prefix := args[0]
		entries, err := engine.Scan(prefix)
		if err != nil {
			errorColor.Printf("Scan failed: %v\n", err)
			return
		}

		if len(entries) == 0 {
			warnColor.Printf("No keys found with prefix '%s'\n", prefix)
			return
		}

		infoColor.Printf("Found %d entries:\n\n", len(entries))

		switch outputFmt {
		case "json":
			prettyPrint(entries)
		case "table":
			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{"Key", "Value", "Version", "Timestamp"})

			for _, entry := range entries {
				table.Append([]string{
					entry.Key,
					truncate(string(entry.Value), 50),
					fmt.Sprintf("%d", entry.Version),
					entry.TimeStamp.Format("2006-01-02 15:04:05"),
				})
			}
			table.Render()
		default:
			for _, entry := range entries {
				keyColor.Printf("%s", entry.Key)
				fmt.Print(" => ")
				valueColor.Printf("%s\n", string(entry.Value))
			}
		}
	},
}

var keysCmd = &cobra.Command{
	Use:   "keys [prefix]",
	Short: "List all keys",
	Long:  "List all keys, optionally filtered by prefix",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		var keys []string
		if len(args) > 0 {
			entries, err := engine.Scan(args[0])
			if err != nil {
				errorColor.Printf("Scan failed: %v\n", err)
				return
			}
			keys = make([]string, len(entries))
			for i, e := range entries {
				keys[i] = e.Key
			}
		} else {
			keys, err = engine.Keys()
			if err != nil {
				errorColor.Printf("Failed to get keys: %v\n", err)
				return
			}
		}

		if len(keys) == 0 {
			warnColor.Println("No keys found")
			return
		}

		infoColor.Printf("Total keys: %d\n\n", len(keys))

		switch outputFmt {
		case "json":
			prettyPrint(keys)
		case "table":
			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{"#"})
			for i, key := range keys {
				table.Append([]string{fmt.Sprintf("%d. %s", i+1, key)})
			}
			table.Render()
		default:
			for i, key := range keys {
				fmt.Printf("%3d. ", i+1)
				keyColor.Println(key)
			}
		}
	},
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show database statistics",
	Long:  "Display database statistics and metrics",
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		stats := engine.Stats()

		switch outputFmt {
		case "json":
			prettyPrint(map[string]interface{}{
				"key_count":      stats.KeyCount,
				"memory_bytes":   stats.MemBytes,
				"disk_bytes":     stats.DiskBytes,
				"bloom_fp_rate":  stats.BloomFPRate,
				"memory_human":   formatBytes(uint64(stats.MemBytes)),
				"disk_human":     formatBytes(uint64(stats.DiskBytes)),
			})
		default:
			fmt.Println()
			infoColor.Println("Database Statistics")
			fmt.Println(strings.Repeat("─", 50))

			table := tablewriter.NewWriter(os.Stdout)

			table.Append([]string{"Total Keys:", fmt.Sprintf("%d", stats.KeyCount)})
			table.Append([]string{"Memory Usage:", formatBytes(uint64(stats.MemBytes))})
			table.Append([]string{"Disk Usage:", formatBytes(uint64(stats.DiskBytes))})
			table.Append([]string{"Bloom FP Rate:", fmt.Sprintf("%.4f%%", stats.BloomFPRate*100)})
			table.Render()
			fmt.Println()
		}
	},
}

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Interactive shell",
	Long:  "Start an interactive shell session (Redis-compatible)",
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		successColor.Println("Kasoku Interactive Shell")
		// fmt.Println("Redis-compatible commands: SET, GET, DEL, EXISTS, KEYS, SCAN, TTL, INFO, PING, FLUSHALL, DUMP, CLEAR, EXIT")
		// fmt.Println("Legacy commands: put, get, del, scan, keys, stats")
		// fmt.Println(strings.Repeat("─", 50))

		for {
			prompt := promptui.Prompt{
				Label: "127.0.0.1:6379",
			}

			input, err := prompt.Run()
			if err != nil {
				fmt.Println()
				return
			}

			parts := strings.Fields(input)
			if len(parts) == 0 {
				continue
			}

			// Convert command to uppercase for Redis compatibility
			cmd := strings.ToUpper(parts[0])

			switch cmd {
			case "EXIT", "QUIT", "Q":
				successColor.Println("Goodbye!")
				return
			case "CLEAR", "CLS":
				fmt.Print("\033[H\033[2J")

			// Redis PING command
			case "PING":
				if len(parts) > 1 {
					// PING with argument returns the argument
					fmt.Println(parts[1])
				} else {
					fmt.Println("PONG")
				}

			// Redis SET command
			case "SET":
				if len(parts) < 3 {
					warnColor.Println("(error) ERR wrong number of arguments for 'SET' command")
					continue
				}
				key := parts[1]
				value := strings.Join(parts[2:], " ")

				// Check for EX/NX/XX options (simplified - just consume them)
				for i := 3; i < len(parts); i++ {
					opt := strings.ToUpper(parts[i])
					if opt == "EX" || opt == "PX" {
						i++ // skip the value
					}
					// NX/XX are ignored for now
				}

				if err := engine.Put(key, []byte(value)); err != nil {
					errorColor.Printf("(error) %v\n", err)
				} else {
					fmt.Println("OK")
				}

			// Redis GET command
			case "GET":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'GET' command")
					continue
				}
				entry, err := engine.Get(parts[1])
				if err == storage.ErrKeyNotFound {
					fmt.Println("(nil)")
				} else if err != nil {
					errorColor.Printf("(error) %v\n", err)
				} else if entry.Tombstone {
					fmt.Println("(nil)")
				} else {
					fmt.Printf("\"%s\"\n", string(entry.Value))
				}

			// Redis DEL command
			case "DEL", "UNLINK":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'DEL' command")
					continue
				}
				deleted := 0
				for _, key := range parts[1:] {
					if err := engine.Delete(key); err == nil {
						deleted++
					}
				}
				fmt.Printf("(integer) %d\n", deleted)

			// Redis EXISTS command
			case "EXISTS":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'EXISTS' command")
					continue
				}
				count := 0
				for _, key := range parts[1:] {
					entry, err := engine.Get(key)
					if err == nil && !entry.Tombstone {
						count++
					}
				}
				fmt.Printf("(integer) %d\n", count)

			// Redis KEYS command (pattern matching with * wildcard)
			case "KEYS":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'KEYS' command")
					continue
				}
				pattern := parts[1]
				keys, err := engine.Keys()
				if err != nil {
					errorColor.Printf("(error) %v\n", err)
					continue
				}

				// Simple pattern matching: * matches any sequence
				var matched []string
				for _, key := range keys {
					if matchPattern(key, pattern) {
						matched = append(matched, key)
					}
				}

				if len(matched) == 0 {
					fmt.Println("(empty array)")
				} else {
					for i, key := range matched {
						fmt.Printf("%d) \"%s\"\n", i+1, key)
					}
				}

			// Redis SCAN command (simplified)
			case "SCAN":
				cursor := 0
				pattern := "*"
				count := 10

				if len(parts) > 1 {
					fmt.Sscanf(parts[1], "%d", &cursor)
				}
				for i := 2; i < len(parts); i++ {
					opt := strings.ToUpper(parts[i])
					if opt == "MATCH" && i+1 < len(parts) {
						pattern = parts[i+1]
						i++
					} else if opt == "COUNT" && i+1 < len(parts) {
						fmt.Sscanf(parts[i+1], "%d", &count)
						i++
					}
				}

				keys, err := engine.Keys()
				if err != nil {
					errorColor.Printf("(error) %v\n", err)
					continue
				}

				var matched []string
				for _, key := range keys {
					if matchPattern(key, pattern) {
						matched = append(matched, key)
					}
				}

				// Return cursor "0" to indicate iteration complete (simplified)
				fmt.Println("0")
				for _, key := range matched {
					fmt.Printf("\"%s\"\n", key)
				}

			// Redis TTL command (returns -2 as TTL is not implemented)
			case "TTL", "PTTL":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'TTL' command")
					continue
				}
				entry, err := engine.Get(parts[1])
				if err == storage.ErrKeyNotFound {
					fmt.Println("(integer) -2")
				} else if err != nil {
					errorColor.Printf("(error) %v\n", err)
				} else if entry.Tombstone {
					fmt.Println("(integer) -2")
				} else {
					// TTL not implemented, return -1 (no expiry)
					fmt.Println("(integer) -1")
				}

			// Redis INFO command
			case "INFO":
				stats := engine.Stats()
				fmt.Println("# Server")
				fmt.Println("redis_version:7.0.0")
				fmt.Println("redis_mode:standalone")
				fmt.Println("os:" + os.Getenv("GOOS"))
				fmt.Println()
				fmt.Println("# Stats")
				fmt.Printf("total_connections_received:%d\n", stats.KeyCount)
				fmt.Printf("total_keys:%d\n", stats.KeyCount)
				fmt.Println()
				fmt.Println("# Memory")
				fmt.Printf("used_memory:%d\n", stats.MemBytes)
				fmt.Printf("used_memory_human:%s\n", formatBytes(uint64(stats.MemBytes)))
				fmt.Printf("used_memory_rss:%d\n", stats.MemBytes)
				fmt.Println()
				fmt.Println("# Keyspace")
				fmt.Printf("db0:keys=%d,expires=0,avg_ttl=0\n", stats.KeyCount)

			// Redis FLUSHALL/FLUSHDB command
			case "FLUSHALL", "FLUSHDB":
				keys, err := engine.Keys()
				if err != nil {
					errorColor.Printf("(error) %v\n", err)
					continue
				}
				deleted := 0
				for _, key := range keys {
					if err := engine.Delete(key); err == nil {
						deleted++
					}
				}
				fmt.Println("OK")

			// Redis DUMP command (shows key info)
			case "DUMP":
				if len(parts) < 2 {
					warnColor.Println("(error) ERR wrong number of arguments for 'DUMP' command")
					continue
				}
				entry, err := engine.Get(parts[1])
				if err == storage.ErrKeyNotFound {
					fmt.Println("(nil)")
				} else if err != nil {
					errorColor.Printf("(error) %v\n", err)
				} else if entry.Tombstone {
					fmt.Println("(nil)")
				} else {
					fmt.Printf("\"%s\"\n", string(entry.Value))
					fmt.Printf("(version: %d, timestamp: %s)\n", entry.Version, entry.TimeStamp.Format("2006-01-02 15:04:05"))
				}

			// Legacy commands (lowercase also works)
			case "PUT":
				if len(parts) < 3 {
					warnColor.Println("Usage: put <key> <value>")
					continue
				}
				if err := engine.Put(parts[1], []byte(strings.Join(parts[2:], " "))); err != nil {
					errorColor.Printf("Error: %v\n", err)
				} else {
					successColor.Println("OK")
				}

			// Legacy GET (already handled above, but lowercase fallback)
			case "get":
				if len(parts) < 2 {
					warnColor.Println("Usage: get <key>")
					continue
				}
				entry, err := engine.Get(parts[1])
				if err == storage.ErrKeyNotFound {
					warnColor.Println("(nil)")
				} else if err != nil {
					errorColor.Printf("Error: %v\n", err)
				} else if entry.Tombstone {
					warnColor.Println("(deleted)")
				} else {
					valueColor.Printf("\"%s\"\n", string(entry.Value))
				}

			// Legacy DEL (already handled above, but lowercase fallback)
			case "del", "delete", "rm":
				if len(parts) < 2 {
					warnColor.Println("Usage: del <key>")
					continue
				}
				if err := engine.Delete(parts[1]); err != nil {
					errorColor.Printf("Error: %v\n", err)
				} else {
					successColor.Println("OK")
				}

			// Legacy PUT (already handled above, but lowercase fallback)
			case "put":
				if len(parts) < 3 {
					warnColor.Println("Usage: put <key> <value>")
					continue
				}
				if err := engine.Put(parts[1], []byte(strings.Join(parts[2:], " "))); err != nil {
					errorColor.Printf("Error: %v\n", err)
				} else {
					successColor.Println("OK")
				}

			default:
				warnColor.Printf("(error) ERR unknown command '%s'\n", parts[0])
			}
		}
	},
}

var flushCmd = &cobra.Command{
	Use:   "flush",
	Short: "Force flush memtable",
	Long:  "Manually flush the active memtable to SSTable",
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		infoColor.Println("Flushing memtable...")
		if err := engine.Flush(); err != nil {
			errorColor.Printf("Flush failed: %v\n", err)
			return
		}
		successColor.Println("Memtable flushed to SSTable")
	},
}

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Trigger compaction",
	Long:  "Manually trigger LSM compaction",
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		infoColor.Println("Triggering compaction...")
		engine.TriggerCompaction()
		successColor.Println("Compaction triggered (runs in background)")
	},
}

var importCmd = &cobra.Command{
	Use:   "import <file.json>",
	Short: "Import data from JSON",
	Long:  "Import key-value pairs from a JSON file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		filename := args[0]
		data, err := os.ReadFile(filename)
		if err != nil {
			errorColor.Printf("Read failed: %v\n", err)
			return
		}

		var kvmap map[string]string
		if err := json.Unmarshal(data, &kvmap); err != nil {
			errorColor.Printf("Parse failed: %v\n", err)
			return
		}

		count := 0
		for key, value := range kvmap {
			if err := engine.Put(key, []byte(value)); err != nil {
				warnColor.Printf("Warning: failed to put %s: %v\n", key, err)
				continue
			}
			count++
		}

		successColor.Printf("Imported %d keys from %s\n", count, filename)
	},
}

var exportCmd = &cobra.Command{
	Use:   "export <file.json>",
	Short: "Export data to JSON",
	Long:  "Export all key-value pairs to a JSON file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		filename := args[0]
		keys, err := engine.Keys()
		if err != nil {
			errorColor.Printf("Failed to get keys: %v\n", err)
			return
		}

		output := make(map[string]string)
		for _, key := range keys {
			entry, err := engine.Get(key)
			if err != nil || entry.Tombstone {
				continue
			}
			output[key] = string(entry.Value)
		}

		data, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			errorColor.Printf("Marshal failed: %v\n", err)
			return
		}

		if err := os.WriteFile(filename, data, 0644); err != nil {
			errorColor.Printf("Write failed: %v\n", err)
			return
		}

		successColor.Printf("Exported %d keys to %s\n", len(output), filename)
	},
}

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run benchmarks",
	Long:  "Run performance benchmarks",
	Run: func(cmd *cobra.Command, args []string) {
		engine, err := getEngine()
		if err != nil {
			errorColor.Printf("Failed to open database: %v\n", err)
			return
		}
		defer engine.Close()

		const benchCount = 10000

		fmt.Println()
		infoColor.Printf("Running benchmarks (%d ops)...\n", benchCount)
		fmt.Println(strings.Repeat("─", 50))

		// Write benchmark
		start := time.Now()
		for i := 0; i < benchCount; i++ {
			key := fmt.Sprintf("bench:key:%06d", i)
			value := fmt.Sprintf("value:%d", i)
			engine.Put(key, []byte(value))
		}
		writeDuration := time.Since(start)

		// Read benchmark
		start = time.Now()
		for i := 0; i < benchCount; i++ {
			key := fmt.Sprintf("bench:key:%06d", i)
			engine.Get(key)
		}
		readDuration := time.Since(start)

		fmt.Println()
		fmt.Println("Results:")
		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Operation", "Ops", "Duration", "Ops/sec"})
		table.Append([]string{
			"Write",
			fmt.Sprintf("%d", benchCount),
			writeDuration.Round(time.Millisecond).String(),
			fmt.Sprintf("%.0f", float64(benchCount)/writeDuration.Seconds()),
		})
		table.Append([]string{
			"Read",
			fmt.Sprintf("%d", benchCount),
			readDuration.Round(time.Millisecond).String(),
			fmt.Sprintf("%.0f", float64(benchCount)/readDuration.Seconds()),
		})
		table.Render()

		// Cleanup
		fmt.Println()
		warnColor.Println("Cleaning up benchmark data...")
		for i := 0; i < benchCount; i++ {
			key := fmt.Sprintf("bench:key:%06d", i)
			engine.Delete(key)
		}
		successColor.Println("Done")
	},
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Configuration file (YAML)")
	rootCmd.PersistentFlags().StringVarP(&dataDir, "dir", "d", "", "Data directory (overrides config)")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "text", "Output format (text, json, table)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	rootCmd.PersistentFlags().IntVar(&walSyncMs, "wal-sync", 0, "WAL fsync interval in ms (0=sync on every write, 100=every 100ms)")

	// Add commands
	rootCmd.AddCommand(putCmd, getCmd, deleteCmd, scanCmd, keysCmd, statsCmd, shellCmd, flushCmd, compactCmd, importCmd, exportCmd, benchCmd)
}

func loadConfig() error {
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Override data dir if specified
	if dataDir != "" {
		cfg.DataDir = dataDir
	}

	return nil
}

func getEngine() (*lsmengine.LSMEngine, error) {
	if cfg == nil {
		if err := loadConfig(); err != nil {
			return nil, err
		}
	}

	// Use background sync if walSyncMs is set
	if walSyncMs > 0 {
		return lsmengine.NewLSMEngineWithConfig(cfg.DataDir, lsmengine.LSMConfig{
			WALSyncInterval: time.Duration(walSyncMs) * time.Millisecond,
		})
	}

	return lsmengine.NewLSMEngine(cfg.DataDir)
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > storage.MaxKeyLen {
		return fmt.Errorf("key too long (max %d bytes)", storage.MaxKeyLen)
	}
	return nil
}

func validateValue(value string) error {
	if len(value) > storage.MaxValueLen {
		return fmt.Errorf("value too long (max %d bytes)", storage.MaxValueLen)
	}
	return nil
}

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return "" // Not enough space for content + "..."
	}
	return s[:maxLen-3] + "..."
}

func prettyPrint(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// matchPattern matches a key against a Redis-style pattern with * wildcard
// e.g., "user:*" matches "user:123", "*key*" matches "mykey123"
func matchPattern(key, pattern string) bool {
	return simpleMatch(key, pattern)
}

// simpleMatch performs glob-style matching
func simpleMatch(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern == "*" {
		return true
	}

	// Handle ? wildcard (single character)
	if containsRune(pattern, '?') || containsRune(pattern, '*') {
		return globMatch(s, pattern)
	}

	// No wildcards - exact match
	return s == pattern
}

// containsRune checks if a string contains a specific rune
func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}

// globMatch performs glob-style pattern matching with * and ?
func globMatch(s, pattern string) bool {
	// Convert pattern to segments separated by *
	segments := splitPattern(pattern)

	pos := 0
	for i, seg := range segments {
		if seg == "" {
			continue
		}

		if len(segments) == 1 && !strings.ContainsRune(pattern, '*') {
			// No wildcards at all - exact match with ? support
			return matchExactSegment(s, seg)
		} else if i == 0 && !strings.HasPrefix(pattern, "*") {
			// First segment must match at start
			if !matchSegment(s[pos:], seg) {
				return false
			}
			pos += segLen(seg)
		} else if i == len(segments)-1 && !strings.HasSuffix(pattern, "*") {
			// Last segment must match at end
			if len(s) < segLen(seg) {
				return false
			}
			if !matchSegment(s[len(s)-segLen(seg):], seg) {
				return false
			}
		} else {
			// Middle segment - find first match
			found := false
			for pos <= len(s)-segLen(seg) {
				if matchSegment(s[pos:pos+segLen(seg)], seg) {
					pos += segLen(seg)
					found = true
					break
				}
				pos++
			}
			if !found {
				return false
			}
		}
	}

	return true
}

// matchExactSegment matches a segment exactly (no extra chars allowed)
func matchExactSegment(s, seg string) bool {
	// Count how many characters we need to match
	needed := 0
	for range seg {
		needed++
	}

	// String length must match exactly
	if len(s) != needed {
		return false
	}

	for i, r := range seg {
		if r == '?' {
			continue // ? matches any character
		}
		if s[i] != byte(r) {
			return false
		}
	}
	return true
}

// splitPattern splits pattern by * keeping empty segments
func splitPattern(pattern string) []string {
	var segments []string
	current := ""
	for _, r := range pattern {
		if r == '*' {
			segments = append(segments, current)
			current = ""
		} else {
			current += string(r)
		}
	}
	segments = append(segments, current)
	return segments
}

// segLen returns the length of a segment (number of characters to match)
func segLen(seg string) int {
	count := 0
	for _, r := range seg {
		if r == '?' {
			count++
		} else {
			count++
		}
	}
	return count
}

// matchSegment matches a segment against a string (? matches any single char)
func matchSegment(s, seg string) bool {
	// Count how many characters we need to match
	needed := 0
	for range seg {
		needed++
	}

	// String must be at least as long as segment requires
	if len(s) < needed {
		return false
	}

	for i, r := range seg {
		if r == '?' {
			continue // ? matches any character
		}
		if i >= len(s) || s[i] != byte(r) {
			return false
		}
	}
	return true
}
