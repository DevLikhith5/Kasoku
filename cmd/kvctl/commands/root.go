package commands

import (
	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/internal/config"
	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	dataDir   string
	outputFmt string
	verbose   bool
	walSyncMs int
	cfg       *config.Config
)

var RootCmd = &cobra.Command{
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
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initEngine()
	},
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() error {
	return RootCmd.Execute()
}

func initEngine() error {
	if err := loadConfig(); err != nil {
		return err
	}

	engine.SetConfig(cfg)
	engine.SetWALSyncMs(walSyncMs)

	return nil
}

func loadConfig() error {
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return err
	}

	// Override data dir if specified
	if dataDir != "" {
		cfg.DataDir = dataDir
	}

	return nil
}

func init() {
	// Global flags
	RootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Configuration file (YAML)")
	RootCmd.PersistentFlags().StringVarP(&dataDir, "dir", "d", "", "Data directory (overrides config)")
	RootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "text", "Output format (text, json, table)")
	RootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	RootCmd.PersistentFlags().IntVar(&walSyncMs, "wal-sync", 0, "WAL fsync interval in ms")
}
