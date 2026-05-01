package commands

import (
	"encoding/json"
	"fmt"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show current configuration",
	Long:  "Display the configuration currently loaded by the Kasoku CLI",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg == nil {
			output.PrintError("Config not loaded\n")
			return nil
		}

		switch outputFmt {
		case "json":
			bytes, err := json.MarshalIndent(cfg, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(bytes))
		default: // table or text
			table := output.NewTable()
			table.Header([]string{"Section", "Configuration"})

			// General Settings
			table.Append([]string{"General", fmt.Sprintf("DataDir: %s\nPort: %d\nHTTPPort: %d\nLogLevel: %s",
				cfg.DataDir, cfg.Port, cfg.HTTPPort, cfg.LogLevel)})

			// LSM Settings
			table.Append([]string{"LSM Engine", fmt.Sprintf("Levels: %d\nLevelRatio: %.2f\nL0BaseSize: %s",
				cfg.LSM.Levels, cfg.LSM.LevelRatio, output.FormatBytes(cfg.LSM.L0BaseSize))})

			// Memory Settings
			table.Append([]string{"Memory", fmt.Sprintf("MemTableSize: %s\nMaxMemtableBytes: %s\nBloomFPRate: %.2f%%\nBlockCacheSize: %s",
				output.FormatBytes(cfg.Memory.MemTableSize), output.FormatBytes(cfg.Memory.MaxMemtableBytes), cfg.Memory.BloomFPRate*100, output.FormatBytes(cfg.Memory.BlockCacheSize))})

			// Compaction Settings
			table.Append([]string{"Compaction", fmt.Sprintf("Threshold: %d\nMaxConcurrent: %d\nL0SizeThreshold: %s",
				cfg.Compaction.Threshold, cfg.Compaction.MaxConcurrent, output.FormatBytes(cfg.Compaction.L0SizeThreshold))})

			// WAL Settings
			table.Append([]string{"WAL", fmt.Sprintf("Sync: %v\nSyncInterval: %s\nMaxFileSize: %s",
				cfg.WAL.Sync, cfg.WAL.SyncInterval, output.FormatBytes(cfg.WAL.MaxFileSize))})

			// Cluster Settings
			table.Append([]string{"Cluster", fmt.Sprintf("Enabled: %v\nNodeID: %s\nNodeAddr: %s\nPeers: %v\nReplicationFactor: %d",
				cfg.Cluster.Enabled, cfg.Cluster.NodeID, cfg.Cluster.NodeAddr, cfg.Cluster.Peers, cfg.Cluster.ReplicationFactor)})

			table.Render()
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(configCmd)
}
