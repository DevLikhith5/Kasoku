package commands

import (
	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var flushCmd = &cobra.Command{
	Use:   "flush",
	Short: "Force flush memtable to disk",
	Long:  "Force flush the active memtable to disk as an SSTable",
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		if err := engineInst.Flush(); err != nil {
			output.PrintError("Flush failed: %v\n", err)
			return err
		}

		output.PrintSuccess("OK - Memtable flushed to disk\n")
		return nil
	},
}

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Trigger compaction",
	Long:  "Manually trigger LSM compaction",
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		output.PrintInfo("Triggering compaction...\n")
		engineInst.TriggerCompaction()
		output.PrintSuccess("Compaction triggered (runs in background)\n")

		return nil
	},
}

func init() {
	RootCmd.AddCommand(flushCmd)
	RootCmd.AddCommand(compactCmd)
}
