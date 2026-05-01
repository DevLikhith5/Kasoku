package commands

import (
	"fmt"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show database statistics",
	Long:  "Show database statistics including size, key count, and memory usage",
	RunE: func(cmd *cobra.Command, args []string) error {
		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		stats := engineInst.Stats()

		// Output based on format
		switch outputFmt {
		case "json":
			data := map[string]interface{}{
				"key_count":      stats.KeyCount,
				"disk_bytes":     stats.DiskBytes,
				"mem_bytes":      stats.MemBytes,
				"bloom_fp_rate":  stats.BloomFPRate,
				"disk_formatted": output.FormatBytes(stats.DiskBytes),
				"mem_formatted":  output.FormatBytes(stats.MemBytes),
			}
			output.PrettyPrint(data)

		default: // table
			table := output.NewTable()
			table.Header([]string{"Metric", "Value"})
			table.Append([]string{"Key Count", fmt.Sprintf("%d", stats.KeyCount)})
			table.Append([]string{"Disk Usage", output.FormatBytes(stats.DiskBytes)})
			table.Append([]string{"Memory Usage", output.FormatBytes(stats.MemBytes)})
			table.Append([]string{"Bloom Filter FP Rate", fmt.Sprintf("%.2f%%", stats.BloomFPRate*100)})
			table.Render()
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(statsCmd)
}
