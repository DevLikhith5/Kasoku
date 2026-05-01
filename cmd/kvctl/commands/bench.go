package commands

import (
	"fmt"
	"strconv"
	"time"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var (
	benchCount int
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run benchmarks",
	Long:  "Run performance benchmarks for write and read operations",
	RunE: func(cmd *cobra.Command, args []string) error {
		output.PrintInfo("Running benchmarks (%d ops)...\n", benchCount)
		fmt.Println()

		engineInst, err := engine.GetEngine()
		if err != nil {
			output.PrintError("Failed to open database: %v\n", err)
			return err
		}
		defer engineInst.Close()

		// Pre-allocate key/value buffers to reduce allocations
		keyBuf := make([]byte, 0, 64)
		valueBuf := make([]byte, 0, 128)

		// Write benchmark
		output.PrintInfo("Writing %d entries...\n", benchCount)
		start := time.Now()
		for i := 0; i < benchCount; i++ {
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, "bench:key:"...)
			keyBuf = strconv.AppendInt(keyBuf, int64(i), 10)

			valueBuf = valueBuf[:0]
			valueBuf = append(valueBuf, "value_"...)
			valueBuf = strconv.AppendInt(valueBuf, int64(i), 10)

			if err := engineInst.Put(string(keyBuf), valueBuf); err != nil {
				output.PrintError("Write failed: %v\n", err)
				return err
			}
		}
		writeDuration := time.Since(start)

		// Read benchmark
		output.PrintInfo("Reading %d entries...\n", benchCount)
		start = time.Now()
		for i := 0; i < benchCount; i++ {
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, "bench:key:"...)
			keyBuf = strconv.AppendInt(keyBuf, int64(i), 10)

			_, err := engineInst.Get(string(keyBuf))
			if err != nil {
				output.PrintError("Read failed: %v\n", err)
				return err
			}
		}
		readDuration := time.Since(start)

		// Calculate ops/sec
		writeOpsSec := float64(benchCount) / writeDuration.Seconds()
		readOpsSec := float64(benchCount) / readDuration.Seconds()

		// Print results
		fmt.Println()
		output.PrintSuccess("Results:\n")
		output.RenderTable(
			[]string{"Operation", "Ops", "Duration", "Ops/sec"},
			[][]string{
				{"Write", fmt.Sprintf("%d", benchCount), writeDuration.Round(time.Millisecond).String(), fmt.Sprintf("%.0f", writeOpsSec)},
				{"Read", fmt.Sprintf("%d", benchCount), readDuration.Round(time.Millisecond).String(), fmt.Sprintf("%.0f", readOpsSec)},
			},
		)

		// Cleanup
		fmt.Println()
		output.PrintWarn("Cleaning up benchmark data...\n")
		for i := 0; i < benchCount; i++ {
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, "bench:key:"...)
			keyBuf = strconv.AppendInt(keyBuf, int64(i), 10)
			if err := engineInst.Delete(string(keyBuf)); err != nil {
				output.PrintWarn("Warning: failed to delete key %d: %v\n", i, err)
			}
		}
		output.PrintSuccess("Done\n")

		return nil
	},
}

func init() {
	benchCmd.Flags().IntVar(&benchCount, "count", 10000, "Number of operations")
	RootCmd.AddCommand(benchCmd)
}
