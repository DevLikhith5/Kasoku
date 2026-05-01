package commands

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevLikhith5/kasoku/cmd/kvctl/engine"
	"github.com/DevLikhith5/kasoku/cmd/kvctl/output"
	"github.com/spf13/cobra"
)

var (
	pressureOps       int
	pressureThreads   int
	pressureWorkload  string
	pressureKeyRange  int
	pressureValueSize int
	pressureWarmup    int
	pressureDist      string
)

type opType int

const (
	opRead opType = iota
	opWrite
	opScan
	opRMW
)

type opResult struct {
	op      opType
	latency time.Duration
	err     error
}

type workloadDef struct {
	name        string
	description string
	readPct     float64
	scanPct     float64
	rmwPct      float64
	writePct    float64
}

var workloads = map[string]workloadDef{
	"a": {
		name:        "YCSB-A",
		description: "Update heavy (50% read, 50% write)",
		readPct:     0.50,
		writePct:    0.50,
	},
	"b": {
		name:        "YCSB-B",
		description: "Read mostly (95% read, 5% write)",
		readPct:     0.95,
		writePct:    0.05,
	},
	"c": {
		name:        "YCSB-C",
		description: "Read only (100% read)",
		readPct:     1.00,
		writePct:    0.00,
	},
	"d": {
		name:        "YCSB-D",
		description: "Read latest (95% read, 5% insert — zipfian)",
		readPct:     0.95,
		writePct:    0.05,
	},
	"e": {
		name:        "YCSB-E",
		description: "Short ranges (75% scan, 25% insert)",
		scanPct:     0.75,
		writePct:    0.25,
	},
	"f": {
		name:        "YCSB-F",
		description: "Read-modify-write (50% read, 50% RMW)",
		readPct:     0.50,
		rmwPct:      0.50,
	},
	"mixed": {
		name:        "Mixed-80/20",
		description: "Realistic mixed (80% read, 20% write)",
		readPct:     0.80,
		writePct:    0.20,
	},
}

var pressureCmd = &cobra.Command{
	Use:   "pressure",
	Short: "YCSB-style database benchmark",
	Long: `Run realistic database workloads inspired by YCSB (Yahoo! Cloud Serving Benchmark).

Workloads:
  a     YCSB-A: 50% read / 50% write (update heavy)
  b     YCSB-B: 95% read / 5% write (read mostly)
  c     YCSB-C: 100% read
  d     YCSB-D: 95% read / 5% write (zipfian latest)
  e     YCSB-E: 75% scan / 25% insert
  f     YCSB-F: 50% read / 50% read-modify-write
  mixed 80% read / 20% write

Key distributions:
  uniform  Equal probability for all keys
  zipfian  Skewed — a few hot keys get most traffic (default, realistic)

Examples:
  kvctl pressure -w b -n 100000 -t 8 -k 1000000
  kvctl pressure -w a -n 50000 -t 4 -D uniform
  kvctl pressure -w mixed -n 200000 -t 16 -v 1024`,
	RunE: runPressure,
}

func init() {
	pressureCmd.Flags().IntVarP(&pressureOps, "num-ops", "n", 100000, "Total number of operations")
	pressureCmd.Flags().IntVarP(&pressureThreads, "threads", "t", 1, "Number of concurrent threads")
	pressureCmd.Flags().StringVarP(&pressureWorkload, "workload", "w", "b", "Workload type (a,b,c,d,e,f,mixed)")
	pressureCmd.Flags().IntVarP(&pressureKeyRange, "keys", "k", 1000000, "Number of distinct keys")
	pressureCmd.Flags().IntVarP(&pressureValueSize, "value-size", "v", 128, "Value size in bytes")
	pressureCmd.Flags().IntVarP(&pressureWarmup, "warmup", "W", 10000, "Number of keys to pre-load")
	pressureCmd.Flags().StringVarP(&pressureDist, "dist", "D", "zipfian", "Key distribution (uniform, zipfian)")
	RootCmd.AddCommand(pressureCmd)
}

type zipfGen struct {
	rng  *rand.Rand
	zipf *rand.Zipf
	n    int
}

func newZipfGen(seed int64, n int) *zipfGen {
	r := rand.New(rand.NewSource(seed))
	return &zipfGen{
		rng:  r,
		zipf: rand.NewZipf(r, 1.01, 1, uint64(n)-1),
		n:    n,
	}
}

func (z *zipfGen) next() string {
	return fmt.Sprintf("user%d", z.zipf.Uint64())
}

func runPressure(cmd *cobra.Command, args []string) error {
	wl, ok := workloads[pressureWorkload]
	if !ok {
		output.PrintError("Unknown workload: %s\nAvailable: a, b, c, d, e, f, mixed\n", pressureWorkload)
		return fmt.Errorf("unknown workload")
	}

	engineInst, err := engine.GetEngine()
	if err != nil {
		output.PrintError("Failed to open database: %v\n", err)
		return err
	}
	defer engineInst.Close()

	fmt.Println()
	output.PrintInfo("╔════════════════════════════════════════════════════════════╗\n")
	output.PrintInfo("║            KASOKU PRESSURE BENCHMARK (YCSB-style)         ║\n")
	output.PrintInfo("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Println()

	output.PrintInfo("  Workload:     %s — %s\n", wl.name, wl.description)
	output.PrintInfo("  Operations:   %d\n", pressureOps)
	output.PrintInfo("  Threads:      %d\n", pressureThreads)
	output.PrintInfo("  Key range:    %d\n", pressureKeyRange)
	output.PrintInfo("  Value size:   %d bytes\n", pressureValueSize)
	output.PrintInfo("  Distribution: %s\n", pressureDist)
	fmt.Println()

	warmupCount := min(pressureWarmup, pressureKeyRange)
	if warmupCount > 0 {
		output.PrintInfo("Loading %d keys (warmup phase)...\n", warmupCount)
		valueBuf := make([]byte, pressureValueSize)
		for i := 0; i < warmupCount; i++ {
			for j := range valueBuf {
				valueBuf[j] = byte('a' + (j % 26))
			}
			key := fmt.Sprintf("user%d", i)
			if err := engineInst.Put(key, valueBuf); err != nil {
				output.PrintError("Warmup put failed: %v\n", err)
				return err
			}
		}
		output.PrintSuccess("Loaded %d keys\n", warmupCount)
	}

	var readCount, writeCount, scanCount, rmwCount atomic.Int64
	results := make([]opResult, 0, pressureOps)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	totalOps := int64(pressureOps)
	var opsDone atomic.Int64
	barWidth := 50

	output.PrintInfo("Running workload %s with %d threads...\n\n", wl.name, pressureThreads)

	start := time.Now()

	perThread := pressureOps / pressureThreads
	remainder := pressureOps % pressureThreads

	for t := 0; t < pressureThreads; t++ {
		threadOps := perThread
		if t < remainder {
			threadOps++
		}
		if threadOps == 0 {
			continue
		}

		wg.Add(1)
		go func(threadID, n int) {
			defer wg.Done()
			gen := newZipfGen(int64(threadID)+42, pressureKeyRange)

			threadChooseOp := func() opType {
				r := gen.rng.Float64()
				cum := 0.0
				if wl.readPct > 0 {
					cum += wl.readPct
					if r < cum {
						return opRead
					}
				}
				if wl.scanPct > 0 {
					cum += wl.scanPct
					if r < cum {
						return opScan
					}
				}
				if wl.rmwPct > 0 {
					cum += wl.rmwPct
					if r < cum {
						return opRMW
					}
				}
				return opWrite
			}()

			vbuf := make([]byte, pressureValueSize)
			for i := range vbuf {
				vbuf[i] = byte('a' + (i % 26))
			}

			for i := 0; i < n; i++ {
				op := threadChooseOp
				if pressureThreads > 1 {
					r := gen.rng.Float64()
					cum := 0.0
					if wl.readPct > 0 {
						cum += wl.readPct
						if r < cum {
							op = opRead
						} else if wl.scanPct > 0 {
							cum += wl.scanPct
							if r < cum {
								op = opScan
							} else if wl.rmwPct > 0 {
								cum += wl.rmwPct
								if r < cum {
									op = opRMW
								} else {
									op = opWrite
								}
							} else {
								op = opWrite
							}
						} else if wl.rmwPct > 0 {
							cum += wl.rmwPct
							if r < cum {
								op = opRMW
							} else {
								op = opWrite
							}
						} else {
							op = opWrite
						}
					} else if wl.scanPct > 0 {
						cum += wl.scanPct
						if r < cum {
							op = opScan
						} else if wl.rmwPct > 0 {
							cum += wl.rmwPct
							if r < cum {
								op = opRMW
							} else {
								op = opWrite
							}
						} else {
							op = opWrite
						}
					} else if wl.rmwPct > 0 {
						cum += wl.rmwPct
						if r < cum {
							op = opRMW
						} else {
							op = opWrite
						}
					}
				}

				var key string
				if pressureDist == "zipfian" {
					key = gen.next()
				} else {
					key = fmt.Sprintf("user%d", gen.rng.Intn(pressureKeyRange))
				}

				var latency time.Duration
				var opErr error

				switch op {
				case opRead:
					t0 := time.Now()
					_, opErr = engineInst.Get(key)
					latency = time.Since(t0)
					readCount.Add(1)
				case opWrite:
					t0 := time.Now()
					opErr = engineInst.Put(key, vbuf)
					latency = time.Since(t0)
					writeCount.Add(1)
				case opScan:
					t0 := time.Now()
					prefix := key
					if len(prefix) > 4 {
						prefix = prefix[:4]
					}
					_, opErr = engineInst.Scan(prefix)
					latency = time.Since(t0)
					scanCount.Add(1)
				case opRMW:
					t0 := time.Now()
					entry, gerr := engineInst.Get(key)
					if gerr == nil {
						newVal := make([]byte, len(entry.Value)+1)
						copy(newVal, entry.Value)
						opErr = engineInst.Put(key, newVal)
					} else {
						opErr = engineInst.Put(key, vbuf)
					}
					latency = time.Since(t0)
					rmwCount.Add(1)
				}

				resultsMu.Lock()
				results = append(results, opResult{op: op, latency: latency, err: opErr})
				resultsMu.Unlock()

				done := opsDone.Add(1)
				if done%1000 == 0 {
					pct := float64(done) * 100.0 / float64(totalOps)
					elapsed := time.Since(start).Seconds()
					tp := float64(done) / elapsed
					filled := int(pct / 100.0 * float64(barWidth))
					bar := ""
					for b := 0; b < barWidth; b++ {
						if b < filled {
							bar += "█"
						} else {
							bar += "░"
						}
					}
					fmt.Printf("\r  [%s] %5.1f%% — %.0f ops/sec  ", bar, pct, tp)
				}
			}
		}(t, threadOps)
	}

	wg.Wait()
	totalDuration := time.Since(start)

	fullBar := ""
	for i := 0; i < barWidth; i++ {
		fullBar += "█"
	}
	fmt.Printf("\r  [%s] 100%% — done!                       \n", fullBar)
	fmt.Println()

	sort.Slice(results, func(i, j int) bool { return results[i].latency < results[j].latency })

	allLats := make([]time.Duration, len(results))
	var totalLat time.Duration
	var errorCount int64
	var readLats, writeLats, scanLats, rmwLats []time.Duration
	for i, r := range results {
		allLats[i] = r.latency
		totalLat += r.latency
		if r.err != nil {
			errorCount++
		}
		switch r.op {
		case opRead:
			readLats = append(readLats, r.latency)
		case opWrite:
			writeLats = append(writeLats, r.latency)
		case opScan:
			scanLats = append(scanLats, r.latency)
		case opRMW:
			rmwLats = append(rmwLats, r.latency)
		}
	}

	percentile := func(lats []time.Duration, pct float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		idx := int(float64(len(lats)-1) * pct / 100.0)
		return lats[idx]
	}

	throughput := float64(pressureOps) / totalDuration.Seconds()
	var avgLat time.Duration
	if len(results) > 0 {
		avgLat = totalLat / time.Duration(len(results))
	}

	fmt.Println()
	output.PrintSuccess("╔════════════════════════════════════════════════════════════╗\n")
	output.PrintSuccess("║                      RESULTS                               ║\n")
	output.PrintSuccess("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Println()

	output.RenderTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Workload", fmt.Sprintf("%s — %s", wl.name, wl.description)},
			{"Total Ops", fmt.Sprintf("%d", pressureOps)},
			{"Duration", totalDuration.Round(time.Millisecond).String()},
			{"Throughput", fmt.Sprintf("%.0f ops/sec", throughput)},
			{"Errors", fmt.Sprintf("%d (%.2f%%)", errorCount, float64(errorCount)*100/float64(max(pressureOps, 1)))},
			{"", ""},
			{"READ ops", fmt.Sprintf("%d", readCount.Load())},
			{"WRITE ops", fmt.Sprintf("%d", writeCount.Load())},
			{"SCAN ops", fmt.Sprintf("%d", scanCount.Load())},
			{"RMW ops", fmt.Sprintf("%d", rmwCount.Load())},
		},
	)

	fmt.Println()
	output.PrintInfo("Overall Latency Distribution:\n\n")
	output.RenderTable(
		[]string{"Percentile", "Latency"},
		[][]string{
			{"Min", percentile(allLats, 0).String()},
			{"p25", percentile(allLats, 25).String()},
			{"p50 (median)", percentile(allLats, 50).String()},
			{"p75", percentile(allLats, 75).String()},
			{"p90", percentile(allLats, 90).String()},
			{"p95", percentile(allLats, 95).String()},
			{"p99", percentile(allLats, 99).String()},
			{"p99.9", percentile(allLats, 99.9).String()},
			{"Max", percentile(allLats, 100).String()},
			{"Average", avgLat.String()},
		},
	)

	if len(readLats) > 0 {
		fmt.Println()
		output.PrintInfo("READ Latency Distribution:\n\n")
		output.RenderTable(
			[]string{"Percentile", "Latency"},
			[][]string{
				{"p50", percentile(readLats, 50).String()},
				{"p75", percentile(readLats, 75).String()},
				{"p90", percentile(readLats, 90).String()},
				{"p95", percentile(readLats, 95).String()},
				{"p99", percentile(readLats, 99).String()},
				{"p99.9", percentile(readLats, 99.9).String()},
				{"Average", (totalLatFor(readLats) / time.Duration(len(readLats))).String()},
			},
		)
	}

	if len(writeLats) > 0 {
		fmt.Println()
		output.PrintInfo("WRITE Latency Distribution:\n\n")
		output.RenderTable(
			[]string{"Percentile", "Latency"},
			[][]string{
				{"p50", percentile(writeLats, 50).String()},
				{"p75", percentile(writeLats, 75).String()},
				{"p90", percentile(writeLats, 90).String()},
				{"p95", percentile(writeLats, 95).String()},
				{"p99", percentile(writeLats, 99).String()},
				{"p99.9", percentile(writeLats, 99.9).String()},
				{"Average", (totalLatFor(writeLats) / time.Duration(len(writeLats))).String()},
			},
		)
	}

	fmt.Println()
	if readCount.Load() > 0 {
		output.PrintInfo("  READ  throughput: %10.0f ops/sec (%d ops)\n", float64(readCount.Load())/totalDuration.Seconds(), readCount.Load())
	}
	if writeCount.Load() > 0 {
		output.PrintInfo("  WRITE throughput: %10.0f ops/sec (%d ops)\n", float64(writeCount.Load())/totalDuration.Seconds(), writeCount.Load())
	}
	if scanCount.Load() > 0 {
		output.PrintInfo("  SCAN  throughput: %10.0f ops/sec (%d ops)\n", float64(scanCount.Load())/totalDuration.Seconds(), scanCount.Load())
	}
	if rmwCount.Load() > 0 {
		output.PrintInfo("  RMW   throughput: %10.0f ops/sec (%d ops)\n", float64(rmwCount.Load())/totalDuration.Seconds(), rmwCount.Load())
	}

	fmt.Println()
	output.PrintSuccess("Benchmark complete!\n")

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func totalLatFor(lats []time.Duration) time.Duration {
	var total time.Duration
	for _, l := range lats {
		total += l
	}
	return total
}
