package storage

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrashDurability verifies that data persists even if the process is killed with SIGKILL.
// It uses a self-spawning technique to simulate a crash.
func TestCrashDurability(t *testing.T) {
	// If this environment variable is set, we act as the "crashy" child process
	if os.Getenv("BE_CRASHY") == "1" {
		walPath := os.Getenv("WAL_PATH")
		engine, err := NewHashmapEngine(walPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create engine: %v\n", err)
			os.Exit(1)
		}

		for i := 0; i < 1000; i++ {
			err := engine.Put(fmt.Sprintf("key-%d", i), []byte("data"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to put key-%d: %v\n", i, err)
				os.Exit(1)
			}
			
			// Kill ourselves exactly after 500 writes are completed and synced
			if i == 499 {
				// syscall.Kill(pid, SIGKILL) is the same as 'kill -9'
				// This process dies immediately.
				_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
			}
		}
		return
	}

	// --- Parent Test Logic ---
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "crash_test.wal")

	// Run this same test binary but with the BE_CRASHY flag
	// This invokes the block above in a separate process.
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashDurability")
	cmd.Env = append(os.Environ(), "BE_CRASHY=1", "WAL_PATH="+walPath)

	// cmd.Run will return an error because the process was killed (exit status -1/signal 9)
	err := cmd.Run()
	require.Error(t, err, "Expected process to be killed by SIGKILL")

	// Now, reopen the engine and see if the 500 records survived.
	// Since every Put() calls fsync, they must be there.
	engine, err := NewHashmapEngine(walPath)
	require.NoError(t, err)
	defer engine.Close()

	keys, _ := engine.Keys()
	t.Logf("Successfully recovered %d keys from crashed WAL", len(keys))

	// We expect at least 500 keys. 
	// It could be 501 if the 501st write reached the kernel but the process died before Sync returned,
	// though usually it will be exactly 500 because of the sequential nature.
	assert.GreaterOrEqual(t, len(keys), 500)
	
	// Verify the last synced key
	val, err := engine.Get("key-499")
	require.NoError(t, err)
	assert.Equal(t, "data", string(val.Value))
}
