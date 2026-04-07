//go:build ignore

// Run with: go run demo/replication_demo.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"
)

func main() {
	// Clean old data
	os.RemoveAll("/tmp/kasoku-demo")

	fmt.Println("=== Kasoku 3-Node Replication Demo ===")
	fmt.Println()

	// Start 3 nodes as separate processes
	nodes := []struct {
		id   string
		port int
	}{
		{"node-1", 18081},
		{"node-2", 18082},
		{"node-3", 18083},
	}

	var cmds []*exec.Cmd
	for _, n := range nodes {
		cmd := exec.Command("go", "run", "cmd/server/main.go",
			"--node-id", n.id,
			"--port", fmt.Sprintf("%d", n.port),
			"--data-dir", fmt.Sprintf("/tmp/kasoku-demo/%s", n.id),
			"--cluster-enabled",
			"--replication-factor", "3",
			"--quorum-size", "2",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Start()
		cmds = append(cmds, cmd)
		fmt.Printf("✅ Started %s on :%d (pid=%d)\n", n.id, n.port, cmd.Process.Pid)
	}

	// Cleanup on exit
	defer func() {
		for _, c := range cmds {
			c.Process.Kill()
		}
		os.RemoveAll("/tmp/kasoku-demo")
	}()

	fmt.Println()
	fmt.Println("Waiting for servers to be ready...")
	time.Sleep(3 * time.Second)

	nodeURLs := []string{
		"http://localhost:18081",
		"http://localhost:18082",
		"http://localhost:18083",
	}

	// DEMO 1: Write to node-1, read from all
	fmt.Println()
	fmt.Println("--- Demo 1: Write user:1=Alice to node-1 ---")

	http.Post(nodeURLs[0]+"/keys/user:1", "application/json",
		bytes.NewReader([]byte(`{"value":"Alice"}`)))

	time.Sleep(500 * time.Millisecond)

	for i, url := range nodeURLs {
		resp, err := http.Get(url + "/keys/user:1")
		if err != nil {
			fmt.Printf("❌ Node %d unreachable\n", i+1)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var r map[string]any
		json.Unmarshal(body, &r)
		if v, ok := r["value"]; ok {
			fmt.Printf("✅ Node %d has user:1 = %v\n", i+1, v)
		} else {
			fmt.Printf("❌ Node %d missing user:1\n", i+1)
		}
	}

	// DEMO 2: Delete
	fmt.Println()
	fmt.Println("--- Demo 2: Delete user:1 via node-2 ---")

	req, _ := http.NewRequest("DELETE", nodeURLs[1]+"/keys/user:1", nil)
	http.DefaultClient.Do(req)

	time.Sleep(500 * time.Millisecond)

	for i, url := range nodeURLs {
		resp, _ := http.Get(url + "/keys/user:1")
		if resp.StatusCode == 404 {
			fmt.Printf("✅ Node %d: user:1 deleted\n", i+1)
		} else {
			fmt.Printf("❌ Node %d still has user:1\n", i+1)
		}
		resp.Body.Close()
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
	fmt.Println("Servers running. Press Enter to stop all nodes.")
	fmt.Scanln()
}
