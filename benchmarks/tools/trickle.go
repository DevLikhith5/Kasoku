//go:build notest

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
		},
	}

	var getsReq uint64
	var putsReq uint64

	fmt.Println("🚀 Spawning benchmark traffic to localhost:9000 (Running endlessly...)")
	fmt.Println("Switch to your browser dashboard to watch the 'Metrics' tab!")

	for i := 0; i < 10; i++ {
		go func(workerID int) {
			for {
				key := fmt.Sprintf("bench-key-%d", time.Now().UnixNano()%10000)
				url := fmt.Sprintf("http://localhost:9000/api/v1/put/%s", key)
				req, _ := http.NewRequest("PUT", url, bytes.NewReader([]byte(`{"value": "test data"}`)))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
					atomic.AddUint64(&putsReq, 1)
				}
				time.Sleep(200 * time.Millisecond) // Slowed down significantly (gentle trickle)
			}
		}(i)
	}

	for i := 0; i < 30; i++ {
		go func(workerID int) {
			for {
				key := fmt.Sprintf("bench-key-%d", time.Now().UnixNano()%10000)
				url := fmt.Sprintf("http://localhost:9000/api/v1/get/%s", key)
				req, _ := http.NewRequest("GET", url, nil)

				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
					atomic.AddUint64(&getsReq, 1)
				}
				time.Sleep(100 * time.Millisecond) // Gentle trickle
			}
		}(i)
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Run forever until you kill the terminal process
	for {
		<-ticker.C
		gets := atomic.LoadUint64(&getsReq)
		puts := atomic.LoadUint64(&putsReq)
		fmt.Printf("... Sent %d GETs and %d PUTs total ...\n", gets, puts)
	}
}
