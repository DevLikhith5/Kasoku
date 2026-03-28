package main

import (
    "fmt"
    "time"
    "os"
)

func main() {
    f, _ := os.Create("test.wal")
    defer os.Remove("test.wal")
    defer f.Close()

    start := time.Now()
    for i := 0; i < 10000; i++ {
        f.Write([]byte("some data to write\n"))
    }
    dur := time.Since(start)
    
    fmt.Printf("10000 writes without fsyncs took %v (%.2f syncs/sec)\n", dur, 10000/dur.Seconds())
}
