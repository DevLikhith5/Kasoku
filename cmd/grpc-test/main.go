package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/DevLikhith5/kasoku/api"
)

func main() {
	fmt.Println("=== gRPC Connection Test ===")

	conn, err := grpc.NewClient("localhost:9002",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Printf("❌ gRPC dial failed: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("✓ gRPC client connected")

	client := api.NewKasokuServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	md := metadata.Pairs("test", "value")
	ctx = metadata.NewOutgoingContext(ctx, md)

	fmt.Println("Testing Put with metadata...")
	resp, err := client.Put(ctx, &api.PutRequest{
		Key:   "test:hello",
		Value: []byte("world"),
	})
	if err != nil {
		fmt.Printf("❌ Put failed: %v\n", err)
		return
	}
	fmt.Printf("✓ Put succeeded: success=%v, error=%s\n", resp.Success, resp.Error)
}