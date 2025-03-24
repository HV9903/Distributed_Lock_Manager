package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"part_1/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_client.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	client := client.NewClient()

	// Initialize connection
	err := client.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer client.RPC_close()

	// Acquire lock
	lockName := "global_lock"
	log.Printf("Trying to acquire lock: %s", lockName)
	err = client.RPC_lock_acquire(lockName)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	log.Printf("Lock acquired: %s", lockName)

	// Append to file
	fileName := "file_0"
	data := fmt.Sprintf("Data written at %s\n", time.Now().Format(time.RFC3339))
	log.Printf("Appending to file: %s", fileName)
	err = client.RPC_append_file(fileName, data)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file: %s", fileName)
	}

	// Hold the lock for a while
	time.Sleep(5 * time.Second)

	// Release lock
	log.Printf("Releasing lock: %s", lockName)
	err = client.RPC_lock_release(lockName)
	if err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	log.Printf("Lock released: %s", lockName)
}
