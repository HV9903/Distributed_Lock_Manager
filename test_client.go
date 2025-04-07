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
	dlmClient := client.NewClient()

	// Initialize connection
	log.Printf("Connecting to server at %s", serverAddr)
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer func() {
		log.Println("Closing connection to server")
		if err := dlmClient.RPC_close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Acquire lock
	lockName := "global_lock"
	log.Printf("Trying to acquire lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	log.Printf("Lock acquired: %s", lockName)

	// Append to file
	fileName := "file_0"
	data := fmt.Sprintf("Data written at %s by client\n", time.Now().Format(time.RFC3339))
	log.Printf("Appending to file: %s", fileName)
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file: %s", fileName)
	}

	// Simulate some work
	log.Printf("Holding lock for 5 seconds...")
	time.Sleep(5 * time.Second)

	// Release lock
	log.Printf("Releasing lock: %s", lockName)
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	log.Printf("Lock released: %s", lockName)

	// Test fault tolerance by simulating network issues
	if len(os.Args) > 2 && os.Args[2] == "test-fault-tolerance" {
		testFaultTolerance(dlmClient)
	}
}

// testFaultTolerance tests the fault tolerance mechanisms
func testFaultTolerance(dlmClient *client.Client) {
	log.Println("\n--- Testing Fault Tolerance ---")
	serverAddr := os.Args[1]

	// Test 1: Rapid lock acquire/release
	log.Println("Test 1: Rapid lock acquire/release")
	for i := 0; i < 3; i++ {
		lockName := fmt.Sprintf("test_lock_%d", i)
		
		// Acquire lock
		log.Printf("Acquiring lock: %s", lockName)
		err := dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			continue
		}
		log.Printf("Lock acquired: %s", lockName)
		
		// Release immediately
		log.Printf("Releasing lock: %s", lockName)
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released: %s", lockName)
		}
	}

	// Test 2: Append to multiple files
	log.Println("\nTest 2: Append to multiple files")
	lockName := "multi_file_lock"
	err := dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
	} else {
		log.Printf("Lock acquired: %s", lockName)
		
		for i := 1; i <= 3; i++ {
			fileName := fmt.Sprintf("file_%d", i)
			data := fmt.Sprintf("Fault tolerance test at %s\n", time.Now().Format(time.RFC3339))
			
			log.Printf("Appending to file: %s", fileName)
			err = dlmClient.RPC_append_file(fileName, data, lockName)
			if err != nil {
				log.Printf("Failed to append to file: %v", err)
			} else {
				log.Printf("Successfully appended to file: %s", fileName)
			}
		}
		
		// Release lock
		log.Printf("Releasing lock: %s", lockName)
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released: %s", lockName)
		}
	}

	// Test 3: Simulate duplicate requests
	log.Println("\nTest 3: Simulate duplicate requests")
	lockName = "duplicate_test_lock"

	// First acquire
	log.Printf("First acquire of lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
	} else {
		log.Printf("Lock acquired: %s", lockName)
		
		// Try to acquire the same lock again with a different client
		// This should fail because the lock is already held
		tempClient := client.NewClient()
		err := tempClient.RPC_init(serverAddr)
		if err != nil {
			log.Printf("Failed to initialize temp client: %v", err)
		} else {
			log.Printf("Trying to acquire the same lock with different client: %s", lockName)
			err = tempClient.RPC_lock_acquire(lockName)
			if err != nil {
				log.Printf("Expected failure: %v", err)
			} else {
				log.Printf("Unexpected success in acquiring lock twice")
			}
			tempClient.RPC_close()
		}
		
		// Release lock
		log.Printf("Releasing lock: %s", lockName)
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released: %s", lockName)
		}
	}

	log.Println("Fault tolerance tests completed")
}
