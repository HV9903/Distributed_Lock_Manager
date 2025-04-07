package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"part_1/client"
)

// TestDuplicateRequestHandling tests the server's ability to handle duplicate requests
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_goal2.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	log.Printf("Testing Goal 2: Duplicate Request Handling on server %s", serverAddr)

	// Test 1: Basic duplicate request handling
	testBasicDuplicateRequests(serverAddr)

	// Test 2: Duplicate lock acquisition
	testDuplicateLockAcquisition(serverAddr)

	// Test 3: Duplicate file append
	testDuplicateFileAppend(serverAddr)

	// Test 4: Concurrent duplicate requests
	testConcurrentDuplicateRequests(serverAddr)

	// Test 5: Edge case - rapid duplicate requests
	testRapidDuplicateRequests(serverAddr)

	log.Println("All Goal 2 tests completed")
}

func testBasicDuplicateRequests(serverAddr string) {
	log.Println("\n--- Test 1: Basic Duplicate Request Handling ---")
	
	// Create a custom client that allows us to manipulate request IDs
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// First request - normal lock acquisition
	lockName := "duplicate_test_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Second request - should be treated as duplicate and return success
	log.Printf("Sending duplicate lock acquisition request")
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Duplicate request failed: %v", err)
	} else {
		log.Printf("Duplicate request handled successfully")
	}
	
	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testDuplicateLockAcquisition(serverAddr string) {
	log.Println("\n--- Test 2: Duplicate Lock Acquisition ---")
	
	// Create two clients
	client1 := client.NewClient()
	err := client1.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize client 1: %v", err)
	}
	defer client1.RPC_close()
	
	client2 := client.NewClient()
	err = client2.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize client 2: %v", err)
	}
	defer client2.RPC_close()
	
	// Client 1 acquires the lock
	lockName := "duplicate_lock_test"
	log.Printf("Client 1: Acquiring lock: %s", lockName)
	err = client1.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 1: Lock acquired successfully")
	
	// Client 2 tries to acquire the same lock (should fail or block)
	log.Printf("Client 2: Trying to acquire the same lock")
	
	// Use a goroutine with timeout to prevent blocking indefinitely
	var wg sync.WaitGroup
	wg.Add(1)
	
	var client2Err error
	go func() {
		defer wg.Done()
		client2Err = client2.RPC_lock_acquire(lockName)
	}()
	
	// Wait for a short time to see if client2 gets blocked
	time.Sleep(2 * time.Second)
	
	// Client 1 releases the lock
	log.Printf("Client 1: Releasing lock")
	err = client1.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to release lock: %v", err)
	} else {
		log.Printf("Client 1: Lock released successfully")
	}
	
	// Wait for client2 to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		if client2Err != nil {
			log.Printf("Client 2: Failed to acquire lock: %v", client2Err)
		} else {
			log.Printf("Client 2: Successfully acquired lock after client 1 released it")
			
			// Release the lock
			err = client2.RPC_lock_release(lockName)
			if err != nil {
				log.Printf("Client 2: Failed to release lock: %v", err)
			} else {
				log.Printf("Client 2: Lock released successfully")
			}
		}
	case <-time.After(5 * time.Second):
		log.Printf("Client 2: Timed out waiting for lock acquisition")
	}
}

func testDuplicateFileAppend(serverAddr string) {
	log.Println("\n--- Test 3: Duplicate File Append ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// Acquire a lock
	lockName := "file_append_test_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Append to a file
	fileName := "file_1"
	data := fmt.Sprintf("Duplicate test data at %s\n", time.Now().Format(time.RFC3339))
	log.Printf("Appending to file: %s", fileName)
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file")
	}
	
	// Send duplicate append request with the same data
	log.Printf("Sending duplicate append request")
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Duplicate append failed: %v", err)
	} else {
		log.Printf("Duplicate append handled successfully")
	}
	
	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testConcurrentDuplicateRequests(serverAddr string) {
	log.Println("\n--- Test 4: Concurrent Duplicate Requests ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// Acquire a lock
	lockName := "concurrent_duplicate_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Send multiple duplicate append requests concurrently
	var wg sync.WaitGroup
	requestCount := 5
	fileName := "file_2"
	data := fmt.Sprintf("Concurrent duplicate test at %s\n", time.Now().Format(time.RFC3339))
	
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			log.Printf("Goroutine %d: Sending append request", index)
			err := dlmClient.RPC_append_file(fileName, data, lockName)
			if err != nil {
				log.Printf("Goroutine %d: Append failed: %v", index, err)
			} else {
				log.Printf("Goroutine %d: Append succeeded", index)
			}
		}(i)
	}
	
		wg.Wait()
	log.Println("All concurrent requests completed")

	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testRapidDuplicateRequests(serverAddr string) {
	log.Println("\n--- Test 5: Rapid Duplicate Requests ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// Acquire a lock
	lockName := "rapid_duplicate_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Send rapid duplicate requests
	requestCount := 100
	fileName := "file_3"
	data := fmt.Sprintf("Rapid duplicate test at %s\n", time.Now().Format(time.RFC3339))
	
	startTime := time.Now()
	for i := 0; i < requestCount; i++ {
		err := dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Request %d: Append failed: %v", i, err)
		}
	}
	duration := time.Since(startTime)
	
	log.Printf("Completed %d rapid requests in %v", requestCount, duration)
	
	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}
