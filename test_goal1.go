
package main

import (
	// "context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"part_1/client"


	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
)

// TestPacketLossHandling tests the retry mechanism for handling packet loss
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_goal1.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	log.Printf("Testing Goal 1: Packet Loss Handling on server %s", serverAddr)

	// Test 1: Basic retry mechanism
	testBasicRetry(serverAddr)

	// Test 2: Concurrent clients with retries
	testConcurrentRetries(serverAddr)

	// Test 3: Maximum retry limit
	testMaxRetries(serverAddr)

	// Test 4: Retry with exponential backoff
	testExponentialBackoff(serverAddr)

	// Test 5: Edge case - server unavailable initially
	testServerInitiallyUnavailable(serverAddr)

	log.Println("All Goal 1 tests completed")
}

func testBasicRetry(serverAddr string) {
	log.Println("\n--- Test 1: Basic Retry Mechanism ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()

	// Acquire lock with potential packet loss
	lockName := "test_retry_lock"
	log.Printf("Acquiring lock: %s", lockName)
	
	startTime := time.Now()
	err = dlmClient.RPC_lock_acquire(lockName)
	duration := time.Since(startTime)
	
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
	} else {
		log.Printf("Lock acquired after %v", duration)
		
		// Release the lock
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released successfully")
		}
	}
}

func testConcurrentRetries(serverAddr string) {
	log.Println("\n--- Test 2: Concurrent Clients with Retries ---")
	
	var wg sync.WaitGroup
	clientCount := 5
	
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			
			dlmClient := client.NewClient()
			err := dlmClient.RPC_init(serverAddr)
			if err != nil {
				log.Printf("Client %d: Failed to initialize: %v", clientID, err)
				return
			}
			defer dlmClient.RPC_close()
			
			lockName := fmt.Sprintf("concurrent_lock_%d", clientID)
			log.Printf("Client %d: Acquiring lock: %s", clientID, lockName)
			
			startTime := time.Now()
			err = dlmClient.RPC_lock_acquire(lockName)
			duration := time.Since(startTime)
			
			if err != nil {
				log.Printf("Client %d: Failed to acquire lock: %v", clientID, err)
			} else {
				log.Printf("Client %d: Lock acquired after %v", clientID, duration)
				
				// Append to file
				fileName := fmt.Sprintf("file_%d", clientID%10)
				data := fmt.Sprintf("Data from client %d at %s\n", clientID, time.Now().Format(time.RFC3339))
				err = dlmClient.RPC_append_file(fileName, data, lockName)
				if err != nil {
					log.Printf("Client %d: Failed to append to file: %v", clientID, err)
				} else {
					log.Printf("Client %d: Successfully appended to file", clientID)
				}
				
				// Release lock
				err = dlmClient.RPC_lock_release(lockName)
				if err != nil {
					log.Printf("Client %d: Failed to release lock: %v", clientID, err)
				} else {
					log.Printf("Client %d: Lock released successfully", clientID)
				}
			}
		}(i)
	}
	
	wg.Wait()
	log.Println("All concurrent clients completed")
}

func testMaxRetries(serverAddr string) {
	log.Println("\n--- Test 3: Maximum Retry Limit ---")
	
	// Create a client with a custom retry limit
	dlmClient := client.NewClient()
	// Note: In a real implementation, you would have a way to set the max retries
	// dlmClient.SetMaxRetries(3)
	
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Printf("Failed to initialize: %v", err)
		log.Println("This is expected if the server is unreachable")
		return
	}
	defer dlmClient.RPC_close()
	
	// Try to acquire a lock with an invalid name to trigger retries
	invalidLockName := string([]byte{0xFF, 0xFE, 0xFD}) // Invalid UTF-8
	log.Printf("Attempting to acquire lock with invalid name")
	
	startTime := time.Now()
	err = dlmClient.RPC_lock_acquire(invalidLockName)
	duration := time.Since(startTime)
	
	if err != nil {
		log.Printf("Failed after %v with error: %v", duration, err)
		log.Println("This is expected - client should give up after max retries")
	} else {
		log.Printf("Unexpectedly succeeded in acquiring invalid lock")
		// Try to release it anyway
		dlmClient.RPC_lock_release(invalidLockName)
	}
}

func testExponentialBackoff(serverAddr string) {
	log.Println("\n--- Test 4: Retry with Exponential Backoff ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// Acquire and release a lock multiple times to observe backoff
	lockName := "backoff_test_lock"
	
	for i := 0; i < 3; i++ {
		log.Printf("Iteration %d: Acquiring lock", i)
		
		startTime := time.Now()
		err = dlmClient.RPC_lock_acquire(lockName)
		duration := time.Since(startTime)
		
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			continue
		}
		
		log.Printf("Lock acquired after %v", duration)
		
		// Hold the lock briefly
		time.Sleep(500 * time.Millisecond)
		
		// Release the lock
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released successfully")
		}
		
		// Wait a bit before next iteration
		time.Sleep(1 * time.Second)
	}
}

func testServerInitiallyUnavailable(serverAddr string) {
	log.Println("\n--- Test 5: Server Initially Unavailable ---")
	log.Println("This test assumes you can stop and start the server manually")
	log.Println("1. Stop the server now")
	log.Println("2. Press Enter to continue the test")
	log.Println("3. Start the server again before pressing Enter")
	
	// Wait for user input
	fmt.Scanln()
	
	log.Println("Attempting to connect to server (should be down initially)")
	
	// Create a client that will retry connecting to the server
	dlmClient := client.NewClient()
	
	// Start a goroutine to initialize the client
	var wg sync.WaitGroup
	wg.Add(1)
	
	var initErr error
	go func() {
		defer wg.Done()
		initErr = dlmClient.RPC_init(serverAddr)
	}()
	
	// Wait for the initialization to complete or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		if initErr != nil {
			log.Printf("Failed to initialize after retries: %v", initErr)
		} else {
			log.Println("Successfully connected to server after it became available")
			
			// Try to acquire a lock
			lockName := "recovery_test_lock"
			err := dlmClient.RPC_lock_acquire(lockName)
			if err != nil {
				log.Printf("Failed to acquire lock: %v", err)
			} else {
				log.Printf("Successfully acquired lock after server recovery")
				
				// Release the lock
				err = dlmClient.RPC_lock_release(lockName)
				if err != nil {
					log.Printf("Failed to release lock: %v", err)
				} else {
					log.Printf("Successfully released lock")
				}
			}
			
			dlmClient.RPC_close()
		}
	case <-time.After(200 * time.Second):
		log.Println("Test timed out - server may not have been restarted")
	}
}
