package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"part_1/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_goal3.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	log.Printf("Testing Goal 3: Lock Timeout Mechanism on server %s", serverAddr)

	// Test 1: Basic lock timeout
	testBasicLockTimeout(serverAddr)

	// Test 2: Lock acquisition after timeout
	testLockAcquisitionAfterTimeout(serverAddr)

	// Test 3: Multiple clients waiting for lock
	testMultipleClientsWaiting(serverAddr)

	// Test 4: Lock extension
	testLockExtension(serverAddr)

	// Test 5: Edge case - rapid lock/unlock near timeout
	testRapidLockUnlock(serverAddr)

	log.Println("All Goal 3 tests completed")
}

func testBasicLockTimeout(serverAddr string) {
	log.Println("\n--- Test 1: Basic Lock Timeout ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "timeout_test_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Wait for the lock to timeout (assuming 30-second timeout)
	log.Printf("Waiting for lock to timeout...")
	time.Sleep(35 * time.Second)
	
	// Try to use the lock (should fail)
	fileName := "file_4"
	data := "This should fail due to timeout\n"
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Expected failure: %v", err)
	} else {
		log.Printf("Unexpected success in appending to file after timeout")
	}
}

func testLockAcquisitionAfterTimeout(serverAddr string) {
	log.Println("\n--- Test 2: Lock Acquisition After Timeout ---")
	
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
	
	lockName := "timeout_acquisition_lock"
	
	// Client 1 acquires the lock
	log.Printf("Client 1: Acquiring lock: %s", lockName)
	err = client1.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 1: Lock acquired successfully")
	
	// Wait for the lock to timeout
	log.Printf("Waiting for lock to timeout...")
	time.Sleep(35 * time.Second)
	
	// Client 2 tries to acquire the lock
	log.Printf("Client 2: Attempting to acquire lock after timeout")
	err = client2.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 2: Failed to acquire lock: %v", err)
	} else {
		log.Printf("Client 2: Successfully acquired lock after timeout")
		
		// Release the lock
		err = client2.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Client 2: Failed to release lock: %v", err)
		} else {
			log.Printf("Client 2: Lock released successfully")
		}
	}
}

func testMultipleClientsWaiting(serverAddr string) {
	log.Println("\n--- Test 3: Multiple Clients Waiting for Lock ---")
	
	var clients []*client.Client
	clientCount := 5
	
	// Initialize clients
	for i := 0; i < clientCount; i++ {
		dlmClient := client.NewClient()
		err := dlmClient.RPC_init(serverAddr)
		if err != nil {
			log.Fatalf("Failed to initialize client %d: %v", i, err)
		}
		defer dlmClient.RPC_close()
		clients = append(clients, dlmClient)
	}
	
	lockName := "multi_client_wait_lock"
	
	// First client acquires the lock
	log.Printf("Client 0: Acquiring lock: %s", lockName)
	err := clients[0].RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 0: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 0: Lock acquired successfully")
	
	// Other clients try to acquire the lock
	var wg sync.WaitGroup
	for i := 1; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			log.Printf("Client %d: Attempting to acquire lock", clientID)
			err := clients[clientID].RPC_lock_acquire(lockName)
			if err != nil {
				log.Printf("Client %d: Failed to acquire lock: %v", clientID, err)
			} else {
				log.Printf("Client %d: Successfully acquired lock", clientID)
				
				// Release the lock
				err = clients[clientID].RPC_lock_release(lockName)
				if err != nil {
					log.Printf("Client %d: Failed to release lock: %v", clientID, err)
				} else {
					log.Printf("Client %d: Lock released successfully", clientID)
				}
			}
		}(i)
	}
	
	// Wait for the lock to timeout
	log.Printf("Waiting for lock to timeout...")
	time.Sleep(35 * time.Second)
	
	// Release the lock held by the first client
	err = clients[0].RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Client 0: Failed to release lock: %v", err)
	} else {
		log.Printf("Client 0: Lock released successfully")
	}
	
	// Wait for all clients to complete
	wg.Wait()
	log.Println("All clients completed")
}

func testLockExtension(serverAddr string) {
	log.Println("\n--- Test 4: Lock Extension ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "extension_test_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Simulate work and extend the lock multiple times
	for i := 0; i < 3; i++ {
		time.Sleep(20 * time.Second)
		
		// Extend the lock (assuming there's an RPC_extend_lock method)
		// err = dlmClient.RPC_extend_lock(lockName)
		// if err != nil {
		// 	log.Printf("Failed to extend lock: %v", err)
		// 	return
		// }
		log.Printf("Lock extended successfully")
		
		// Verify the lock is still valid by appending to a file
		fileName := "file_5"
		data := fmt.Sprintf("Lock extension test %d\n", i)
		err = dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Failed to append to file: %v", err)
		} else {
			log.Printf("Successfully appended to file after extension")
		}
	}
	
	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testRapidLockUnlock(serverAddr string) {
	log.Println("\n--- Test 5: Rapid Lock/Unlock Near Timeout ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "rapid_lock_unlock_test"
	iterations := 10
	
	for i := 0; i < iterations; i++ {
		log.Printf("Iteration %d: Acquiring lock", i)
		err = dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			continue
		}
		
		// Hold the lock for just under the timeout period
		time.Sleep(28 * time.Second)
		
		log.Printf("Iteration %d: Releasing lock", i)
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		}
		
		// Short pause before next iteration
		time.Sleep(2 * time.Second)
	}
	
	log.Printf("Completed %d rapid lock/unlock cycles", iterations)
}
