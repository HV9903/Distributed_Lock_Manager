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
		fmt.Println("Usage: go run test_goal4.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	log.Printf("Testing Goal 4: Fencing Token Protection on server %s", serverAddr)

	// Test 1: Basic fencing token usage
	testBasicFencingToken(serverAddr)

	// Test 2: Stale fencing token rejection
	testStaleFencingToken(serverAddr)

	// Test 3: Concurrent clients with fencing tokens
	testConcurrentFencingTokens(serverAddr)

	// Test 4: Fencing token after server restart
	testFencingTokenAfterRestart(serverAddr)

	// Test 5: Edge case - rapid lock acquisitions
	testRapidLockAcquisitions(serverAddr)

	log.Println("All Goal 4 tests completed")
}

func testBasicFencingToken(serverAddr string) {
	log.Println("\n--- Test 1: Basic Fencing Token Usage ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "fencing_test_lock"
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Append to file using the acquired fencing token
	fileName := "file_6"
	data := "Fencing token test data\n"
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file with valid fencing token")
	}
	
	// Release the lock
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testStaleFencingToken(serverAddr string) {
	log.Println("\n--- Test 2: Stale Fencing Token Rejection ---")
	
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
	
	lockName := "stale_token_test_lock"
	fileName := "file_7"
	
	// Client 1 acquires the lock
	log.Printf("Client 1: Acquiring lock: %s", lockName)
	err = client1.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 1: Lock acquired successfully")
	
	// Client 1 appends to file
	data1 := "Data from client 1\n"
	err = client1.RPC_append_file(fileName, data1, lockName)
	if err != nil {
		log.Printf("Client 1: Failed to append to file: %v", err)
	} else {
		log.Printf("Client 1: Successfully appended to file")
	}
	
	// Simulate client 1 crash by not releasing the lock
	log.Printf("Simulating client 1 crash (not releasing lock)")
	
	// Wait for lock timeout
	log.Printf("Waiting for lock to timeout...")
	time.Sleep(35 * time.Second)
	
	// Client 2 acquires the lock
	log.Printf("Client 2: Acquiring lock after timeout: %s", lockName)
	err = client2.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 2: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 2: Lock acquired successfully")
	
	// Client 2 appends to file
	data2 := "Data from client 2\n"
	err = client2.RPC_append_file(fileName, data2, lockName)
	if err != nil {
		log.Printf("Client 2: Failed to append to file: %v", err)
	} else {
		log.Printf("Client 2: Successfully appended to file")
	}
	
	// Simulate client 1 coming back online and trying to append with stale token
	log.Printf("Client 1: Attempting to append with stale token")
	data3 := "More data from client 1 (should fail)\n"
	err = client1.RPC_append_file(fileName, data3, lockName)
	if err != nil {
		log.Printf("Client 1: Expected failure with stale token: %v", err)
	} else {
		log.Printf("Client 1: Unexpected success with stale token!")
	}
	
	// Client 2 releases the lock
	log.Printf("Client 2: Releasing lock")
	err = client2.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Client 2: Failed to release lock: %v", err)
	} else {
		log.Printf("Client 2: Lock released successfully")
	}
}

func testConcurrentFencingTokens(serverAddr string) {
	log.Println("\n--- Test 3: Concurrent Clients with Fencing Tokens ---")
	
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
	
	lockName := "concurrent_token_lock"
	fileName := "file_8"
	
	// Clients take turns acquiring the lock and appending to file
	for i := 0; i < clientCount; i++ {
		log.Printf("Client %d: Acquiring lock", i)
		err := clients[i].RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Client %d: Failed to acquire lock: %v", i, err)
			continue
		}
		log.Printf("Client %d: Lock acquired successfully", i)
		
		// Append to file
		data := fmt.Sprintf("Data from client %d\n", i)
		err = clients[i].RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Client %d: Failed to append to file: %v", i, err)
		} else {
			log.Printf("Client %d: Successfully appended to file", i)
		}
		
		// Release the lock
		log.Printf("Client %d: Releasing lock", i)
		err = clients[i].RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Client %d: Failed to release lock: %v", i, err)
		} else {
			log.Printf("Client %d: Lock released successfully", i)
		}
	}
	
	// Now have all clients try to append concurrently (should all fail except the last one)
	log.Printf("All clients attempting to append concurrently")
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			data := fmt.Sprintf("Concurrent data from client %d\n", clientID)
			err := clients[clientID].RPC_append_file(fileName, data, lockName)
			if err != nil {
				log.Printf("Client %d: Expected failure: %v", clientID, err)
			} else {
				log.Printf("Client %d: Unexpected success!", clientID)
			}
		}(i)
	}
	wg.Wait()
}

func testFencingTokenAfterRestart(serverAddr string) {
	log.Println("\n--- Test 4: Fencing Token After Server Restart ---")
	log.Println("This test requires manual server restart")
	log.Println("1. Acquire a lock")
	log.Println("2. Restart the server")
	log.Println("3. Verify the token is still valid")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "restart_token_lock"
	fileName := "file_9"
	
	// Acquire lock
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Append to file
	data1 := "Data before server restart\n"
	err = dlmClient.RPC_append_file(fileName, data1, lockName)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file")
	}
	
	log.Println("Please restart the server now, then press Enter to continue...")
	fmt.Scanln()
	
	// Try to append after server restart
	log.Printf("Attempting to append after server restart")
	data2 := "Data after server restart\n"
	err = dlmClient.RPC_append_file(fileName, data2, lockName)
	if err != nil {
		log.Printf("Failed to append after restart: %v", err)
	} else {
		log.Printf("Successfully appended after restart")
	}
	
	// Release the lock
	log.Printf("Releasing lock")
	err = dlmClient.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Failed to release lock: %v", err)
	} else {
		log.Printf("Lock released successfully")
	}
}

func testRapidLockAcquisitions(serverAddr string) {
	log.Println("\n--- Test 5: Rapid Lock Acquisitions ---")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	iterations := 10
	lockName := "rapid_token_lock"
	fileName := "file_10"
	
	for i := 0; i < iterations; i++ {
		// Acquire lock
		log.Printf("Iteration %d: Acquiring lock", i)
		err = dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			continue
		}
		
		// Append to file
		data := fmt.Sprintf("Data from iteration %d\n", i)
		err = dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Failed to append to file: %v", err)
		} else {
			log.Printf("Successfully appended to file")
		}
		
		// Release lock
		log.Printf("Releasing lock")
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released successfully")
		}
	}
	
	log.Printf("Completed %d rapid lock acquisition cycles", iterations)
}