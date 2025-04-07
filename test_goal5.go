package main


import (
	"fmt"
	"log"
	"os"
	// "sync"
	"time"

	"part_1/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_goal5.go <server_address>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	log.Printf("Testing Goal 5: Server Crash Recovery on server %s", serverAddr)

	// Test 1: Basic state recovery
	testBasicStateRecovery(serverAddr)

	// Test 2: Lock state persistence
	testLockStatePersistence(serverAddr)

	// Test 3: Request cache persistence
	testRequestCachePersistence(serverAddr)

	// Test 4: Fencing token persistence
	testFencingTokenPersistence(serverAddr)

	// Test 5: Edge case - crash during operation
	testCrashDuringOperation(serverAddr)

	log.Println("All Goal 5 tests completed")
}

func testBasicStateRecovery(serverAddr string) {
	log.Println("\n--- Test 1: Basic State Recovery ---")
	log.Println("This test requires manual server restart")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	// Create some state on the server
	lockNames := []string{"recovery_lock_1", "recovery_lock_2", "recovery_lock_3"}
	
	for _, lockName := range lockNames {
		log.Printf("Acquiring lock: %s", lockName)
		err = dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			continue
		}
		log.Printf("Lock acquired successfully")
		
		// Append to a file
		fileName := "file_11"
		data := fmt.Sprintf("Data for lock %s\n", lockName)
		err = dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Failed to append to file: %v", err)
		} else {
			log.Printf("Successfully appended to file")
		}
	}
	
	log.Println("Please restart the server now, then press Enter to continue...")
	fmt.Scanln()
	
	// Reconnect to the server
	err = dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to reconnect: %v", err)
	}
	
	// Verify the locks are still held
	for _, lockName := range lockNames {
		// Try to acquire the lock (should fail if recovery worked)
		log.Printf("Attempting to acquire lock after restart: %s", lockName)
		err = dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Expected failure (lock still held): %v", err)
		} else {
			log.Printf("Unexpected success (lock not recovered)")
			
			// Release the lock
			err = dlmClient.RPC_lock_release(lockName)
			if err != nil {
				log.Printf("Failed to release lock: %v", err)
			}
		}
	}
	
	// Release all locks
	for _, lockName := range lockNames {
		log.Printf("Releasing lock: %s", lockName)
		err = dlmClient.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Failed to release lock: %v", err)
		} else {
			log.Printf("Lock released successfully")
		}
	}
}

func testLockStatePersistence(serverAddr string) {
	log.Println("\n--- Test 2: Lock State Persistence ---")
	log.Println("This test requires manual server restart")
	
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
	
	lockName := "persistence_test_lock"
	
	// Client 1 acquires the lock
	log.Printf("Client 1: Acquiring lock: %s", lockName)
	err = client1.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Client 1: Lock acquired successfully")
	
	log.Println("Please restart the server now, then press Enter to continue...")
	fmt.Scanln()
	
	// Reconnect clients
	err = client1.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to reconnect client 1: %v", err)
	}
	
	err = client2.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to reconnect client 2: %v", err)
	}
	
	// Client 2 tries to acquire the same lock
	log.Printf("Client 2: Attempting to acquire lock after restart")
	err = client2.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 2: Expected failure (lock still held): %v", err)
	} else {
		log.Printf("Client 2: Unexpected success (lock not recovered)")
		
		// Release the lock
		err = client2.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Client 2: Failed to release lock: %v", err)
		}
	}
	
	// Client 1 releases the lock
	log.Printf("Client 1: Releasing lock")
	err = client1.RPC_lock_release(lockName)
	if err != nil {
		log.Printf("Client 1: Failed to release lock: %v", err)
	} else {
		log.Printf("Client 1: Lock released successfully")
	}
	
	// Client 2 should now be able to acquire the lock
	log.Printf("Client 2: Attempting to acquire lock after release")
	err = client2.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Client 2: Failed to acquire lock: %v", err)
	} else {
		log.Printf("Client 2: Lock acquired successfully")
		
		// Release the lock
		err = client2.RPC_lock_release(lockName)
		if err != nil {
			log.Printf("Client 2: Failed to release lock: %v", err)
		} else {
			log.Printf("Client 2: Lock released successfully")
		}
	}
}

func testRequestCachePersistence(serverAddr string) {
	log.Println("\n--- Test 3: Request Cache Persistence ---")
	log.Println("This test requires manual server restart")
	
	dlmClient := client.NewClient()
	err := dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer dlmClient.RPC_close()
	
	lockName := "cache_test_lock"
	fileName := "file_12"
	
	// Acquire lock
	log.Printf("Acquiring lock: %s", lockName)
	err = dlmClient.RPC_lock_acquire(lockName)
	if err != nil {
		log.Printf("Failed to acquire lock: %v", err)
		return
	}
	log.Printf("Lock acquired successfully")
	
	// Append to file
	data := "Request cache test data\n"
	log.Printf("Appending to file: %s", fileName)
	err = dlmClient.RPC_append_file(fileName, data, lockName)
	if err != nil {
		log.Printf("Failed to append to file: %v", err)
	} else {
		log.Printf("Successfully appended to file")
	}
	
	log.Println("Please restart the server now, then press Enter to continue...")
	fmt.Scanln()
	
	// Reconnect to the server
	err = dlmClient.RPC_init(serverAddr)
	if err != nil {
		log.Fatalf("Failed to reconnect: %v", err)
	}
	
		// Send the same append request again (should be idempotent)
		log.Printf("Sending the same append request after restart")
		err = dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Failed to append to file: %v", err)
		} else {
			log.Printf("Successfully handled duplicate append request")
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
	
	func testFencingTokenPersistence(serverAddr string) {
		log.Println("\n--- Test 4: Fencing Token Persistence ---")
		log.Println("This test requires manual server restart")
		
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
		
		lockName := "token_persistence_lock"
		fileName := "file_13"
		
		// Client 1 acquires the lock
		log.Printf("Client 1: Acquiring lock: %s", lockName)
		err = client1.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Client 1: Failed to acquire lock: %v", err)
			return
		}
		log.Printf("Client 1: Lock acquired successfully")
		
		log.Println("Please restart the server now, then press Enter to continue...")
		fmt.Scanln()
		
		// Reconnect clients
		err = client1.RPC_init(serverAddr)
		if err != nil {
			log.Fatalf("Failed to reconnect client 1: %v", err)
		}
		
		err = client2.RPC_init(serverAddr)
		if err != nil {
			log.Fatalf("Failed to reconnect client 2: %v", err)
		}
		
		// Client 1 tries to append with its token
		log.Printf("Client 1: Attempting to append with token after restart")
		data1 := "Data from client 1 after restart\n"
		err = client1.RPC_append_file(fileName, data1, lockName)
		if err != nil {
			log.Printf("Client 1: Failed to append: %v", err)
		} else {
			log.Printf("Client 1: Successfully appended with token after restart")
		}
		
		// Wait for lock timeout
		log.Printf("Waiting for lock to timeout...")
		time.Sleep(35 * time.Second)
		
		// Client 2 acquires the lock
		log.Printf("Client 2: Acquiring lock after timeout")
		err = client2.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Client 2: Failed to acquire lock: %v", err)
			return
		}
		log.Printf("Client 2: Lock acquired successfully")
		
		// Client 1 tries to append with its old token
		log.Printf("Client 1: Attempting to append with stale token")
		data2 := "Data from client 1 with stale token\n"
		err = client1.RPC_append_file(fileName, data2, lockName)
		if err != nil {
			log.Printf("Client 1: Expected failure with stale token: %v", err)
		} else {
			log.Printf("Client 1: Unexpected success with stale token!")
		}
		
		// Client 2 appends with its token
		log.Printf("Client 2: Appending with current token")
		data3 := "Data from client 2 with current token\n"
		err = client2.RPC_append_file(fileName, data3, lockName)
		if err != nil {
			log.Printf("Client 2: Failed to append: %v", err)
		} else {
			log.Printf("Client 2: Successfully appended with current token")
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
	
	func testCrashDuringOperation(serverAddr string) {
		log.Println("\n--- Test 5: Crash During Operation ---")
		log.Println("This test requires manual server restart during an operation")
		
		dlmClient := client.NewClient()
		err := dlmClient.RPC_init(serverAddr)
		if err != nil {
			log.Fatalf("Failed to initialize: %v", err)
		}
		defer dlmClient.RPC_close()
		
		lockName := "crash_during_op_lock"
		
		// Acquire lock
		log.Printf("Acquiring lock: %s", lockName)
		err = dlmClient.RPC_lock_acquire(lockName)
		if err != nil {
			log.Printf("Failed to acquire lock: %v", err)
			return
		}
		log.Printf("Lock acquired successfully")
		
		// Prepare for a long-running operation
		log.Println("About to perform a long-running operation")
		log.Println("Please restart the server during this operation, then press Enter...")
		
		// Simulate a long-running operation
		for i := 0; i < 10; i++ {
			log.Printf("Operation in progress: step %d/10", i+1)
			time.Sleep(3 * time.Second)
		}
		
		fmt.Scanln()
		
		// Reconnect to the server
		err = dlmClient.RPC_init(serverAddr)
		if err != nil {
			log.Fatalf("Failed to reconnect: %v", err)
		}
		
		// Check if we still hold the lock
		fileName := "file_14"
		data := "Data after crash during operation\n"
		log.Printf("Attempting to append to file after crash")
		err = dlmClient.RPC_append_file(fileName, data, lockName)
		if err != nil {
			log.Printf("Failed to append (lock may have been lost): %v", err)
			
			// Try to acquire the lock again
			log.Printf("Attempting to acquire lock again")
			err = dlmClient.RPC_lock_acquire(lockName)
			if err != nil {
				log.Printf("Failed to acquire lock: %v", err)
			} else {
				log.Printf("Lock acquired successfully")
				
				// Try to append again
				log.Printf("Attempting to append with new lock")
				err = dlmClient.RPC_append_file(fileName, data, lockName)
				if err != nil {
					log.Printf("Failed to append: %v", err)
				} else {
					log.Printf("Successfully appended with new lock")
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
		} else {
			log.Printf("Successfully appended (lock was preserved)")
			
			// Release the lock
			log.Printf("Releasing lock")
			err = dlmClient.RPC_lock_release(lockName)
			if err != nil {
				log.Printf("Failed to release lock: %v", err)
			} else {
				log.Printf("Lock released successfully")
			}
		}
	}
	