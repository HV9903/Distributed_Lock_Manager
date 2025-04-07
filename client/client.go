package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "part_1/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a DLM client
type Client struct {
	clientID    string
	conn        *grpc.ClientConn
	client      pb.LockServiceClient
	locksMutex  sync.Mutex
	locks       map[string]int64 // lockName -> fencingToken
	requestIDs  map[string]string // operationKey -> requestID
	maxRetries  int
}

// NewClient creates a new DLM client
func NewClient() *Client {
	return &Client{
		clientID:   uuid.New().String(),
		locks:      make(map[string]int64),
		requestIDs: make(map[string]string),
		maxRetries: 5,
	}
}

// generateRequestID creates a unique request ID
func (c *Client) generateRequestID() string {
	return uuid.New().String()
}

// getRequestID gets or creates a request ID for an operation
func (c *Client) getRequestID(operationKey string) string {
	if id, exists := c.requestIDs[operationKey]; exists {
		return id
	}
	id := c.generateRequestID()
	c.requestIDs[operationKey] = id
	return id
}

// RPC_init initializes the connection to the server
func (c *Client) RPC_init(serverAddr string) error {
	// Set up a connection to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	
	c.conn = conn
	c.client = pb.NewLockServiceClient(conn)
	
	// Send init request to server with retry
	operationKey := "init"
	requestID := c.getRequestID(operationKey)
	
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		resp, err := c.client.Init(ctx, &pb.InitRequest{
			ClientId: c.clientID,
			RequestId: requestID,
		})
		
		if err == nil && resp.ReturnCode == 0 {
			log.Printf("Connected to server: %s", resp.Message)
			return nil
		}
		
		if attempt < c.maxRetries-1 {
			backoff := calculateBackoff(attempt)
			log.Printf("Init failed, retrying in %v: %v", backoff, err)
			time.Sleep(backoff)
		}
	}
	
	c.conn.Close()
	return fmt.Errorf("failed to initialize after %d attempts", c.maxRetries)
}

// RPC_lock_acquire acquires a lock from the server
func (c *Client) RPC_lock_acquire(lockName string) error {
	operationKey := fmt.Sprintf("acquire:%s", lockName)
	requestID := c.getRequestID(operationKey)
	
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		resp, err := c.client.LockAcquire(ctx, &pb.LockAcquireRequest{
			ClientId: c.clientID,
			LockName: lockName,
			RequestId: requestID,
		})
		
		if err == nil {
			if resp.ReturnCode == 0 {
				// Store the fencing token
				c.locksMutex.Lock()
				c.locks[lockName] = resp.FencingToken
				c.locksMutex.Unlock()
				return nil
			} else if resp.ReturnCode == 2 { // In progress
				// Wait and retry with the same request ID
				backoff := calculateBackoff(attempt)
				log.Printf("Lock acquisition in progress, retrying in %v", backoff)
				time.Sleep(backoff)
				continue
			} else {
				return fmt.Errorf("failed to acquire lock: %s", resp.Message)
			}
		}
		
		if attempt < c.maxRetries-1 {
			backoff := calculateBackoff(attempt)
			log.Printf("Lock acquisition failed, retrying in %v: %v", backoff, err)
			time.Sleep(backoff)
		}
	}
	
	return fmt.Errorf("failed to acquire lock after %d attempts", c.maxRetries)
}

// RPC_lock_release releases a lock
func (c *Client) RPC_lock_release(lockName string) error {
	c.locksMutex.Lock()
	token, exists := c.locks[lockName]
	c.locksMutex.Unlock()
	
	if !exists {
		return fmt.Errorf("lock not held: %s", lockName)
	}
	
	operationKey := fmt.Sprintf("release:%s", lockName)
	requestID := c.getRequestID(operationKey)
	
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		resp, err := c.client.LockRelease(ctx, &pb.LockReleaseRequest{
			ClientId: c.clientID,
			LockName: lockName,
			RequestId: requestID,
			FencingToken: token,
		})
		
		if err == nil {
			if resp.ReturnCode == 0 {
				c.locksMutex.Lock()
				delete(c.locks, lockName)
				c.locksMutex.Unlock()
				return nil
			} else if resp.ReturnCode == 2 { // In progress
				backoff := calculateBackoff(attempt)
				log.Printf("Lock release in progress, retrying in %v", backoff)
				time.Sleep(backoff)
				continue
			} else {
				return fmt.Errorf("failed to release lock: %s", resp.Message)
			}
		}
		
		if attempt < c.maxRetries-1 {
			backoff := calculateBackoff(attempt)
			log.Printf("Lock release failed, retrying in %v: %v", backoff, err)
			time.Sleep(backoff)
		}
	}
	
	return fmt.Errorf("failed to release lock after %d attempts", c.maxRetries)
}

// RPC_append_file appends data to a file on the server
func (c *Client) RPC_append_file(fileName string, data string, lockName string) error {
	c.locksMutex.Lock()
	token, exists := c.locks[lockName]
	c.locksMutex.Unlock()
	
	if !exists {
		return fmt.Errorf("lock not held: %s", lockName)
	}
	
	operationKey := fmt.Sprintf("append:%s:%s", fileName, data)
	requestID := c.getRequestID(operationKey)
	
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		resp, err := c.client.AppendFile(ctx, &pb.AppendFileRequest{
			ClientId: c.clientID,
			FileName: fileName,
			Data: data,
			RequestId: requestID,
			FencingToken: token,
		})
		
		if err == nil {
			if resp.ReturnCode == 0 {
				return nil
			} else if resp.ReturnCode == 2 { // In progress
				backoff := calculateBackoff(attempt)
				log.Printf("File append in progress, retrying in %v", backoff)
				time.Sleep(backoff)
				continue
			} else {
				return fmt.Errorf("failed to append to file: %s", resp.Message)
			}
		}
		
		if attempt < c.maxRetries-1 {
			backoff := calculateBackoff(attempt)
			log.Printf("File append failed, retrying in %v: %v", backoff, err)
			time.Sleep(backoff)
		}
	}
	
	return fmt.Errorf("failed to append to file after %d attempts", c.maxRetries)
}

// RPC_close closes the connection to the server
func (c *Client) RPC_close() error {
	if c.conn == nil {
		return nil
	}
	
	operationKey := "close"
	requestID := c.getRequestID(operationKey)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	_, err := c.client.Close(ctx, &pb.CloseRequest{
		ClientId: c.clientID,
		RequestId: requestID,
	})
	if err != nil {
        // Log the error but still try to close the connection
        log.Printf("Error during Close RPC: %v", err)
    }
	
	// Regardless of server response, close the connection
	return c.conn.Close()
}

// calculateBackoff calculates exponential backoff with jitter
func calculateBackoff(attempt int) time.Duration {
	baseDelay := 100 * time.Millisecond
	maxDelay := 5 * time.Second
	
	backoff := baseDelay * time.Duration(1<<uint(attempt))
	if backoff > maxDelay {
		backoff = maxDelay
	}
	
	// Add jitter (±20%)
	jitter := time.Duration(rand.Float64()*0.4 - 0.2) * backoff
	return backoff + jitter
}
