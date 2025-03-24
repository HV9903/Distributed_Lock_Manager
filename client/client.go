package client

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "part_1/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a DLM client
type Client struct {
	clientID string
	conn     *grpc.ClientConn
	client   pb.LockServiceClient
}

// NewClient creates a new DLM client
func NewClient() *Client {
	return &Client{
		clientID: uuid.New().String(),
	}
}

// RPC_init initializes the connection to the server
func (c *Client) RPC_init(serverAddr string) error {
	// Set up a connection to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	
	c.conn = conn
	c.client = pb.NewLockServiceClient(conn)
	
	// Send init request to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := c.client.Init(ctx, &pb.InitRequest{
		ClientId: c.clientID,
	})
	
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to initialize: %v", err)
	}
	
	if resp.ReturnCode != 0 {
		c.conn.Close()
		return fmt.Errorf("server returned error code: %d", resp.ReturnCode)
	}
	
	log.Printf("Connected to server: %s", resp.Message)
	return nil
}

// RPC_lock_acquire acquires a lock from the server
func (c *Client) RPC_lock_acquire(lockName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute) // Long timeout for blocking
	defer cancel()
	
	resp, err := c.client.LockAcquire(ctx, &pb.LockAcquireRequest{
		ClientId: c.clientID,
		LockName: lockName,
	})
	
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %v", err)
	}
	
	if resp.ReturnCode != 0 {
		return fmt.Errorf("failed to acquire lock: %s", resp.Message)
	}
	
	return nil
}

// RPC_lock_release releases a lock
func (c *Client) RPC_lock_release(lockName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := c.client.LockRelease(ctx, &pb.LockReleaseRequest{
		ClientId: c.clientID,
		LockName: lockName,
	})
	
	if err != nil {
		return fmt.Errorf("failed to release lock: %v", err)
	}
	
	if resp.ReturnCode != 0 {
		return fmt.Errorf("failed to release lock: %s", resp.Message)
	}
	
	return nil
}

// RPC_append_file appends data to a file on the server
func (c *Client) RPC_append_file(fileName string, data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := c.client.AppendFile(ctx, &pb.AppendFileRequest{
		ClientId: c.clientID,
		FileName: fileName,
		Data:     data,
	})
	
	if err != nil {
		return fmt.Errorf("failed to append to file: %v", err)
	}
	
	if resp.ReturnCode != 0 {
		return fmt.Errorf("failed to append to file: %s", resp.Message)
	}
	
	return nil
}

// RPC_close closes the connection to the server
func (c *Client) RPC_close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := c.client.Close(ctx, &pb.CloseRequest{
		ClientId: c.clientID,
	})
	
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to close connection: %v", err)
	}
	
	if resp.ReturnCode != 0 {
		c.conn.Close()
		return fmt.Errorf("server returned error on close: %d", resp.ReturnCode)
	}
	
	return c.conn.Close()
}
