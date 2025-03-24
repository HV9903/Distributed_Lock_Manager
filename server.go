package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	// "time"

	pb "part_1/proto"
	"google.golang.org/grpc"
)

const (
	port        = ":50051"
	filesDirPath = "./files"
	numFiles    = 100
)

// SpinLock implements a simple spinlock
type SpinLock struct {
	lock      sync.Mutex
	isLocked  bool
	waitQueue []chan struct{}
}

// Acquire attempts to acquire the lock
func (sl *SpinLock) Acquire() {
	sl.lock.Lock()
	if !sl.isLocked {
		sl.isLocked = true
		sl.lock.Unlock()
		return
	}

	// Create a channel for this waiter
	waitCh := make(chan struct{})
	sl.waitQueue = append(sl.waitQueue, waitCh)
	sl.lock.Unlock()

	// Wait until notified
	<-waitCh
}

// Release releases the lock
func (sl *SpinLock) Release() {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	if len(sl.waitQueue) > 0 {
		// Notify the next waiter
		nextWaiter := sl.waitQueue[0]
		sl.waitQueue = sl.waitQueue[1:]
		close(nextWaiter)
	} else {
		sl.isLocked = false
	}
}

// LockServer implements the LockService gRPC service
type LockServer struct {
	pb.UnimplementedLockServiceServer
	spinLock    SpinLock
	clientsLock sync.Mutex
	clients     map[string]bool
}

// Init handles the initialization request from clients
func (s *LockServer) Init(ctx context.Context, req *pb.InitRequest) (*pb.InitResponse, error) {
	s.clientsLock.Lock()
	s.clients[req.ClientId] = true
	s.clientsLock.Unlock()

	log.Printf("Client %s connected", req.ClientId)
	return &pb.InitResponse{
		Message:    "Connected!",
		ReturnCode: 0,
	}, nil
}

// LockAcquire handles lock acquisition requests
func (s *LockServer) LockAcquire(ctx context.Context, req *pb.LockAcquireRequest) (*pb.LockAcquireResponse, error) {
	log.Printf("Client %s is trying to acquire lock %s", req.ClientId, req.LockName)
	
	// Acquire the lock (this will block if the lock is already held)
	s.spinLock.Acquire()
	
	log.Printf("Client %s acquired lock %s", req.ClientId, req.LockName)
	return &pb.LockAcquireResponse{
		ReturnCode: 0,
		Message:    "Lock acquired successfully",
	}, nil
}

// LockRelease handles lock release requests
func (s *LockServer) LockRelease(ctx context.Context, req *pb.LockReleaseRequest) (*pb.LockReleaseResponse, error) {
	log.Printf("Client %s is releasing lock %s", req.ClientId, req.LockName)
	
	// Release the lock
	s.spinLock.Release()
	
	log.Printf("Client %s released lock %s", req.ClientId, req.LockName)
	return &pb.LockReleaseResponse{
		ReturnCode: 0,
		Message:    "Lock released successfully",
	}, nil
}

// AppendFile handles file append requests
func (s *LockServer) AppendFile(ctx context.Context, req *pb.AppendFileRequest) (*pb.AppendFileResponse, error) {
	log.Printf("Client %s is appending to file %s", req.ClientId, req.FileName)
	
	// Check if the file exists
	filePath := filepath.Join(filesDirPath, req.FileName)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return &pb.AppendFileResponse{
			ReturnCode: 1,
			Message:    "File not found",
		}, nil
	}
	
	// Append data to the file
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return &pb.AppendFileResponse{
			ReturnCode: 1,
			Message:    "Error opening file",
		}, nil
	}
	defer file.Close()
	
	_, err = file.WriteString(req.Data)
	if err != nil {
		log.Printf("Error writing to file: %v", err)
		return &pb.AppendFileResponse{
			ReturnCode: 1,
			Message:    "Error writing to file",
		}, nil
	}
	
	log.Printf("Client %s successfully appended to file %s", req.ClientId, req.FileName)
	return &pb.AppendFileResponse{
		ReturnCode: 0,
		Message:    "Data appended successfully",
	}, nil
}

// Close handles client disconnection
func (s *LockServer) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.clientsLock.Lock()
	delete(s.clients, req.ClientId)
	s.clientsLock.Unlock()
	
	log.Printf("Client %s disconnected", req.ClientId)
	return &pb.CloseResponse{
		Message:    "Disconnected!",
		ReturnCode: 0,
	}, nil
}

// initializeFiles creates the initial set of files
func initializeFiles() error {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(filesDirPath, 0755); err != nil {
		return err
	}
	
	// Create 100 empty files
	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("file_%d", i)
		filePath := filepath.Join(filesDirPath, fileName)
		
		// Check if file already exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			file, err := os.Create(filePath)
			if err != nil {
				return err
			}
			file.Close()
		}
	}
	
	return nil
}

func main() {
	// Initialize files
	if err := initializeFiles(); err != nil {
		log.Fatalf("Failed to initialize files: %v", err)
	}
	
	// Create a listener on the specified port
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	
	// Create a new gRPC server
	grpcServer := grpc.NewServer()
	
	// Register our service
	lockServer := &LockServer{
		clients: make(map[string]bool),
	}
	pb.RegisterLockServiceServer(grpcServer, lockServer)
	
	log.Printf("Server started on port %s", port)
	
	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
