package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"strings"

	pb "part_1/proto"
	"google.golang.org/grpc"
)

const (
	port         = ":50051"
	filesDirPath = "./files"
	numFiles     = 100
	walPath      = "./wal"
	snapshotPath = "./snapshot.json"
)

// RequestStatus represents the status of a request
type RequestStatus int

const (
	StatusPending RequestStatus = iota
	StatusCompleted
	StatusFailed
)

// RequestResult stores the result of a request
type RequestResult struct {
	Status   RequestStatus
	Response interface{}
	Error    string
	Timestamp time.Time
}

// RequestCache caches request results
type RequestCache struct {
	cache map[string]*RequestResult
	mu    sync.Mutex
}

// LockInfo stores information about a lock
type LockInfo struct {
	ClientID     string
	AcquiredTime time.Time
	Deadline     time.Time
	FencingToken int64
}

// Operation represents an operation in the WAL
type Operation struct {
	Type        string
	LockName    string
	ClientID    string
	RequestID   string
	Deadline    time.Time
	FencingToken int64
	Timestamp   time.Time
}

// ServerState represents the server state for persistence
type ServerState struct {
	Locks        map[string]*LockInfo
	RequestCache map[string]*RequestResult
	NextToken    int64
}

// LockServer implements the LockService gRPC service
type LockServer struct {
	pb.UnimplementedLockServiceServer
	locks        map[string]*LockInfo
	locksMutex   sync.Mutex
	requestCache RequestCache
	nextToken    int64
	lockTimeout  time.Duration
	walFile      *os.File
}

// NewLockServer creates a new lock server
func NewLockServer() *LockServer {
	server := &LockServer{
		locks:       make(map[string]*LockInfo),
		lockTimeout: 30 * time.Second,
	}
	server.requestCache.cache = make(map[string]*RequestResult)
	
	// Initialize WAL
	if err := os.MkdirAll(filepath.Dir(walPath), 0755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	
	var err error
	server.walFile, err = os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	
	// Recover state
	server.recoverState()
	
	// Start background tasks
	go server.checkExpiredLocks()
	go server.periodicSnapshot()
	go server.cleanupRequestCache()
	
	return server
}

// logOperation logs an operation to the WAL
func (s *LockServer) logOperation(op *Operation) error {
	op.Timestamp = time.Now()
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	
	data = append(data, '\n')
	_, err = s.walFile.Write(data)
	if err != nil {
		return err
	}
	
	return s.walFile.Sync()
}

// recoverState recovers the server state from snapshot and WAL
func (s *LockServer) recoverState() {
	// Try to load from snapshot first
	if data, err := os.ReadFile(snapshotPath); err == nil {
		var state ServerState
		if err := json.Unmarshal(data, &state); err == nil {
			s.locksMutex.Lock()
			s.locks = state.Locks
			s.nextToken = state.NextToken
			s.locksMutex.Unlock()
			
			s.requestCache.mu.Lock()
			s.requestCache.cache = state.RequestCache
			s.requestCache.mu.Unlock()
			
			log.Printf("Recovered state from snapshot with %d locks", len(state.Locks))
		}
	}
	
	// Replay WAL
	if data, err := os.ReadFile(walPath); err == nil {
		lines := []byte{}
		for _, b := range data {
			if b == '\n' {
				var op Operation
				if err := json.Unmarshal(lines, &op); err == nil {
					s.applyOperation(&op)
				}
				lines = []byte{}
			} else {
				lines = append(lines, b)
			}
		}
		log.Printf("Replayed WAL operations")
	}
}

// applyOperation applies an operation to the server state
func (s *LockServer) applyOperation(op *Operation) {
	s.locksMutex.Lock()
	defer s.locksMutex.Unlock()
	
	switch op.Type {
	case "acquire":
		s.locks[op.LockName] = &LockInfo{
			ClientID:     op.ClientID,
			AcquiredTime: op.Timestamp,
			Deadline:     op.Deadline,
			FencingToken: op.FencingToken,
		}
		if op.FencingToken >= s.nextToken {
			s.nextToken = op.FencingToken + 1
		}
	case "release":
		delete(s.locks, op.LockName)
	case "extend":
		if lock, exists := s.locks[op.LockName]; exists && lock.ClientID == op.ClientID {
			lock.Deadline = op.Deadline
		}
	}
}

// periodicSnapshot periodically takes a snapshot of the server state
func (s *LockServer) periodicSnapshot() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		s.takeSnapshot()
	}
}

// takeSnapshot takes a snapshot of the server state
func (s *LockServer) takeSnapshot() {
	s.locksMutex.Lock()
	s.requestCache.mu.Lock()
	
	state := ServerState{
		Locks:        s.locks,
		RequestCache: s.requestCache.cache,
		NextToken:    s.nextToken,
	}
	
	s.locksMutex.Unlock()
	s.requestCache.mu.Unlock()
	
	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}
	
	tempFile := snapshotPath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		log.Printf("Failed to write snapshot: %v", err)
		return
	}
	
	if err := os.Rename(tempFile, snapshotPath); err != nil {
		log.Printf("Failed to rename snapshot: %v", err)
	}
}

// cleanupRequestCache periodically cleans up old request cache entries
func (s *LockServer) cleanupRequestCache() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	
	for range ticker.C {
		s.requestCache.mu.Lock()
		now := time.Now()
		for key, result := range s.requestCache.cache {
			// Remove entries older than 24 hours
			if now.Sub(result.Timestamp) > 24*time.Hour {
				delete(s.requestCache.cache, key)
			}
		}
		s.requestCache.mu.Unlock()
	}
}

// checkExpiredLocks periodically checks for expired locks
func (s *LockServer) checkExpiredLocks() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		s.locksMutex.Lock()
		now := time.Now()
		for lockName, lockInfo := range s.locks {
			if now.After(lockInfo.Deadline) {
				log.Printf("Lock %s expired for client %s", lockName, lockInfo.ClientID)
				
				// Log the release operation
				op := &Operation{
					Type:      "release",
					LockName:  lockName,
					ClientID:  lockInfo.ClientID,
					Timestamp: now,
				}
				s.logOperation(op)
				
				// Remove the lock
				delete(s.locks, lockName)
			}
		}
		s.locksMutex.Unlock()
	}
}

// generateFencingToken generates a new fencing token
func (s *LockServer) generateFencingToken() int64 {
	return atomic.AddInt64(&s.nextToken, 1)
}

// processRequest processes a request with deduplication
func (s *LockServer) processRequest(clientID, requestID, operationType string, handler func() (interface{}, error)) (interface{}, error) {
    if requestID == "" {
        return nil, fmt.Errorf("request ID cannot be empty")
    }
    
    cacheKey := fmt.Sprintf("%s:%s:%s", clientID, operationType, requestID)
    
    // Check if we've seen this request before
    s.requestCache.mu.Lock()
    if result, exists := s.requestCache.cache[cacheKey]; exists {
        s.requestCache.mu.Unlock()
        
        if result.Status == StatusPending {
            // Request is still being processed
            // Return a response appropriate for the operation type
            if strings.HasPrefix(operationType, "acquire") {
                return &pb.LockAcquireResponse{
                    ReturnCode: 2, // In progress
                    Message:    "Request is being processed",
                }, nil
            } else if strings.HasPrefix(operationType, "append") {
                return &pb.AppendFileResponse{
                    ReturnCode: 2, // In progress
                    Message:    "Request is being processed",
                }, nil
            } else if strings.HasPrefix(operationType, "release") {
                return &pb.LockReleaseResponse{
                    ReturnCode: 2, // In progress
                    Message:    "Request is being processed",
                }, nil
            } else {
                return nil, fmt.Errorf("request is being processed")
            }
        } else if result.Status == StatusFailed {
            // Request failed previously
            return nil, fmt.Errorf(result.Error)
        } else {
            // Request completed successfully
            // Use type assertion with safety check
            return result.Response, nil
        }
    }
    
    // Create a pending entry
    result := &RequestResult{
        Status:    StatusPending,
        Timestamp: time.Now(),
    }
    s.requestCache.cache[cacheKey] = result
    s.requestCache.mu.Unlock()
    
    // Process the request
    response, err := handler()
    
    // Update the cache entry
    s.requestCache.mu.Lock()
    if err != nil {
        result.Status = StatusFailed
        result.Error = err.Error()
    } else {
        result.Status = StatusCompleted
        result.Response = response
    }
    s.requestCache.mu.Unlock()
    
    return response, err
}


// Init handles the initialization request from clients
func (s *LockServer) Init(ctx context.Context, req *pb.InitRequest) (*pb.InitResponse, error) {
	response, err := s.processRequest(req.ClientId, req.RequestId, "init", func() (interface{}, error) {
		log.Printf("Client %s connected", req.ClientId)
		return &pb.InitResponse{
			Message:    "Connected!",
			ReturnCode: 0,
		}, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return response.(*pb.InitResponse), nil
}

// LockAcquire handles lock acquisition requests
func (s *LockServer) LockAcquire(ctx context.Context, req *pb.LockAcquireRequest) (*pb.LockAcquireResponse, error) {
	response, err := s.processRequest(req.ClientId, req.RequestId, "acquire:"+req.LockName, func() (interface{}, error) {
		s.locksMutex.Lock()
		defer s.locksMutex.Unlock()
		
		// Check if lock is already held
		if lock, exists := s.locks[req.LockName]; exists {
			if time.Now().Before(lock.Deadline) {
				return &pb.LockAcquireResponse{
					ReturnCode: 1,
					Message:    "Lock already held by another client",
				}, nil
			}
			// Lock has expired, release it
			delete(s.locks, req.LockName)
		}
		
		// Generate a new fencing token
		token := s.generateFencingToken()
		deadline := time.Now().Add(s.lockTimeout)
		
		// Log the operation
		op := &Operation{
			Type:        "acquire",
			LockName:    req.LockName,
			ClientID:    req.ClientId,
			RequestID:   req.RequestId,
			Deadline:    deadline,
			FencingToken: token,
		}
		if err := s.logOperation(op); err != nil {
			return nil, fmt.Errorf("failed to log operation: %v", err)
		}
		
		// Grant the lock
		s.locks[req.LockName] = &LockInfo{
			ClientID:     req.ClientId,
			AcquiredTime: time.Now(),
			Deadline:     deadline,
			FencingToken: token,
		}
		
		log.Printf("Client %s acquired lock %s with token %d", req.ClientId, req.LockName, token)
		
		return &pb.LockAcquireResponse{
			ReturnCode:   0,
			Message:      "Lock acquired successfully",
			FencingToken: token,
		}, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return response.(*pb.LockAcquireResponse), nil
}

// LockRelease handles lock release requests
func (s *LockServer) LockRelease(ctx context.Context, req *pb.LockReleaseRequest) (*pb.LockReleaseResponse, error) {
	response, err := s.processRequest(req.ClientId, req.RequestId, "release:"+req.LockName, func() (interface{}, error) {
		s.locksMutex.Lock()
		defer s.locksMutex.Unlock()
		
		// Check if lock exists and is held by the client
		lock, exists := s.locks[req.LockName]
		if !exists {
			return &pb.LockReleaseResponse{
				ReturnCode: 1,
				Message:    "Lock not found",
			}, nil
		}
		
		if lock.ClientID != req.ClientId {
			return &pb.LockReleaseResponse{
				ReturnCode: 1,
				Message:    "Lock not held by client",
			}, nil
		}
		
		// Check fencing token
		if lock.FencingToken != req.FencingToken {
			return &pb.LockReleaseResponse{
				ReturnCode: 1,
				Message:    "Invalid fencing token",
			}, nil
		}
		
		// Log the operation
		op := &Operation{
			Type:      "release",
			LockName:  req.LockName,
			ClientID:  req.ClientId,
			RequestID: req.RequestId,
		}
		if err := s.logOperation(op); err != nil {
			return nil, fmt.Errorf("failed to log operation: %v", err)
		}
		
		// Release the lock
		delete(s.locks, req.LockName)
		
		log.Printf("Client %s released lock %s", req.ClientId, req.LockName)
		
		return &pb.LockReleaseResponse{
			ReturnCode: 0,
			Message:    "Lock released successfully",
		}, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return response.(*pb.LockReleaseResponse), nil
}

// AppendFile handles file append requests
func (s *LockServer) AppendFile(ctx context.Context, req *pb.AppendFileRequest) (*pb.AppendFileResponse, error) {
	response, err := s.processRequest(req.ClientId, req.RequestId, "append:"+req.FileName+":"+req.Data, func() (interface{}, error) {
		// Verify fencing token
		s.locksMutex.Lock()
		validToken := false
		for _, lock := range s.locks {
			if lock.ClientID == req.ClientId && lock.FencingToken == req.FencingToken {
				validToken = true
				break
			}
		}
		s.locksMutex.Unlock()
		
		if !validToken {
			return &pb.AppendFileResponse{
				ReturnCode: 1,
				Message:    "Invalid or stale fencing token",
			}, nil
		}
		
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
	})
	
	if err != nil {
		return nil, err
	}
	
	return response.(*pb.AppendFileResponse), nil
}

// Close handles client disconnection
func (s *LockServer) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	response, err := s.processRequest(req.ClientId, req.RequestId, "close", func() (interface{}, error) {
		log.Printf("Client %s disconnected", req.ClientId)
		return &pb.CloseResponse{
			Message:    "Disconnected!",
			ReturnCode: 0,
		}, nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return response.(*pb.CloseResponse), nil
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
    lockServer := NewLockServer()
    pb.RegisterLockServiceServer(grpcServer, lockServer)
    
    log.Printf("Server started on port %s", port)
    
    // Start serving
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
