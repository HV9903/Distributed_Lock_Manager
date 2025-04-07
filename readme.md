# Distributed Lock Manager (DLM) - Part 2: Fault Tolerance

## Overview

This project implements a fault-tolerant Distributed Lock Manager (DLM) that ensures mutual exclusion across multiple nodes in a distributed system. Part 2 builds upon the basic implementation from Part 1 by adding mechanisms to handle network failures and node crashes.

## Key Features

- **Retry Mechanism**: Clients automatically retry requests with exponential backoff when responses are lost
- **Request Deduplication**: Server ensures idempotency by tracking and properly handling duplicate requests
- **Lock Timeout**: Locks are automatically released after a timeout period to prevent indefinite holding
- **Fencing Tokens**: Monotonically increasing tokens protect against stale lock holders corrupting shared resources
- **State Persistence**: Server state is persisted to disk for recovery after crashes

## Architecture

The system consists of:

- **Client Library**: Provides functions for acquiring locks, releasing locks, and appending to files
- **Lock Server**: Manages lock state and handles client requests
- **Write-Ahead Log (WAL)**: Records all operations for crash recovery
- **State Snapshots**: Periodically captures server state for faster recovery

## Fault Tolerance Mechanisms

### Goal 1: Packet Loss Handling
The client library implements retry mechanisms with exponential backoff to handle network packet loss. If a request or response is lost, clients will retry the operation with increasing delays between attempts, ensuring liveness even in unreliable networks.

### Goal 2: Duplicate Request Handling
The server tracks processed requests using unique request IDs. When a duplicate request is received, the server returns the cached result without re-executing the operation, ensuring idempotency and preventing issues like double appends.

### Goal 3: Lock Timeout
Locks are automatically released after a configurable timeout period (default: 30 seconds). This prevents a crashed client from holding a lock indefinitely, allowing other clients to make progress.

### Goal 4: Fencing Token Protection
Each lock acquisition generates a monotonically increasing fencing token. Clients must include this token when performing operations, and the server rejects operations with stale tokens. This prevents a crashed client that comes back online from corrupting shared resources.

### Goal 5: Server Crash Recovery
The server persists its state using a combination of:
- Write-Ahead Logging (WAL) for durability
- Periodic state snapshots for faster recovery
- State reconciliation on startup

## Testing

Comprehensive test suites are provided to verify each fault tolerance mechanism:

- **test_goal1.go**: Tests retry mechanisms for packet loss
- **test_goal2.go**: Tests duplicate request handling
- **test_goal3.go**: Tests lock timeout behavior
- **test_goal4.go**: Tests fencing token protection
- **test_goal5.go**: Tests server crash recovery

## Usage

### Building the Project

```bash
make proto  # Generate gRPC code
make build  # Build server and client
```

### Running the Server

```bash
./bin/server
```

### Running Tests

```bash
./bin/test_goal1 localhost:50051
./bin/test_goal2 localhost:50051
./bin/test_goal3 localhost:50051
./bin/test_goal4 localhost:50051
./bin/test_goal5 localhost:50051
```
