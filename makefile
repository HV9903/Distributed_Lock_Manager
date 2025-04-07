.PHONY: all proto build clean run-server run-client

all: proto build

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/lock.proto

build:
	go build -o bin/server server.go
	go build -o bin/test_client test_client.go
	go build -o bin/test_goal1 test_goal1.go
	go build -o bin/test_goal2 test_goal2.go
	go build -o bin/test_goal3 test_goal3.go
	go build -o bin/test_goal4 test_goal4.go
	go build -o bin/test_goal5 test_goal5.go


clean:
	rm -rf bin/
	rm -rf files/

server: build
	./bin/server

client: build
	./bin/test_client localhost:50051
