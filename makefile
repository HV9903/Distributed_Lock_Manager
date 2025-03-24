.PHONY: all proto build clean run-server run-client

all: proto build

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/lock.proto

build:
	go build -o bin/server server.go
	go build -o bin/test_client test_client.go

clean:
	rm -rf bin/
	rm -rf files/

server: build
	./bin/server

client: build
	./bin/test_client localhost:50051
