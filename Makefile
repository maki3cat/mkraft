
.PHONY: clean-mocks test run protogen mockgen build clean test-nodes

all: clean build

test:
	go test -v ./...

run:
	echo "Running the main program..."
	go run main.go -c local/config1.yaml

protogen:
	protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	echo "Protocol buffer files generated successfully."

mockgen: clean-mocks
	mockgen -source=rpc/service_grpc.pb.go -destination=./rpc/service_mock.go -package rpc
	mockgen -source=common/config.go -destination=./common/config_mock.go -package common
	mockgen -source=mkraft/client.go -destination=./mkraft/client_mock.go -package mkraft
	mockgen -source=mkraft/membership.go -destination=./mkraft/membership_mock.go -package mkraft
	mockgen -source=mkraft/consensus.go -destination=./mkraft/consensus_mock.go -package mkraft
	mockgen -source=mkraft/node.go -destination=./mkraft/node_mock.go -package mkraft
	mockgen -source=mkraft/raftlog.go -destination=./mkraft/raftlog_mock.go -package mkraft
clean-mocks:
	find . -type f -name '*_mock.go' -exec rm -f {} +

build:
	echo "Building the project..."
	go build -o bin/mkraft cmd/main.go

clean:
	rm bin/*
	rm *.log *.pid

test-nodes: build
	echo "Starting mkraft nodes..."
	./bin/mkraft -c ./config/local/base.yaml > node1.log 2>&1 & echo $$! > node1.pid
	./bin/mkraft -c ./config/local/node2.yaml > node2.log 2>&1 & echo $$! > node2.pid
	./bin/mkraft -c ./config/local/node3.yaml > node3.log 2>&1 & echo $$! > node3.pid
	echo "Nodes running for 30 seconds..."
	sleep 30
	echo "Stopping nodes..."
	-kill -15 $$(cat node1.pid)
	-kill -15 $$(cat node2.pid) 
	-kill -15 $$(cat node3.pid)
	sleep 10
	rm -f node1.pid node2.pid node3.pid
	@ps aux | grep "mkraft"
	echo "All nodes stopped"
