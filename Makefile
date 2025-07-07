
.PHONY: clean-mocks test run protogen mockgen build clean test-nodes

all: clean build

test:
	go test -v ./...

run:
	echo "Running the main program..."
	go run main.go -c local/config1.yaml

gen: protogen mockgen

protogen:
	protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	echo "Protocol buffer files generated successfully."

mockgen: clean-mocks
	mockgen -source=rpc/service_grpc.pb.go -destination=./rpc/service_mock.go -package rpc
	mockgen -source=mkraft/node/node.go -destination=./mkraft/node/node_mock.go -package node
	mockgen -source=mkraft/node/consensus.go -destination=./mkraft/node/consensus_mock.go -package node

	mockgen -source=mkraft/peers/client.go -destination=./mkraft/peers/client_mock.go -package peers
	mockgen -source=mkraft/peers/membership.go -destination=./mkraft/peers/membership_mock.go -package peers

	mockgen -source=mkraft/plugs/statemachine.go -destination=./mkraft/plugs/statemachine_mock.go -package plugs

	mockgen -source=mkraft/log/raftlog.go -destination=./mkraft/log/raftlog_mock.go -package log
	mockgen -source=mkraft/log/serde.go -destination=./mkraft/log/serde_mock.go -package log

clean-mocks:
	find . -type f -name '*_mock.go' -exec rm -f {} +

build:
	echo "Building the project..."
	go build -o bin/mkraft cmd/main.go

clean:
	rm bin/*
	rm *.log *.pid

test-nodes: build
	echo "Clearning up the node data..."
	rm -rf ./data/node1/*
	rm -rf ./data/node2/*
	rm -rf ./data/node3/*
	echo "Starting mkraft nodes..."
	./bin/mkraft -c ./config/local/node1.yaml > ./data/node1/node.log 2>&1 & echo $$! > ./data/node1/node.pid
	./bin/mkraft -c ./config/local/node2.yaml > ./data/node2/node.log 2>&1 & echo $$! > ./data/node2/node.pid
	./bin/mkraft -c ./config/local/node3.yaml > ./data/node3/node.log 2>&1 & echo $$! > ./data/node3/node.pid
	echo "Nodes running for 30 seconds..."
	sleep 60
	echo "Stopping nodes..."
	-kill -15 $$(cat ./data/node1/node.pid)
	-kill -15 $$(cat ./data/node2/node.pid)
	-kill -15 $$(cat ./data/node3/node.pid)
	sleep 10
	rm -f ./data/node1/node.pid ./data/node2/node.pid ./data/node3/node.pid
	@ps aux | grep "mkraft"
	echo "All nodes stopped"