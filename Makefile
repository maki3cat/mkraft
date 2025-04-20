default:
	@go test -v ./...

run:
	@echo "Running the main program..."
	@go run main.go -c local/config1.yaml

protogen:
	@protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto
	@echo "Protocol buffer files generated successfully."

