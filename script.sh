

- generate the Go code from the proto file using protoc
```#!/bin/bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/mkraft/service.proto
```

protoc --go_out=. --go-grpc_out=. proto/mkraft/service.proto