package main

import (
	"context"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func callHello() {
	conn, err := grpc.NewClient("localhost:18080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.GetSugarLogger().Errorw("error in creating grpc client", "error", err)
	}
	// todo: put it in graceful shutdown
	// defer conn.Close()
	rawClient := rpc.NewRaftServiceClient(conn)

	resp, err := rawClient.SayHello(context.Background(), &rpc.HelloRequest{
		Name: "test",
	})
	if err != nil {
		util.GetSugarLogger().Errorw("error in sending hello to member", "member", rawClient, "error", err)
	} else {
		util.GetSugarLogger().Info("hello response from member", "response", resp)
	}
}

func main() {
	callHello()
}
