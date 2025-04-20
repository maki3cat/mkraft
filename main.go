package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

// THE RPC SERVER, which is different from the RAFT SERVER
// server interface
// shall be implemented by a grpc server and try to make an abstract interface
type RPCServerIface interface {
	SendRequestVote(request pb.RequestVoteRequest) pb.RequestVoteResponse
	SendAppendEntries(request pb.AppendEntriesRequest) pb.AppendEntriesResponse
}

/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.
// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedRaftServiceServer
}

var logger = util.GetSugarLogger()

// maki: gogymnastics pattern serving and gracefully shutdown
func startRPCServer(ctx context.Context, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalw("failed to listen", "error", err)
	}

	s := grpc.NewServer()
	pb.RegisterRaftServiceServer(s, &server{})
	logger.Infof("server listening at %v", lis.Addr())

	go func() {
		logger.Info("starting gRPC server...")
		if err := s.Serve(lis); err != nil {
			if errors.Is(err, grpc.ErrServerStopped) {
				logger.Info("gRPC server stopped")
				return
			} else {
				logger.Errorw("failed to serve", "error", err)
				panic(err)
			}
		}
	}()

	go func() {
		logger.Info("waiting for context cancellation or server quit...")
		<-ctx.Done()
		logger.Info("context canceled, stopping gRPC server...")
		s.GracefulStop() // or s.Stop() for immediate stop
	}()
}

func main() {
	logger := util.GetSugarLogger()
	defaultPath := "./config/config1.yaml"

	// read config from the yaml file
	configPath := flag.String("c", "", "the path of the config file")
	if *configPath == "" {
		*configPath = defaultPath
	}
	flag.Parse()
	logger.Info("config file path: ", *configPath)
	membershipConfig := &raft.Membership{}
	yamlFile, err := os.ReadFile(*configPath)
	if err != nil {
		logger.Fatalf("yamlFile.Get err  #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, membershipConfig)
	if err != nil {
		logger.Fatalf("Unmarshal: %v", err)
	}
	logger.Infof("Config: %v", membershipConfig)

	raft.InitGlobalMembershipWithStaticConfig(membershipConfig)

	// signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// maki: gogymnastics pattern can wait on multiple context
	// start raft and rpc servers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startRPCServer(ctx, membershipConfig.CurrentPort)
	raft.StartRaftNode(ctx)

	sig := <-signalChan
	logger.Warn("\nReceived signal: %s\n", sig)
	cancel()
	time.Sleep(2 * time.Second)
	logger.Warn("Main exiting")
}
