package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

var logger = util.GetSugarLogger()

type server struct {
	pb.UnimplementedRaftServiceServer
}

func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	logger.Infof("Received: %v", in)
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	defaultPath := "./config.yaml"

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
	port := membershipConfig.CurrentPort

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalw("failed to listen", "error", err)
	}

	s := grpc.NewServer()
	pb.RegisterRaftServiceServer(s, &server{})
	logger.Infof("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			logger.Info("gRPC server stopped")
			return
		} else {
			logger.Errorw("failed to serve", "error", err)
			panic(err)
		}
	}
}
