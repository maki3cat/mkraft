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

var logger = util.GetSugarLogger()

func PingMembers(ctx context.Context) {
	members, err := raft.GetAllPeerClients()
	if err != nil {
		logger.Fatal("error in getting all peer clients", err)
	}
	ticker := time.NewTicker(time.Second * time.Duration(10))
	defer ticker.Stop()
	name := raft.GetCurrentNodeID()
	logger.Infof("start pinging members: %v", members)
	for {
		select {
		case <-ctx.Done():
			logger.Info("context canceled, stopping pinging members...")
			return
		case <-ticker.C:
			logger.Debug("issue hello to all members")
			for _, memberCli := range members {
				logger.Debugw("member clients", "memberCli", memberCli)
				resp, err := memberCli.SayHello(context.Background(), &pb.HelloRequest{
					Name: "this is " + name,
				})
				if err != nil {
					logger.Infow("error in sending hello to member", "member", memberCli, "error", err)
				} else {
					logger.Debugw("hello response from member", "response", resp)
				}
			}
		}
	}
}

// maki: gogymnastics pattern serving and gracefully shutdown
func startRPCServer(ctx context.Context, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalw("failed to listen", "error", err)
	}

	s := grpc.NewServer()
	pb.RegisterRaftServiceServer(s, &Server{})

	go func() {
		logger.Infof("serving gRPC at %v...", port)
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
	go PingMembers(ctx)

	raft.StartRaftNode(ctx)

	sig := <-signalChan
	logger.Warn("\nReceived signal: %s\n", sig)
	cancel()
	time.Sleep(2 * time.Second)
	logger.Warn("Main exiting")
}
