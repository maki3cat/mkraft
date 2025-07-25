package mkraft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft"
	"github.com/maki3cat/mkraft/mkraft/node"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/persister"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	pb "github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewServer(cfg *common.Config, logger *zap.Logger) (*Server, error) {
	membership, err := peers.NewMembershipWithStaticConfig(logger, cfg)
	if err != nil {
		return nil, err
	}

	nodeID := cfg.GetMembership().CurrentNodeID

	raftLog := persister.NewRaftLogsImplAndLoad(cfg.GetDataDir(), logger, nil)
	statemachine := plugs.NewStateMachineNoOpImpl()

	consensus := node.NewConsensus(logger, membership)
	n := node.NewNode(nodeID, cfg, logger, membership, statemachine, raftLog, consensus)

	handlers := mkraft.NewHandlers(logger, n)

	server := &Server{
		logger:     logger,
		cfg:        cfg,
		node:       n,
		membership: membership,
		handler:    handlers,
	}
	serverOptions := grpc.ChainUnaryInterceptor(
		server.contextCheckInterceptor,
		server.monitorInterceptor)
	server.grpcServer = grpc.NewServer(serverOptions)

	pb.RegisterRaftServiceServer(server.grpcServer, server.handler)
	return server, nil
}

type Server struct {
	logger     *zap.Logger
	cfg        *common.Config
	node       node.Node
	membership peers.Membership

	grpcServer *grpc.Server
	handler    *mkraft.Handlers
}

func (s *Server) contextCheckInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.New(codes.Canceled, "context done").Err()
	}
	return handler(ctx, req)
}

func (s *Server) monitorInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, requestID := common.GetOrGenerateRequestIDAtServer(ctx)
	s.logger.Debug("gRPC request", zap.String("requestID", requestID), zap.Any("request", req))
	resp, err := handler(ctx, req)
	if err != nil {
		s.logger.Error("gRPC response error", zap.String("requestID", requestID), zap.Error(err))
	} else {
		s.logger.Debug("gRPC response", zap.String("requestID", requestID), zap.Any("response", resp))
	}
	return resp, err
}

func (s *Server) Stop() {
	// grpc server graceful shutdown
	waitingDuration := s.cfg.GetGracefulShutdownTimeout()
	timer := time.AfterFunc(waitingDuration, func() {
		s.grpcServer.Stop()
	})
	defer timer.Stop()
	s.grpcServer.GracefulStop()
	s.membership.GracefulStop()
	s.node.GracefulStop()
}

// no blocking start
func (s *Server) Start(ctx context.Context) error {

	// start the gRPC server
	port := s.cfg.GetMembership().CurrentPort
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

	go func() {
		s.logger.Info("gRPC server is starting...")
		if err := s.grpcServer.Serve(lis); err != nil {
			if errors.Is(err, grpc.ErrServerStopped) {
				s.logger.Info("gRPC server has stopped")
				return
			} else {
				s.logger.Error("failed to serve", zap.Error(err))
				panic(err)
			}
		}
	}()

	go func() {
		s.logger.Info("waiting for context cancellation or server quit...")
		<-ctx.Done()
		s.Stop()
		s.logger.Info("context canceled, stopping gRPC server...")
	}()

	// start the node
	s.logger.Info("starting raft node...")
	s.node.Start(ctx)
	return nil
}
