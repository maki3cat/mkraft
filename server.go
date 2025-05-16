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
	"github.com/maki3cat/mkraft/mkraft/pluggable"
	pb "github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewServer(cfg common.ConfigIface, logger *zap.Logger) (*Server, error) {
	membershipMgr, err := mkraft.NewMembershipMgrWithStaticConfig(logger, cfg)
	if err != nil {
		return nil, err
	}

	raftLogIface := mkraft.NewRaftLogsImpl(cfg.GetRaftLogFilePath())
	statemachine := pluggable.NewStateMachineNoOpImpl()

	nodeID := cfg.GetMembership().CurrentNodeID
	node := mkraft.NewNode(nodeID, cfg, logger, membershipMgr, raftLogIface, statemachine)
	handlers := mkraft.NewHandlers(logger, node)

	server := &Server{
		logger:     logger,
		cfg:        cfg,
		node:       node,
		membership: membershipMgr,
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
	cfg        common.ConfigIface
	node       mkraft.NodeIface
	membership mkraft.MembershipMgrIface

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
}

// no blocking start
func (s *Server) Start(ctx context.Context) error {

	// start the gRPC server
	port := s.cfg.GetMembership().CurrentPort
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
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
