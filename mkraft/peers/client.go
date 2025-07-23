package peers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc/codes"
)

var _ PeerClient = (*peerClient)(nil)

type PeerClient interface {
	AppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error)
	RequestVote(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error)
	GetNodeID() string
	PeerConnCheck(ctx context.Context) bool
	Close() error
}

type peerClient struct {
	nodeId    string
	nodeAddr  string
	rawClient rpc.RaftServiceClient
	conn      *grpc.ClientConn
	logger    *zap.Logger
	cfg       *common.Config
}

func NewPeerClientImpl(
	nodeID, nodeAddr string, logger *zap.Logger, cfg *common.Config) (*peerClient, error) {
	client := &peerClient{
		nodeId:   nodeID,
		nodeAddr: nodeAddr,
		logger:   logger,
		cfg:      cfg,
	}

	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(client.timeoutClientInterceptor),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		grpc.WithDefaultServiceConfig(cfg.GetgRPCServiceConf()),
	}
	conn, err := grpc.NewClient(nodeAddr, clientOptions...)
	if err != nil {
		logger.Error("failed to create gRPC connection", zap.String("nodeID", nodeID), zap.String("nodeAddr", nodeAddr), zap.Error(err))
		return nil, err
	}
	client.conn = conn
	client.rawClient = rpc.NewRaftServiceClient(conn)
	return client, nil
}

func (rc *peerClient) PeerConnCheck(ctx context.Context) bool {
	_, err := rc.rawClient.SayHello(ctx, &rpc.HelloRequest{Name: "ping"})
	if err != nil {
		code := status.Code(err)
		if code == codes.Unavailable {
			return false
		}
		// currently we don't treat other errors as broken conn
		return true
	}
	return true
}

func (rc *peerClient) GetNodeID() string {
	return rc.nodeId
}

func (rc *peerClient) Close() error {
	if rc.conn != nil {
		err := rc.conn.Close()
		if err != nil {
			rc.logger.Error("failed to close gRPC connection", zap.String("nodeID", rc.nodeId), zap.Error(err))
		}
		return err
	}
	rc.logger.Warn("gRPC connection is nil, close is a no-op")
	return nil
}

func (rc *peerClient) RequestVote(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	return rc.rawClient.RequestVote(ctx, req)
}

func (rc *peerClient) AppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	return rc.rawClient.AppendEntries(ctx, req)
}

// timeoutClientInterceptor enforces a timeout on RPC calls. This interceptor is used
// in NewRobustClient() when creating the gRPC client connection:
//
//	conn, err := grpc.Dial(
//		address,
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//		grpc.WithUnaryInterceptor(chainUnaryInterceptors(
//			rc.timeoutClientInterceptor,
//			rc.loggerInterceptor,
//		)),
//	)
//
// both logger and timeout are handled in this interceptor
func (rc *peerClient) timeoutClientInterceptor(
	ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	requestID := uuid.New().String()
	start := time.Now()
	rpcTimeout := rc.cfg.GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()
	rc.logger.Debug("TimeoutClientInterceptor: Starting RPC call",
		zap.String("method", method),
		zap.String("timeout", rpcTimeout.String()),
		zap.String("target", cc.Target()),
		zap.String("requestID", requestID))

	err := invoker(singleCallCtx, method, req, reply, cc, opts...)

	end := time.Now()
	if err != nil {
		rc.logger.Error("TimeoutClientInterceptor: RPC call error",
			zap.String("method", method),
			zap.Error(err),
			zap.Any("request", req),
			zap.String("requestID", requestID))
	} else {
		rc.logger.Debug("TimeoutClientInterceptor: Finished RPC call",
			zap.String("method", method),
			zap.String("requestID", requestID),
			zap.String("reply", fmt.Sprintf("%+v", reply)),
			zap.Duration("duration", end.Sub(start)))
	}
	return err
}
