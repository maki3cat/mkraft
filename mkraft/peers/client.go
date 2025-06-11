package peers

import (
	"context"
	"time"

	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/google/uuid"
	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
)

var _ PeerClient = (*peerClient)(nil)

type PeerClient interface {

	// 1. it has forever retry until the context is done or the response is received
	// 2. it has retry logic to handle the rpc timeout -> which is handled by the timeoutClientInterceptor
	// 3. it is synchronous call
	RequestVoteWithRetry(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error)

	// implementation gap:
	// currently, the retry logic is simpler for append entries that we retry 3 times
	AppendEntriesWithRetry(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error)

	Close() error
}

type peerClient struct {
	nodeId    string
	nodeAddr  string
	rawClient rpc.RaftServiceClient
	conn      *grpc.ClientConn
	logger    *zap.Logger
	cfg       common.ConfigIface
}

func NewPeerClientImpl(
	nodeID, nodeAddr string, logger *zap.Logger, cfg common.ConfigIface) (*peerClient, error) {
	client := &peerClient{
		nodeId:   nodeID,
		nodeAddr: nodeAddr,
		logger:   logger,
		cfg:      cfg,
	}

	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(client.loggerInterceptor),
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

func (rc *peerClient) Close() error {
	if rc.conn != nil {
		err := rc.conn.Close()
		if err != nil {
			rc.logger.Error("failed to close gRPC connection", zap.String("nodeID", rc.nodeId), zap.Error(err))
		}
		return err
	}
	rc.logger.Warn("gRPC connection is nil, cannot close")
	return nil
}

func (rc *peerClient) RequestVoteWithRetry(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	requestID := common.GetRequestID(ctx)
	rc.logger.Debug("send SendRequestVote",
		zap.Any("request", req),
		zap.String("requestID", requestID))
	for {
		select {
		case <-ctx.Done():
			return nil, common.ContextDoneErr()
		default:
			singleResChan := rc.asyncCallRequestVote(ctx, req)
			select {
			case <-ctx.Done():
				return nil, common.ContextDoneErr()
			case resp := <-singleResChan:
				if resp.Err != nil {
					rc.logger.Error("need retry, RPC error:",
						zap.Error(resp.Err),
						zap.String("requestID", requestID))
					deadline, ok := ctx.Deadline()
					if ok && time.Until(deadline) < rc.cfg.GetRPCDeadlineMargin() {
						return nil, resp.Err
					} else {
						continue
					}
				} else {
					return resp.Resp, nil
				}
			}
		}
	}
}

// broadcast timeout is used in this caller
func (rc *peerClient) asyncCallRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) chan utils.RPCRespWrapper[*rpc.RequestVoteResponse] {
	singleResChan := make(chan utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1) // must be buffered
	go func() {
		resp, err := rc.rawClient.RequestVote(ctx, req)
		if err != nil {
			requestID := common.GetRequestID(ctx)
			rc.logger.Error("single RPC error in SendAppendEntries:",
				zap.Error(err),
				zap.String("requestID", requestID))
		}
		wrapper := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
			Resp: resp,
			Err:  err,
		}
		singleResChan <- wrapper
	}()
	return singleResChan
}

func (rc *peerClient) AppendEntriesWithRetry(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	const maxRetries = 3
	var lastErr error

	for i := range maxRetries {
		resp, err := rc.rawClient.AppendEntries(ctx, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		requestID := common.GetRequestID(ctx)
		rc.logger.Error("RPC error in AppendEntries, will retry:",
			zap.Error(err),
			zap.String("requestID", requestID),
			zap.Int("attempt", i+1),
			zap.Int("maxRetries", maxRetries))

		// Check if we should continue retrying
		select {
		case <-ctx.Done():
			return nil, common.ContextDoneErr()
		default:
			// Continue to next retry
		}
	}
	return nil, lastErr
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
func (rc *peerClient) timeoutClientInterceptor(
	ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	requestID := uuid.New().String()
	start := time.Now()
	rc.logger.Debug("Starting RPC call",
		zap.String("method", method),
		zap.String("requestID", requestID))

	rpcTimeout := rc.cfg.GetRPCRequestTimeout()
	singleCallCtx, singleCallCancel := context.WithTimeout(ctx, rpcTimeout)
	defer singleCallCancel()

	err := invoker(singleCallCtx, method, req, reply, cc, opts...)

	end := time.Now()
	rc.logger.Debug("Finished RPC call",
		zap.String("method", method),
		zap.String("requestID", requestID),
		zap.Duration("duration", end.Sub(start)))

	return err
}

func (rc *peerClient) loggerInterceptor(
	ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	ctx, requestID := common.SetClientRequestID(ctx)

	rc.logger.Debug("Starting RPC call",
		zap.String("method", method),
		zap.Any("request", req),
		zap.String("target", cc.Target()),
		zap.String("requestID", requestID))

	err := invoker(ctx, method, req, reply, cc, opts...)

	if err != nil {
		rc.logger.Error("RPC call error",
			zap.String("method", method),
			zap.Error(err),
			zap.Any("request", req),
			zap.String("requestID", requestID))
	} else {
		rc.logger.Debug("RPC call has succeeded",
			zap.String("method", method),
			zap.Any("request", req),
			zap.Any("response", reply),
			zap.Error(err),
			zap.String("requestID", requestID))
	}
	return err
}
