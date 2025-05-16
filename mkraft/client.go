package mkraft

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/google/uuid"
	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
)

var _ InternalClientIface = (*InternalClientImpl)(nil)

type InternalClientIface interface {

	// send request vote is keep retrying until the context is done or the response is received
	SendRequestVoteWithRetries(ctx context.Context, req *rpc.RequestVoteRequest) chan RPCRespWrapper[*rpc.RequestVoteResponse]

	// send append entries is one simple sync rpc call with rpc timeout
	SendAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) RPCRespWrapper[*rpc.AppendEntriesResponse]

	SayHello(ctx context.Context, req *rpc.HelloRequest) (*rpc.HelloReply, error)

	String() string

	Close() error
}

type InternalClientImpl struct {
	nodeId    string
	nodeAddr  string
	rawClient rpc.RaftServiceClient
	conn      *grpc.ClientConn
	logger    *zap.Logger
	cfg       common.ConfigIface
}

func NewInternalClient(
	nodeID, nodeAddr string, logger *zap.Logger, cfg common.ConfigIface) (InternalClientIface, error) {
	client := &InternalClientImpl{
		nodeId:   nodeID,
		nodeAddr: nodeAddr,
		logger:   logger,
		cfg:      cfg,
	}

	clientOptions := []grpc.DialOption{
		// todo: add creds
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
	client.rawClient = rpc.NewRaftServiceClient(conn)
	return client, nil
}

func (rc *InternalClientImpl) Close() error {
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

func (rc *InternalClientImpl) String() string {
	return fmt.Sprintf("InternalClientImpl: %s, %s", rc.nodeId, rc.nodeAddr)
}

func (rc *InternalClientImpl) SayHello(ctx context.Context, req *rpc.HelloRequest) (*rpc.HelloReply, error) {
	return rc.rawClient.SayHello(ctx, req)
}

func (rc *InternalClientImpl) SendAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) RPCRespWrapper[*rpc.AppendEntriesResponse] {
	resp, err := rc.syncCallAppendEntries(ctx, req)
	wrapper := RPCRespWrapper[*rpc.AppendEntriesResponse]{
		Resp: resp,
		Err:  err,
	}
	return wrapper
}

// the context shall be timed out in election timeout period
func (rc *InternalClientImpl) SendRequestVoteWithRetries(ctx context.Context, req *rpc.RequestVoteRequest) chan RPCRespWrapper[*rpc.RequestVoteResponse] {
	requestID := common.GetRequestID(ctx)
	rc.logger.Debug("send SendRequestVote",
		zap.String("to", rc.String()),
		zap.Any("request", req),
		zap.String("requestID", requestID))
	out := make(chan RPCRespWrapper[*rpc.RequestVoteResponse], 1)

	retriedRPC := func() {
		singleResChan := rc.asyncCallRequestVote(ctx, req)
		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*rpc.RequestVoteResponse]{
					Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
				return
			case resp := <-singleResChan:
				if resp.Err != nil {
					deadline, ok := ctx.Deadline()
					if ok && time.Until(deadline) < rc.cfg.GetMinRemainingTimeForRPC() {
						out <- RPCRespWrapper[*rpc.RequestVoteResponse]{
							Err: fmt.Errorf("%s", "election timeout to receive any non-error response")}
						return
					} else {
						rc.logger.Error("need retry, RPC error:",
							zap.String("to", rc.String()),
							zap.Error(resp.Err),
							zap.String("requestID", requestID))
						singleResChan = rc.asyncCallRequestVote(ctx, req)
						continue
					}
				} else {
					out <- resp
					return
				}
			}
		}
	}
	go retriedRPC()
	return out
}

func (rc *InternalClientImpl) asyncCallRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) chan RPCRespWrapper[*rpc.RequestVoteResponse] {
	singleResChan := make(chan RPCRespWrapper[*rpc.RequestVoteResponse], 1) // must be buffered
	go func() {
		resp, err := rc.syncCallRequestVote(ctx, req)
		wrapper := RPCRespWrapper[*rpc.RequestVoteResponse]{
			Resp: resp,
			Err:  err,
		}
		singleResChan <- wrapper
	}()
	return singleResChan
}

// should be called when ctx.deadline can handle the rpc timeout
func (rc *InternalClientImpl) syncCallAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	resp, err := rc.rawClient.AppendEntries(ctx, req)
	if err != nil {
		requestID := common.GetRequestID(ctx)
		rc.logger.Error("single RPC error in SendAppendEntries:",
			zap.Error(err),
			zap.String("requestID", requestID))
	}
	return resp, err
}

// should be called when ctx.deadline can handle the rpc timeout
func (rc *InternalClientImpl) syncCallRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	resp, err := rc.rawClient.RequestVote(ctx, req)
	if err != nil {
		requestID := common.GetRequestID(ctx)
		rc.logger.Error("single RPC error in SendAppendEntries:",
			zap.String("to", rc.String()),
			zap.Error(err),
			zap.String("requestID", requestID))
	}
	return resp, err
}

func (rc *InternalClientImpl) loggerInterceptor(
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

func (rc *InternalClientImpl) timeoutClientInterceptor(
	ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// todo: add trace all together and remove this temporary solution
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
