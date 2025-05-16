package mkraft

import (
	"context"

	"github.com/maki3cat/mkraft/common"
	pb "github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type Handlers struct {
	pb.UnimplementedRaftServiceServer
	logger *zap.Logger
	node   NodeIface
}

func NewHandlers(logger *zap.Logger, node NodeIface) *Handlers {
	return &Handlers{
		logger: logger,
		node:   node,
	}
}

func (h *Handlers) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (h *Handlers) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	requestID := common.GetRequestID(ctx)
	respChan := make(chan *RPCRespWrapper[*pb.RequestVoteResponse], 1)
	internalReq := &RequestVoteInternalReq{
		Req:      in,
		RespChan: respChan,
	}
	// todo: should send the ctx into raft server so that it can notice the context is done
	h.node.VoteRequest(internalReq)
	resp := <-respChan
	if resp.Err != nil {
		h.logger.Error("error in getting response from raft server",
			zap.Error(resp.Err),
			zap.String("requestID", requestID))
		return nil, resp.Err
	}
	return resp.Resp, nil
}

func (h *Handlers) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	requestID := common.GetRequestID(ctx)
	respChan := make(chan *RPCRespWrapper[*pb.AppendEntriesResponse], 1)
	req := &AppendEntriesInternalReq{
		Req:      in,
		RespChan: respChan,
	}
	h.node.AppendEntryRequest(req)

	// todo: should send the ctx into raft server so that it can notice the context is done
	resp := <-respChan
	if resp.Err != nil {
		h.logger.Error("error in getting response from raft server",
			zap.Error(resp.Err),
			zap.String("requestID", requestID))
		return nil, resp.Err
	}
	return resp.Resp, nil
}

func (h *Handlers) ClientCommand(ctx context.Context, in *pb.ClientCommandRequest) (*pb.ClientCommandResponse, error) {

	requestID := common.GetRequestID(ctx)
	req := &ClientCommandInternalReq{
		Req:      in,
		RespChan: make(chan *RPCRespWrapper[*pb.ClientCommandResponse], 1),
	}
	h.node.ClientCommand(req)

	resp := <-req.RespChan
	if resp.Err != nil {
		h.logger.Error("error in getting response from raft server",
			zap.Error(resp.Err),
			zap.String("requestID", requestID))
		return nil, resp.Err
	}
	return resp.Resp, nil
}
