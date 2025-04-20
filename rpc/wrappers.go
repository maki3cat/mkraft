// This file defines the RPC client interface and its implementation
// the RPC client is responsible for sending the RPC requests to only one other node and try to get a response
// it doesn't manage the consensus between different nodes
// TODO: maki: need to learn and revist the future and retry pattern here
// golang gymnastics
// summarize the golang patterns here:
// 1) async call with future; 2) retry pattern; 3) context with timeout/cancel; 4)template and generics

// paper: the 2 RPCs defined by the Raft paper
// membership module shall be responsible for the client to each member

package rpc

import (
	"context"
	"fmt"
	"time"

	util "github.com/maki3cat/mkraft/util"
)

var logger = util.GetSugarLogger()

type RPCRespWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	*AppendEntriesResponse | *RequestVoteResponse
}

// the real RPC wrapper used directly for the server
type InternalClientIface interface {
	SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse]
	SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse]
	SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error)
	String() string
}

type InternalClientImpl struct {
	name      string
	rawClient RaftServiceClient
}

func NewInternalClient(raftServiceClient RaftServiceClient) InternalClientIface {
	return &InternalClientImpl{
		name:      "InternalClientImpl",
		rawClient: raftServiceClient,
	}
}

func (rc *InternalClientImpl) String() string {
	return fmt.Sprintf("%s", rc.name)
}

func (rc *InternalClientImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	return rc.rawClient.SayHello(ctx, req)
}

// should call this with goroutine
// the parent shall control the timeout of the election
func (rc *InternalClientImpl) SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCRespWrapper[*RequestVoteResponse] {
	logger.Debugw("send request vote", "req", req)
	out := make(chan RPCRespWrapper[*RequestVoteResponse], 1)
	func() {
		retryTicker := time.NewTicker(time.Millisecond * util.RPC_REUQEST_TIMEOUT_IN_MS)
		defer retryTicker.Stop()

		var singleResChan chan RPCRespWrapper[*RequestVoteResponse]
		callRPC := func() {
			singleCallCtx, singleCallCancel := context.WithTimeout(ctx, time.Millisecond*(util.RPC_REUQEST_TIMEOUT_IN_MS-10))
			defer singleCallCancel()
			// todo: make sure the synchronous call will consume the ctx timeout in someway
			response, err := rc.rawClient.RequestVote(singleCallCtx, req)
			wrapper := RPCRespWrapper[*RequestVoteResponse]{
				Resp: response,
				Err:  err,
			}
			singleResChan <- wrapper
		}
		go callRPC()

		for {
			select {
			case <-ctx.Done():
				out <- RPCRespWrapper[*RequestVoteResponse]{
					Err: fmt.Errorf("SendRequestVote context done before getting a response")}
				return
			case <-retryTicker.C:
				callRPC()
			case out <- <-singleResChan:
				return
			}
		}
	}()
	return out
}

// the generator pattern
func (rc *InternalClientImpl) SendAppendEntries(
	ctx context.Context, req *AppendEntriesRequest) RPCRespWrapper[*AppendEntriesResponse] {
	response, err := rc.rawClient.AppendEntries(ctx, req)
	wrapper := RPCRespWrapper[*AppendEntriesResponse]{
		Resp: response,
		Err:  err,
	}
	return wrapper
}
