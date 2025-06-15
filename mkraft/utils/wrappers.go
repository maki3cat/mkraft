package utils

import (
	"context"
	"sync/atomic"

	"github.com/maki3cat/mkraft/rpc"
)

type RequestVoteInternalReq RPCRequestWrapper[*rpc.RequestVoteRequest, *rpc.RequestVoteResponse]
type AppendEntriesInternalReq RPCRequestWrapper[*rpc.AppendEntriesRequest, *rpc.AppendEntriesResponse]
type ClientCommandInternalReq RPCRequestWrapper[*rpc.ClientCommandRequest, *rpc.ClientCommandResponse]

type RPCRequestWrapper[T RPCRequest, R RPCResponse] struct {
	Req       T
	RespChan  chan *RPCRespWrapper[R]
	IsTimeout atomic.Bool
	Ctx       context.Context
}

type RPCRespWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCRequest interface {
	*rpc.AppendEntriesRequest | *rpc.RequestVoteRequest | *rpc.ClientCommandRequest
}

type RPCResponse interface {
	*rpc.AppendEntriesResponse | *rpc.RequestVoteResponse | *rpc.ClientCommandResponse
}
