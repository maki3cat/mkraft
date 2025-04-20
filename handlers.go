package main

import (
	"context"
	"fmt"
	"time"

	"github.com/maki3cat/mkraft/raft"
	pb "github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
}

func (s *Server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	logger.Infof("Received: %v", in)
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// timeout handling guideline: https://grpc.io/docs/guides/deadlines/
func (s *Server) RequestVote(_ context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	logger.Infof("RequestVote received: %v", in)
	respChan := make(chan *pb.RPCRespWrapper[*pb.RequestVoteResponse], 1)
	internalReq := &raft.RequestVoteInternal{
		Request:    in,
		RespWraper: respChan,
	}

	// todo:
	// so far we assume the raft server is not busy
	// no rate limiting so far
	// but in theory, it can block here
	raft.GetRaftNode().VoteRequest(internalReq)
	timeout := util.GetConfig().GetRPCRequestTimeout()
	timeoutTimer := time.NewTimer(timeout)
	select {
	case <-timeoutTimer.C:
		logger.Error("timeout getting response from raft server")
		internalReq.IsTimeout.Store(true)
		return nil, fmt.Errorf("server timeout")
	case resp := <-respChan:
		if resp.Err != nil {
			logger.Error("error in getting response from raft server", resp.Err)
			return nil, resp.Err
		}
		return resp.Resp, nil
	}
}

func (s *Server) AppendEntries(_ context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	logger.Infof("Received: %v", in)
	return &pb.AppendEntriesResponse{Term: 1, Success: true}, nil
}
