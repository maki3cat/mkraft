package peers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
)

func TestNewInternalClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConfig := common.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetgRPCServiceConf().Return(`{"loadBalancingPolicy":"round_robin"}`).AnyTimes()
	mockConfig.EXPECT().GetRPCRequestTimeout().Return(time.Second).AnyTimes()
	mockConfig.EXPECT().GetRPCDeadlineMargin().Return(time.Millisecond * 100).AnyTimes()

	logger := zap.NewNop()

	client, err := NewInternalClient("test-node", "localhost:8080", logger, mockConfig)
	require.NoError(t, err)
	require.NotNil(t, client)

	impl, ok := client.(*InternalClientImpl)
	require.True(t, ok)
	assert.Equal(t, "test-node", impl.nodeId)
	assert.Equal(t, "localhost:8080", impl.nodeAddr)
}

func TestInternalClientImpl_SayHello(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetRPCRequestTimeout().Return(time.Second).AnyTimes()

	client := &InternalClientImpl{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.HelloRequest{Name: "test"}
	expectedResp := &rpc.HelloReply{Message: "Hello test"}

	mockClient.EXPECT().
		SayHello(gomock.Any(), req).
		Return(expectedResp, nil)

	resp, err := client.SayHello(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestInternalClientImpl_SendAppendEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetRPCRequestTimeout().Return(time.Second).AnyTimes()

	client := &InternalClientImpl{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.AppendEntriesRequest{
		Term:     1,
		LeaderId: "leader",
	}
	expectedResp := &rpc.AppendEntriesResponse{
		Term:    1,
		Success: true,
	}

	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).
		Return(expectedResp, nil)

	wrapper := client.SendAppendEntries(context.Background(), req)
	require.NoError(t, wrapper.Err)
	assert.Equal(t, expectedResp, wrapper.Resp)
}

func TestInternalClientImpl_SendRequestVoteWithRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.NewMockConfigIface(ctrl)
	mockConfig.EXPECT().GetRPCRequestTimeout().Return(time.Second).AnyTimes()
	mockConfig.EXPECT().GetRPCDeadlineMargin().Return(time.Millisecond * 100).AnyTimes()

	client := &InternalClientImpl{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	expectedResp := &rpc.RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	// Test successful case
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	respChan := client.RequestVoteWithInfiniteRetries(ctx, req)
	wrapper := <-respChan
	require.NoError(t, wrapper.Err)
	assert.Equal(t, expectedResp, wrapper.Resp)

	// Test error case with retry
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, assert.AnError).
		Times(1)
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil).
		Times(1)

	respChan = client.RequestVoteWithInfiniteRetries(ctx, req)
	wrapper = <-respChan
	require.NoError(t, wrapper.Err)
	assert.Equal(t, expectedResp, wrapper.Resp)
}

func TestInternalClientImpl_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConfig := common.NewMockConfigIface(ctrl)
	client := &InternalClientImpl{
		nodeId:   "test-node",
		nodeAddr: "localhost:8080",
		logger:   zap.NewNop(),
		cfg:      mockConfig,
	}

	// Test nil connection
	err := client.Close()
	require.NoError(t, err)

	// Test with connection
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	require.NoError(t, err)
	client.conn = conn

	err = client.Close()
	require.NoError(t, err)
}
