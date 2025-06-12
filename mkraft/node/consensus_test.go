package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/mock/gomock"
)

func TestCalculateIfMajorityMet(t *testing.T) {
	tests := []struct {
		name                string
		total               int
		peerVoteAccumulated int
		want                bool
	}{
		{
			name:                "3 nodes, 1 peer vote - met (self vote + 1 peer = 2/3)",
			total:               3,
			peerVoteAccumulated: 1,
			want:                true,
		},
		{
			name:                "3 nodes, 0 peer votes - not met (only self vote = 1/3)",
			total:               3,
			peerVoteAccumulated: 0,
			want:                false,
		},
		{
			name:                "5 nodes, 1 peer vote - not met (self + 1 peer = 2/5)",
			total:               5,
			peerVoteAccumulated: 1,
			want:                false,
		},
		{
			name:                "5 nodes, 2 peer votes - met (self + 2 peers = 3/5)",
			total:               5,
			peerVoteAccumulated: 2,
			want:                true,
		},
		{
			name:                "7 nodes, 2 peer votes - not met (self + 2 peers = 3/7)",
			total:               7,
			peerVoteAccumulated: 2,
			want:                false,
		},
		{
			name:                "7 nodes, 3 peer votes - met (self + 3 peers = 4/7)",
			total:               7,
			peerVoteAccumulated: 3,
			want:                true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateIfMajorityMet(tt.total, tt.peerVoteAccumulated); got != tt.want {
				t.Errorf("calculateIfMajorityMet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateIfAlreadyFail(t *testing.T) {
	tests := []struct {
		name                string
		total               int
		peersCount          int
		peerVoteAccumulated int
		voteFailed          int
		want                bool
	}{
		{
			name:                "3 nodes, 2 peers, no votes, 2 failed - already failed",
			total:               3,
			peersCount:          2,
			peerVoteAccumulated: 0,
			voteFailed:          2,
			want:                true,
		},
		{
			name:                "3 nodes, 2 peers, no votes, 1 failure - not failed (can still get majority)",
			total:               3,
			peersCount:          2,
			peerVoteAccumulated: 0,
			voteFailed:          1,
			want:                false,
		},
		{
			name:                "5 nodes, 4 peers, 1 vote, 3 failed - already failed",
			total:               5,
			peersCount:          4,
			peerVoteAccumulated: 1,
			voteFailed:          3,
			want:                true,
		},
		{
			name:                "5 nodes, 4 peers, 1 vote, 2 failed - not failed (can still get majority)",
			total:               5,
			peersCount:          4,
			peerVoteAccumulated: 1,
			voteFailed:          2,
			want:                false,
		},
		{
			name:                "7 nodes, 6 peers, 2 votes, 4 failed - already failed",
			total:               7,
			peersCount:          6,
			peerVoteAccumulated: 2,
			voteFailed:          4,
			want:                true,
		},
		{
			name:                "7 nodes, 6 peers, 2 votes, 3 failed - not failed (can still get majority)",
			total:               7,
			peersCount:          6,
			peerVoteAccumulated: 2,
			voteFailed:          3,
			want:                false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateIfAlreadyFail(tt.total, tt.peersCount, tt.peerVoteAccumulated, tt.voteFailed); got != tt.want {
				t.Errorf("calculateIfAlreadyFail() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------ConsensusRequestVote---------------------------------------

func TestNode_ConsensusRequestVote_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)

			n := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockedMembership := n.membership.(*peers.MockMembership)
			mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockedMembership.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mockClient1, mockClient2}, nil)

			expectCall1 := mockClient1.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        1,
				VoteGranted: true,
			}, nil)
			expectCall2 := mockClient2.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        1,
				VoteGranted: true,
			}, nil)

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			resp, err := n.consensus.ConsensusRequestVote(ctx, req)
			if err != nil {
				t.Errorf("ConsensusRequestVote() error = %v", err)
				return
			}

			if tt.sleep {
				// wait for the goroutine to finish, because the ConsensusRequestVote is a shortcut method
				time.Sleep(100 * time.Millisecond)
			}

			if !resp.VoteGranted {
				t.Errorf("ConsensusRequestVote() vote not granted")
			}
		})
	}
}

func TestNode_ConsensusRequestVote_MajorityFail(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)
			node := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockedMembership := node.membership.(*peers.MockMembership)
			mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockedMembership.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mockClient1, mockClient2}, nil)

			expectCall1 := mockClient1.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        1,
				VoteGranted: false,
			}, nil)
			expectCall2 := mockClient2.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        1,
				VoteGranted: false,
			}, nil)

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			_, err := node.consensus.ConsensusRequestVote(ctx, req)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err != common.ErrMajorityNotMet {
				t.Errorf("ConsensusRequestVote() error = %v, want %v", err, common.ErrMajorityNotMet)
			}
		})
	}
}

func TestNode_ConsensusRequestVote_HigherTerm(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)
			node := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockedMembership := node.membership.(*peers.MockMembership)
			mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockedMembership.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mockClient1, mockClient2}, nil)

			expectCall1 := mockClient1.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        2, // Higher term
				VoteGranted: false,
			}, nil)
			expectCall2 := mockClient2.EXPECT().RequestVoteWithRetry(gomock.Any(), req).Return(&rpc.RequestVoteResponse{
				Term:        2, // Higher term
				VoteGranted: false,
			}, nil)

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			resp, err := node.consensus.ConsensusRequestVote(ctx, req)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				t.Errorf("ConsensusRequestVote() error = %v", err)
			}
			if resp.Term != 2 || resp.VoteGranted {
				t.Errorf("ConsensusRequestVote() expected term=2 voteGranted=false, got term=%d voteGranted=%v",
					resp.Term, resp.VoteGranted)
			}
		})
	}
}

func TestNode_ConsensusRequestVote_ContextCancelled(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMembership := peers.NewMockMembership(ctrl)
			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)

			node := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockMembership.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mockClient1, mockClient2}, nil)

			expectCall1 := mockClient1.EXPECT().RequestVoteWithRetry(gomock.Any(), req).
				DoAndReturn(func(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
					cancel() // Cancel context before responding
					return nil, context.Canceled
				})
			expectCall2 := mockClient2.EXPECT().RequestVoteWithRetry(gomock.Any(), req).
				DoAndReturn(func(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
					cancel() // Cancel context before responding
					return nil, context.Canceled
				})

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			_, err := node.consensus.ConsensusRequestVote(ctx, req)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err != common.ErrContextDone {
				t.Errorf("ConsensusRequestVote() error = %v, want %v", err, common.ErrContextDone)
			}
		})
	}
}

func TestNode_ConsensusRequestVote_MembershipError(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			n := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockedMembership := n.membership.(*peers.MockMembership)
			mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockedMembership.EXPECT().GetAllPeerClients().Return(nil, errors.New("membership error"))

			_, err := n.consensus.ConsensusRequestVote(ctx, req)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err == nil {
				t.Error("ConsensusRequestVote() expected error, got nil")
			}
		})
	}
}

func TestConsensusRequestVoteContext(t *testing.T) {
	tests := []struct {
		name        string
		ctxTimeout  time.Duration
		wantErr     error
		setupMocks  func(*peers.MockMembership, *peers.MockPeerClient)
		clientCount int
	}{
		{
			name:       "context deadline not set",
			ctxTimeout: 0,
			wantErr:    common.ErrDeadlineNotSet,
			setupMocks: func(mm *peers.MockMembership, mc *peers.MockPeerClient) {
				mm.EXPECT().GetTotalMemberCount().Return(3)
				mm.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mc}, nil)
			},
			clientCount: 1,
		},
		{
			name:       "context cancelled",
			ctxTimeout: 100 * time.Millisecond,
			wantErr:    common.ErrContextDone,
			setupMocks: func(mm *peers.MockMembership, mc *peers.MockPeerClient) {
				mm.EXPECT().GetTotalMemberCount().Return(3)
				mm.EXPECT().GetAllPeerClients().Return([]peers.PeerClient{mc}, nil)
				mc.EXPECT().RequestVoteWithRetry(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
						time.Sleep(200 * time.Millisecond)
						return nil, nil
					})
			},
			clientCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMembership := peers.NewMockMembership(ctrl)
			mockClient := peers.NewMockPeerClient(ctrl)
			node := newMockNode(t)
			defer cleanUpTmpDir()

			req := &rpc.RequestVoteRequest{
				Term:         1,
				CandidateId:  "node1",
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.ctxTimeout > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), tt.ctxTimeout)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()

			tt.setupMocks(mockMembership, mockClient)

			_, err := node.consensus.ConsensusRequestVote(ctx, req)
			if err != tt.wantErr {
				t.Errorf("ConsensusRequestVote() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

/// ---------------------------------------ConsensusAppendEntries---------------------------------------

func TestNode_ConsensusAppendEntries_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMembership := peers.NewMockMembership(ctrl)
			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)

			node := newMockNode(t)
			defer cleanUpTmpDir()

			peerReqs := map[string]*rpc.AppendEntriesRequest{
				"node2": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
				"node3": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockMembership.EXPECT().GetAllPeerClientsV2().Return(map[string]peers.PeerClient{
				"node2": mockClient1,
				"node3": mockClient2,
			}, nil)

			expectCall1 := mockClient1.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node2"]).Return(&rpc.AppendEntriesResponse{
				Term:    1,
				Success: true,
			}, nil)
			expectCall2 := mockClient2.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node3"]).Return(&rpc.AppendEntriesResponse{
				Term:    1,
				Success: true,
			}, nil)

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			resp, err := node.consensus.ConsensusAppendEntries(ctx, peerReqs, 1)
			if err != nil {
				t.Errorf("ConsensusAppendEntries() error = %v", err)
				return
			}

			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}

			if !resp.Success {
				t.Error("ConsensusAppendEntries() expected success")
			}
		})
	}
}

func TestNode_ConsensusAppendEntries_HigherTerm(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMembership := peers.NewMockMembership(ctrl)
			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)

			node := newMockNode(t)
			defer cleanUpTmpDir()

			peerReqs := map[string]*rpc.AppendEntriesRequest{
				"node2": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
				"node3": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockMembership.EXPECT().GetAllPeerClientsV2().Return(map[string]peers.PeerClient{
				"node2": mockClient1,
				"node3": mockClient2,
			}, nil)

			expectCall1 := mockClient1.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node2"]).Return(&rpc.AppendEntriesResponse{
				Term:    2,
				Success: false,
			}, nil)
			expectCall2 := mockClient2.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node3"]).Return(&rpc.AppendEntriesResponse{
				Term:    2,
				Success: false,
			}, nil)

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			resp, err := node.consensus.ConsensusAppendEntries(ctx, peerReqs, 1)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				t.Errorf("ConsensusAppendEntries() error = %v", err)
			}
			if resp.Term != 2 || resp.Success {
				t.Errorf("ConsensusAppendEntries() expected term=2 success=false, got term=%d success=%v",
					resp.Term, resp.Success)
			}
		})
	}
}

func TestNode_ConsensusAppendEntries_MembershipError(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := newMockNode(t)
			defer cleanUpTmpDir()

			peerReqs := map[string]*rpc.AppendEntriesRequest{
				"node2": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockedMembership := node.membership.(*peers.MockMembership)
			mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockedMembership.EXPECT().GetAllPeerClientsV2().Return(nil, errors.New("membership error"))

			_, err := node.consensus.ConsensusAppendEntries(ctx, peerReqs, 1)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err == nil {
				t.Error("ConsensusAppendEntries() expected error, got nil")
			}
		})
	}
}

func TestNode_ConsensusAppendEntries_ContextCancelled(t *testing.T) {
	tests := []struct {
		name     string
		maxTimes int
		sleep    bool
	}{
		{
			name:     "shortcut path",
			maxTimes: 1,
			sleep:    false,
		},
		{
			name:     "normal path",
			maxTimes: 0,
			sleep:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMembership := peers.NewMockMembership(ctrl)
			mockClient1 := peers.NewMockPeerClient(ctrl)
			mockClient2 := peers.NewMockPeerClient(ctrl)

			node := newMockNode(t)
			defer cleanUpTmpDir()

			peerReqs := map[string]*rpc.AppendEntriesRequest{
				"node2": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
				"node3": {
					Term:         1,
					LeaderId:     "node1",
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
					LeaderCommit: 0,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			mockMembership.EXPECT().GetTotalMemberCount().Return(3)
			mockMembership.EXPECT().GetAllPeerClientsV2().Return(map[string]peers.PeerClient{
				"node2": mockClient1,
				"node3": mockClient2,
			}, nil)

			expectCall1 := mockClient1.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node2"]).
				DoAndReturn(func(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
					cancel()
					return nil, context.Canceled
				})

			expectCall2 := mockClient2.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node3"]).
				DoAndReturn(func(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
					cancel()
					return nil, context.Canceled
				})

			if tt.maxTimes > 0 {
				expectCall1.MaxTimes(tt.maxTimes)
				expectCall2.MaxTimes(tt.maxTimes)
			}

			_, err := node.consensus.ConsensusAppendEntries(ctx, peerReqs, 1)
			if tt.sleep {
				time.Sleep(100 * time.Millisecond)
			}
			if err == nil || !errors.Is(err, common.ErrContextDone) {
				t.Errorf("ConsensusAppendEntries() error = %v, want context canceled", err)
			}
		})
	}
}

func TestConsensusAppendEntries_SmallerTermResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := peers.NewMockPeerClient(ctrl)

	node := newMockNode(t)
	defer cleanUpTmpDir()

	peerReqs := map[string]*rpc.AppendEntriesRequest{
		"node2": {
			Term:         2, // Current term is 2
			LeaderId:     "node1",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []*rpc.LogEntry{{Data: []byte("test")}},
			LeaderCommit: 0,
		},
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	mockedMembership := node.membership.(*peers.MockMembership)
	mockedMembership.EXPECT().GetTotalMemberCount().Return(3)
	mockedMembership.EXPECT().GetAllPeerClientsV2().Return(map[string]peers.PeerClient{
		"node2": mockClient,
	}, nil)

	// Mock response with term smaller than current term
	mockClient.EXPECT().AppendEntriesWithRetry(gomock.Any(), peerReqs["node2"]).
		Return(&rpc.AppendEntriesResponse{
			Term:    1, // Response term is 1, smaller than current term 2
			Success: true,
		}, nil)

	resp, err := node.consensus.ConsensusAppendEntries(ctx, peerReqs, 2)

	if err == nil || !errors.Is(err, common.ErrInvariantsBroken) {
		t.Errorf("ConsensusAppendEntries() error = %v, want ErrInvariantsBroken", err)
	}

	if resp != nil {
		t.Errorf("ConsensusAppendEntries() response = %v, want nil", resp)
	}
}
