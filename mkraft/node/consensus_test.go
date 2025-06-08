package node

import (
	"testing"
)
func TestCalculateIfMajorityMet(t *testing.T) {
	tests := []struct {
		name                string
		total              int
		peerVoteAccumulated int
		want               bool
	}{
		{
			name:                "3 nodes, 1 peer vote - met (self vote + 1 peer = 2/3)",
			total:              3,
			peerVoteAccumulated: 1,
			want:               true,
		},
		{
			name:                "3 nodes, 0 peer votes - not met (only self vote = 1/3)",
			total:              3, 
			peerVoteAccumulated: 0,
			want:               false,
		},
		{
			name:                "5 nodes, 1 peer vote - not met (self + 1 peer = 2/5)",
			total:              5,
			peerVoteAccumulated: 1,
			want:               false,
		},
		{
			name:                "5 nodes, 2 peer votes - met (self + 2 peers = 3/5)",
			total:              5,
			peerVoteAccumulated: 2,
			want:               true,
		},
		{
			name:                "7 nodes, 2 peer votes - not met (self + 2 peers = 3/7)",
			total:              7,
			peerVoteAccumulated: 2,
			want:               false,
		},
		{
			name:                "7 nodes, 3 peer votes - met (self + 3 peers = 4/7)",
			total:              7,
			peerVoteAccumulated: 3,
			want:               true,
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
		total              int
		peersCount         int
		peerVoteAccumulated int
		voteFailed         int
		want               bool
	}{
		{
			name:                "3 nodes, 2 peers, no votes, 2 failed - already failed",
			total:              3,
			peersCount:         2,
			peerVoteAccumulated: 0,
			voteFailed:         2,
			want:               true,
		},
		{
			name:                "3 nodes, 2 peers, no votes, 1 failure - not failed (can still get majority)",
			total:              3,
			peersCount:         2,
			peerVoteAccumulated: 0,
			voteFailed:         1,
			want:               false,
		},
		{
			name:                "5 nodes, 4 peers, 1 vote, 3 failed - already failed",
			total:              5,
			peersCount:         4,
			peerVoteAccumulated: 1,
			voteFailed:         3,
			want:               true,
		},
		{
			name:                "5 nodes, 4 peers, 1 vote, 2 failed - not failed (can still get majority)",
			total:              5,
			peersCount:         4,
			peerVoteAccumulated: 1,
			voteFailed:         2,
			want:               false,
		},
		{
			name:                "7 nodes, 6 peers, 2 votes, 4 failed - already failed",
			total:              7,
			peersCount:         6,
			peerVoteAccumulated: 2,
			voteFailed:         4,
			want:               true,
		},
		{
			name:                "7 nodes, 6 peers, 2 votes, 3 failed - not failed (can still get majority)",
			total:              7,
			peersCount:         6,
			peerVoteAccumulated: 2,
			voteFailed:         3,
			want:               false,
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

