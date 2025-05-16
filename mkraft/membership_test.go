package mkraft

import (
	"testing"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

func TestNewMembershipMgrWithStaticConfig(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name          string
		membership    common.Membership
		expectedError bool
	}{
		{
			name: "valid config",
			membership: common.Membership{
				AllMembers: []common.NodeAddr{
					{NodeID: "1", NodeURI: "localhost:1"},
					{NodeID: "2", NodeURI: "localhost:2"},
					{NodeID: "3", NodeURI: "localhost:3"},
				},
			},
			expectedError: false,
		},
		{
			name: "too few members",
			membership: common.Membership{
				AllMembers: []common.NodeAddr{
					{NodeID: "1", NodeURI: "localhost:1"},
					{NodeID: "2", NodeURI: "localhost:2"},
				},
			},
			expectedError: true,
		},
		{
			name: "even number of members",
			membership: common.Membership{
				AllMembers: []common.NodeAddr{
					{NodeID: "1", NodeURI: "localhost:1"},
					{NodeID: "2", NodeURI: "localhost:2"},
					{NodeID: "3", NodeURI: "localhost:3"},
					{NodeID: "4", NodeURI: "localhost:4"},
				},
			},
			expectedError: true,
		},
		{
			name: "empty node ID",
			membership: common.Membership{
				AllMembers: []common.NodeAddr{
					{NodeID: "", NodeURI: "localhost:1"},
					{NodeID: "2", NodeURI: "localhost:2"},
					{NodeID: "3", NodeURI: "localhost:3"},
				},
			},
			expectedError: true,
		},
		{
			name: "empty node URI",
			membership: common.Membership{
				AllMembers: []common.NodeAddr{
					{NodeID: "1", NodeURI: ""},
					{NodeID: "2", NodeURI: "localhost:2"},
					{NodeID: "3", NodeURI: "localhost:3"},
				},
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &common.Config{
				Membership: tt.membership,
			}
			mgr, err := NewMembershipMgrWithStaticConfig(logger, cfg)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if mgr == nil {
					t.Error("expected non-nil manager")
				}
			}
		})
	}
}
