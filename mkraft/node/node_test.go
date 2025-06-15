package node

import (
	"testing"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/stretchr/testify/assert"
)

func TestNode_grantVote_basicsRules(t *testing.T) {
	n, ctrl := newMockNodeWithNoExpectations(t)
	defer cleanUpTmpDir(ctrl)
	t.Run("test case 0: current term is larger than new term", func(t *testing.T) {
		n.CurrentTerm = 2
		granted := n.grantVote(10, 0, 1, n.NodeId)
		assert.False(t, granted)
	})

	t.Run("test case 1: current term is same with the new term, but voteFor is not the candidate", func(t *testing.T) {
		n.CurrentTerm = 0
		n.VotedFor = ""
		granted := n.grantVote(10, 0, 0, n.NodeId)
		assert.False(t, granted)
	})

	t.Run("test case 2: current term is same with the new term, and voteFor is the candidate", func(t *testing.T) {
		n.CurrentTerm = 0
		n.VotedFor = "node2"
		granted := n.grantVote(10, 0, 0, "node2")
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(0))
	})

}

func TestNode_grantVote_voteRestrictions(t *testing.T) {
	n, ctrl := newMockNodeWithNoExpectations(t)
	defer cleanUpTmpDir(ctrl)
	t.Run("test case 1: current term is lower than the new term, leader raftlog last term is higher but length lower", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(8, 2, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
	t.Run("test case 2: current term is lower than the new term, leader raftlog last term is same but length lower", func(t *testing.T) {
		n.CurrentTerm = 0
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(9, 0, 1, n.NodeId)
		assert.False(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(0))
	})
	t.Run("test case 3: current term is lower than the new term, leader raftlog last term is same but length same", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(10, 0, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
	t.Run("test case 4: current term is lower than the new term, leader raftlog last term is same but length higher", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(11, 0, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
}
