package node

import (
	"os"
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/stretchr/testify/assert"
)

func TestGetIdxFileName(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	assert.Contains(t, node.getIdxFileName(), "index.mk")
}

func TestUnsafeSaveAndLoadIdx(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// Set test values
	node.commitIndex = 5
	node.lastApplied = 3

	// Test save
	err := node.unsafeSaveIdx()
	assert.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(node.getIdxFileName())
	assert.NoError(t, err)

	// Create new node to test load
	newNode, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	err = newNode.unsafeLoadIdx()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), newNode.commitIndex)
	assert.Equal(t, uint64(3), newNode.lastApplied)
}

func TestGetCommitIdxAndLastApplied(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// Set test values
	node.commitIndex = 10
	node.lastApplied = 8

	commitIdx, lastApplied := node.getCommitIdxAndLastApplied()
	assert.Equal(t, uint64(10), commitIdx)
	assert.Equal(t, uint64(8), lastApplied)
}

func TestGetCommitIdx(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	node.commitIndex = 15

	commitIdx := node.getCommitIdx()
	assert.Equal(t, uint64(15), commitIdx)
}

func TestIncrementCommitIdx(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("successful increment", func(t *testing.T) {
		node.commitIndex = 5
		node.lastApplied = 3
		err := node.incrementCommitIdx(3, false)
		assert.NoError(t, err)
		assert.Equal(t, uint64(8), node.commitIndex)
	})

	t.Run("breaks invariant", func(t *testing.T) {
		node.commitIndex = 3
		node.lastApplied = 5
		err := node.incrementCommitIdx(1, false)
		assert.ErrorIs(t, err, common.ErrInvariantsBroken)
	})
}

func TestIncrementLastApplied(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	t.Run("successful increment", func(t *testing.T) {
		node.commitIndex = 10
		node.lastApplied = 5
		err := node.incrementLastApplied(2, false)
		assert.NoError(t, err)
		assert.Equal(t, uint64(7), node.lastApplied)
	})

	t.Run("breaks invariant", func(t *testing.T) {
		node.commitIndex = 5
		node.lastApplied = 3
		err := node.incrementLastApplied(3, false)
		assert.ErrorIs(t, err, common.ErrInvariantsBroken)
	})
}

func TestIncrementPeersNextIndexOnSuccess(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// Test first time increment
	node.incrPeerIdxAfterLogRepli("peer1", 3)
	assert.Equal(t, uint64(4), node.nextIndex["peer1"])
	assert.Equal(t, uint64(3), node.matchIndex["peer1"])

	// Test subsequent increment
	node.incrPeerIdxAfterLogRepli("peer1", 2)
	assert.Equal(t, uint64(6), node.nextIndex["peer1"])
	assert.Equal(t, uint64(5), node.matchIndex["peer1"])
}

func TestDecrementPeersNextIndexOnFailure(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	node.nextIndex["peer1"] = 5

	node.decrPeerIdxAfterLogRepli("peer1")
	assert.Equal(t, uint64(4), node.nextIndex["peer1"])

	// Test minimum value
	node.nextIndex["peer1"] = 1
	node.decrPeerIdxAfterLogRepli("peer1")
	assert.Equal(t, uint64(1), node.nextIndex["peer1"])
}

func TestGetPeersNextIndex(t *testing.T) {
	node, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	node.nextIndex["peer1"] = 10

	// Test existing peer
	index := node.getPeersNextIndex("peer1")
	assert.Equal(t, uint64(10), index)

	// Test new peer
	assert.Panics(t, func() {
		_ = node.getPeersNextIndex("peer2")
	})
}
