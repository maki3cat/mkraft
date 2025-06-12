package node

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadCurrentTermAndVotedFor(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()

	// Test loading when file doesn't exist
	err := node.loadCurrentTermAndVotedFor()
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), node.CurrentTerm)
	assert.Equal(t, "", node.VotedFor)

	// Test loading existing file
	err = node.storeCurrentTermAndVotedFor(5, "node1", false)
	assert.NoError(t, err)

	err = node.loadCurrentTermAndVotedFor()
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), node.CurrentTerm)
	assert.Equal(t, "node1", node.VotedFor)
}

func TestStoreCurrentTermAndVotedFor(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()

	// Test storing new values
	err := node.storeCurrentTermAndVotedFor(3, "node2", false)
	assert.NoError(t, err)

	// Verify stored values
	term := node.getCurrentTerm()
	assert.Equal(t, uint32(3), term)

	// Test updating values
	err = node.storeCurrentTermAndVotedFor(4, "node3", false)
	assert.NoError(t, err)

	term = node.getCurrentTerm()
	assert.Equal(t, uint32(4), term)
}

func TestUpdateCurrentTermAndVotedForAsCandidate(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()

	node.CurrentTerm = 1
	err := node.updateCurrentTermAndVotedForAsCandidate(false)
	assert.NoError(t, err)

	// Term should increment
	assert.Equal(t, uint32(2), node.getCurrentTerm())
	assert.Equal(t, node.NodeId, node.VotedFor)

	// Verify persisted to disk
	data, err := os.ReadFile(node.getStateFilePath())
	assert.NoError(t, err)
	assert.Contains(t, string(data), "2,"+node.NodeId)
}

func TestGetCurrentTerm(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()

	// Test initial term
	term := node.getCurrentTerm()
	assert.Equal(t, uint32(0), term)

	// Test after updating term
	node.CurrentTerm = 5
	term = node.getCurrentTerm()
	assert.Equal(t, uint32(5), term)
}
