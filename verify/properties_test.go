package verify

import (
	"testing"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/stretchr/testify/assert"
)

// func TestVerifyLeaderSafety(t *testing.T) {
// 	t.Run("single leader per term", func(t *testing.T) {
// 		nodeToStateFiles := map[string]string{
// 			"node1": "1#2023-01-01T00:00:00Z#node1#leader\n" +
// 				"2#2023-01-01T00:01:00Z#node1#follower\n" +
// 				"2#2023-01-01T00:02:00Z#node1#leader",
// 			"node2": "1#2023-01-01T00:00:00Z#node2#follower\n" +
// 				"2#2023-01-01T00:01:00Z#node2#follower",
// 			"node3": "1#2023-01-01T00:00:00Z#node3#follower\n" +
// 				"2#2023-01-01T00:01:00Z#node3#follower",
// 		}

// 		result, err := verifyLeaderSafety(nodeToStateFiles)
// 		assert.NoError(t, err)
// 		assert.True(t, result)
// 	})

// 	t.Run("multiple leaders in same term", func(t *testing.T) {
// 		nodeToStateFiles := map[string]string{
// 			"node1": "1#2023-01-01T00:00:00Z#node1#leader",
// 			"node2": "1#2023-01-01T00:00:00Z#node2#leader",
// 			"node3": "1#2023-01-01T00:00:00Z#node3#follower",
// 		}
// 		result, err := verifyLeaderSafety(nodeToStateFiles)
// 		assert.NoError(t, err)
// 		assert.False(t, result)
// 	})

// 	t.Run("corrupt entry", func(t *testing.T) {
// 		nodeToStateFiles := map[string]string{
// 			"node1": "invalid#entry#format",
// 			"node2": "1#2023-01-01T00:00:00Z#node2#follower",
// 		}

// 		result, err := verifyLeaderSafety(nodeToStateFiles)
// 		assert.Equal(t, err, common.ErrCorruptLine)
// 		assert.False(t, result)
// 	})

// 	t.Run("no leaders in term", func(t *testing.T) {
// 		nodeToStateFiles := map[string]string{
// 			"node1": "1#2023-01-01T00:00:00Z#node1#follower",
// 			"node2": "1#2023-01-01T00:00:00Z#node2#follower",
// 			"node3": "1#2023-01-01T00:00:00Z#node3#follower",
// 		}
// 		result, err := verifyLeaderSafety(nodeToStateFiles)
// 		assert.NoError(t, err)
// 		assert.False(t, result)
// 	})

// 	t.Run("empty state files", func(t *testing.T) {
// 		nodeToStateFiles := map[string]string{
// 			"node1": "",
// 			"node2": "",
// 			"node3": "",
// 		}
// 		result, err := verifyLeaderSafety(nodeToStateFiles)
// 		assert.NoError(t, err)
// 		assert.True(t, result)
// 	})
// }

// -------------------property of log mathcing-------------------
func TestVerifyLogMatching(t *testing.T) {
	t.Run("matching logs", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
		}
		result, err := VerifyLogMatching(nodeToLogs)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("different log lengths", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd1")},
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
		}
		_, err := VerifyLogMatching(nodeToLogs)
		assert.Error(t, err)
	})

	t.Run("different terms", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 3, Commands: []byte("cmd2")}, // Different term
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
		}
		result, err := VerifyLogMatching(nodeToLogs)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("different last commands", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("different")}, // Different command
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("different")}, // Different command
			},
		}
		result, err := VerifyLogMatching(nodeToLogs)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("different in the middle commands", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 1, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("different")}, // Different command
				{Term: 1, Commands: []byte("cmd2")},
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("different")}, // Different command
				{Term: 1, Commands: []byte("cmd2")},
			},
		}
		result, err := VerifyLogMatching(nodeToLogs)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("multiple discrepancies in logs", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("cmd1")},
				{Term: 2, Commands: []byte("cmd2")},
			},
			"node2": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("different")}, // Different command
				{Term: 1, Commands: []byte("cmd2")},
			},
			"node3": {
				{Term: 1, Commands: []byte("cmd0")},
				{Term: 1, Commands: []byte("different")}, // Different command
				{Term: 1, Commands: []byte("cmd2")},
			},
		}
		result, err := VerifyLogMatching(nodeToLogs)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("empty logs", func(t *testing.T) {
		nodeToLogs := map[string][]*log.RaftLogEntry{
			"node1": {},
			"node2": {},
		}
		_, err := VerifyLogMatching(nodeToLogs)
		assert.Error(t, err)
	})
}

// -------------------property of state machine safety-------------------
func TestVerifyStateMachineSafety(t *testing.T) {
	t.Run("different line count", func(t *testing.T) {
		nodeToStateMachineData := [][]string{
			{"cmd1", "cmd2", "cmd3"},
			{"cmd1", "cmd2", "cmd3"},
			{"cmd1", "cmd2"},
		}
		_, err := VerifyStateMachineSafety(nodeToStateMachineData)
		assert.Error(t, err)
	})
	t.Run("all nodes have same state machine data", func(t *testing.T) {
		nodeToStateMachineData := [][]string{
			{"cmd1", "cmd2", "cmd3"},
			{"cmd1", "cmd2", "cmd3"},
			{"cmd1", "cmd2", "cmd3"},
		}
		result, err := VerifyStateMachineSafety(nodeToStateMachineData)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("nodes have different state machine data", func(t *testing.T) {
		nodeToStateMachineData := [][]string{
			{"cmd1", "cmd2", "cmd3"},
			{"cmd1", "different", "cmd3"}, // Node 2 has different data
			{"cmd1", "cmd2", "cmd3"},
		}
		result, err := VerifyStateMachineSafety(nodeToStateMachineData)
		assert.NoError(t, err)
		assert.False(t, result)
	})

}
