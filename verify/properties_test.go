package verify

import (
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/stretchr/testify/assert"
)

func TestVerifyLeaderSafety(t *testing.T) {
	t.Run("single leader per term", func(t *testing.T) {
		nodeToStateFiles := map[string]string{
			"node1": "1#2023-01-01T00:00:00Z#node1#leader\n" +
				"2#2023-01-01T00:01:00Z#node1#follower\n" +
				"2#2023-01-01T00:02:00Z#node1#leader",
			"node2": "1#2023-01-01T00:00:00Z#node2#follower\n" +
				"2#2023-01-01T00:01:00Z#node2#follower",
			"node3": "1#2023-01-01T00:00:00Z#node3#follower\n" +
				"2#2023-01-01T00:01:00Z#node3#follower",
		}

		result, err := verifyLeaderSafety(nodeToStateFiles)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("multiple leaders in same term", func(t *testing.T) {
		nodeToStateFiles := map[string]string{
			"node1": "1#2023-01-01T00:00:00Z#node1#leader",
			"node2": "1#2023-01-01T00:00:00Z#node2#leader",
			"node3": "1#2023-01-01T00:00:00Z#node3#follower",
		}
		result, err := verifyLeaderSafety(nodeToStateFiles)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("corrupt entry", func(t *testing.T) {
		nodeToStateFiles := map[string]string{
			"node1": "invalid#entry#format",
			"node2": "1#2023-01-01T00:00:00Z#node2#follower",
		}

		result, err := verifyLeaderSafety(nodeToStateFiles)
		assert.Equal(t, err, common.ErrCorruptLine)
		assert.False(t, result)
	})

	t.Run("no leaders in term", func(t *testing.T) {
		nodeToStateFiles := map[string]string{
			"node1": "1#2023-01-01T00:00:00Z#node1#follower",
			"node2": "1#2023-01-01T00:00:00Z#node2#follower",
			"node3": "1#2023-01-01T00:00:00Z#node3#follower",
		}
		result, err := verifyLeaderSafety(nodeToStateFiles)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("empty state files", func(t *testing.T) {
		nodeToStateFiles := map[string]string{
			"node1": "",
			"node2": "",
			"node3": "",
		}
		result, err := verifyLeaderSafety(nodeToStateFiles)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}
