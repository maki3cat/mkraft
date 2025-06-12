package common

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("non_existent_file.yaml")
	assert.Error(t, err)
}

func TestLoadConfig_ValidYAML(t *testing.T) {
	yamlContent := `
basic_config:
  raft_node_request_buffer_size: 123
  rpc_request_timeout_in_ms: 456
  election_timeout_min_in_ms: 1000
  election_timeout_max_in_ms: 2000
  leader_heartbeat_period_in_ms: 50
  min_remaining_time_for_rpc_in_ms: 20
  graceful_shutdown_timeout_in_sec: 10
  raft_log_file_path: "/tmp/raft_log"
membership:
  current_node_id: "node1"
  current_port: 8080
  current_node_addr: "127.0.0.1"
  cluster_size: 3
  all_members:
    - node_id: "node1"
      node_uri: "127.0.0.1:8080"
    - node_id: "node2"
      node_uri: "127.0.0.1:8081"
    - node_id: "node3"
      node_uri: "127.0.0.1:8082"
grpc:
  service: "test"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write([]byte(yamlContent))
	assert.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.NoError(t, err)

	assert.Equal(t, 123, cfg.BasicConfig.RaftNodeRequestBufferSize)
	assert.Equal(t, 456*time.Millisecond, cfg.GetRPCRequestTimeout())
	assert.Equal(t, "node1", cfg.Membership.CurrentNodeID)
	assert.Equal(t, 3, len(cfg.Membership.AllMembers))
	assert.Equal(t, "test", cfg.GRPC["service"])
}

func TestLoadConfig_Default(t *testing.T) {
	yamlContent := `
membership:
  current_node_id: "node1"
  current_port: 8080
  current_node_addr: "127.0.0.1"
  cluster_size: 3
  all_members:
    - node_id: "node1"
      node_uri: "127.0.0.1:8080"
    - node_id: "node2"
      node_uri: "127.0.0.1:8081"
    - node_id: "node3"
      node_uri: "127.0.0.1:8082"
basic_config:
  raft_log_file_path: "/tmp/raft_log"
grpc:
  service: "test"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write([]byte(yamlContent))
	assert.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.NoError(t, err)

	assert.Equal(t, RAFT_NODE_REQUEST_BUFFER_SIZE, cfg.BasicConfig.RaftNodeRequestBufferSize)
	assert.Equal(t, RPC_REUQEST_TIMEOUT_IN_MS*time.Millisecond, cfg.GetRPCRequestTimeout())
	assert.Equal(t, ELECTION_TIMEOUT_MIN_IN_MS, cfg.BasicConfig.ElectionTimeoutMinInMs)
	assert.Equal(t, ELECTION_TIMEOUT_MAX_IN_MS, cfg.BasicConfig.ElectionTimeoutMaxInMs)
	assert.Equal(t, LEADER_HEARTBEAT_PERIOD_IN_MS, cfg.BasicConfig.LeaderHeartbeatPeriodInMs)
	assert.Equal(t, GRACEFUL_SHUTDOWN_IN_SEC, cfg.BasicConfig.GracefulShutdownTimeoutInSec)
	assert.Equal(t, "node1", cfg.Membership.CurrentNodeID)
	assert.Equal(t, 3, len(cfg.Membership.AllMembers))
	assert.Equal(t, "test", cfg.GRPC["service"])
}
