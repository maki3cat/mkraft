package common

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// todo: how about removing the interface abstraction, and just use the struct directly?

func LoadConfig(filePath string) (*Config, error) {
	// start with default config
	cfg := &Config{BasicConfig: *defaultBasicConfig}

	// yaml config
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// validate
	err = validator.Validate(cfg)
	if err != nil {
		return nil, err
	}
	if err = cfg.Validate(); err != nil {
		fmt.Println("err", err)
		return nil, err
	}
	return cfg, nil
}

func GetDefaultConfig() *Config {
	return &Config{BasicConfig: *defaultBasicConfig}
}

var (
	defaultBasicConfig = &BasicConfig{
		RaftNodeRequestBufferSize:    RAFT_NODE_REQUEST_BUFFER_SIZE,
		RPCRequestTimeoutInMs:        RPC_REUQEST_TIMEOUT_IN_MS,
		ElectionTimeoutMinInMs:       ELECTION_TIMEOUT_MIN_IN_MS,
		ElectionTimeoutMaxInMs:       ELECTION_TIMEOUT_MAX_IN_MS,
		LeaderHeartbeatPeriodInMs:    LEADER_HEARTBEAT_PERIOD_IN_MS,
		GracefulShutdownTimeoutInSec: GRACEFUL_SHUTDOWN_IN_SEC,
		RPCDeadlineMarginInMicroSec:  RPC_DEADLINE_MARGIN_IN_MICRO_SEC,
		DataDir:                      DEFAULT_DATA_DIR,
	}
)

const (
	RAFT_NODE_REQUEST_BUFFER_SIZE = 500

	LEADER_BUFFER_SIZE            = 1000
	LEADER_HEARTBEAT_PERIOD_IN_MS = 100

	// paper: $5.6, the broadcast time should be an order of magnitude less thant the election timeout
	RPC_REUQEST_TIMEOUT_IN_MS = 20
	// reference: the Jeff-Dean's number everyone shall know
	RPC_DEADLINE_MARGIN_IN_MICRO_SEC = 500
	ELECTION_TIMEOUT_MIN_IN_MS       = 200
	ELECTION_TIMEOUT_MAX_IN_MS       = 400

	MIN_REMAINING_TIME_FOR_RPC_IN_MS = 50

	GRACEFUL_SHUTDOWN_IN_SEC = 3

	DEFAULT_DATA_DIR = "."
)

type (
	Config struct {
		BasicConfig BasicConfig    `yaml:"basic_config" json:"basic_config"`
		Membership  Membership     `yaml:"membership" json:"membership" validate:"nonzero"`
		GRPC        map[string]any `yaml:"grpc" json:"grpc"`
	}

	Membership struct {
		CurrentNodeID   string     `yaml:"current_node_id" json:"current_node_id" validate:"nonzero"`
		CurrentPort     int        `yaml:"current_port" json:"current_port" validate:"nonzero"`
		CurrentNodeAddr string     `yaml:"current_node_addr" json:"current_node_addr" validate:"nonzero"`
		AllMembers      []NodeAddr `yaml:"all_members" json:"all_members" validate:"nonzero"`
		ClusterSize     int        `yaml:"cluster_size" json:"cluster_size" validate:"min=3"`
	}

	NodeAddr struct {
		NodeID  string `yaml:"node_id" json:"node_id" validate:"nonzero"`
		NodeURI string `yaml:"node_uri" json:"node_uri" validate:"nonzero"`
	}

	BasicConfig struct {
		DataDir                   string `yaml:"data_dir" json:"data_dir" validate:"nonzero"`
		RaftNodeRequestBufferSize int    `yaml:"raft_node_request_buffer_size" json:"raft_node_request_buffer_size" validate:"min=1"`

		// RPC timeout
		RPCRequestTimeoutInMs        int `yaml:"rpc_request_timeout_in_ms" json:"rpc_request_timeout_in_ms" validate:"min=1"`
		GracefulShutdownTimeoutInSec int `yaml:"graceful_shutdown_timeout_in_sec" json:"graceful_shutdown_timeout_in_sec" validate:"min=1"`
		RPCDeadlineMarginInMicroSec  int `yaml:"rpc_deadline_margin_in_micro_sec" json:"rpc_deadline_margin_in_micro_sec" validate:"min=250"`

		// Election timeout
		ElectionTimeoutMinInMs int `yaml:"election_timeout_min_in_ms" json:"election_timeout_min_in_ms" validate:"min=1"`
		ElectionTimeoutMaxInMs int `yaml:"election_timeout_max_in_ms" json:"election_timeout_max_in_ms" validate:"min=1"`

		// Leader
		LeaderHeartbeatPeriodInMs int `yaml:"leader_heartbeat_period_in_ms" json:"leader_heartbeat_period_in_ms" validate:"min=1"`
	}
)

func (c *Config) SetDataDir(dataDir string) {
	c.BasicConfig.DataDir = dataDir
}

func (c *Config) GetDataDir() string {
	return c.BasicConfig.DataDir
}

func (c *Config) GetClusterSize() int {
	return c.Membership.ClusterSize
}

func (c *Config) GetMembership() Membership {
	return c.Membership
}

func (c *Config) GetGracefulShutdownTimeout() time.Duration {
	return time.Duration(c.BasicConfig.GracefulShutdownTimeoutInSec) * time.Second
}

func (c *Config) Validate() error {

	_, err := json.Marshal(c.GRPC)
	fmt.Println("err", err)
	if err != nil {
		return err
	}
	if len(c.Membership.AllMembers)%2 == 0 {
		return errors.New("number of members must be odd")
	}
	if (c.BasicConfig.ElectionTimeoutMinInMs > c.BasicConfig.ElectionTimeoutMaxInMs) ||
		(c.BasicConfig.ElectionTimeoutMinInMs <= 0) ||
		(c.BasicConfig.ElectionTimeoutMaxInMs <= 0) {
		return errors.New("election timeout min must be less than max and both must be positive")
	}
	return nil
}

func (c *Config) GetgRPCServiceConf() string {
	grpcJSON, _ := json.Marshal(c.GRPC)
	return string(grpcJSON)
}

func (c *Config) GetRPCRequestTimeout() time.Duration {
	return time.Duration(c.BasicConfig.RPCRequestTimeoutInMs) * time.Millisecond
}

func (c *Config) GetElectionTimeout() time.Duration {
	b := c.BasicConfig
	timeoutRange := b.ElectionTimeoutMaxInMs - b.ElectionTimeoutMinInMs
	randomMs := rand.Intn(timeoutRange) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
}

func (c *Config) GetRaftNodeRequestBufferSize() int {
	b := c.BasicConfig
	return b.RaftNodeRequestBufferSize
}

func (c *Config) GetLeaderHeartbeatPeriod() time.Duration {
	b := c.BasicConfig
	return time.Duration(b.LeaderHeartbeatPeriodInMs) * time.Millisecond
}

func (c *Config) String() string {
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}

func (c *Config) GetRPCDeadlineMargin() time.Duration {
	return time.Duration(c.BasicConfig.RPCDeadlineMarginInMicroSec) * time.Microsecond
}
