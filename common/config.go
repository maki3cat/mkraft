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

func LoadConfig(filePath string) (*Config, error) {
	// start with default config
	cfg := GetDefaultConfig()

	// yaml config
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// validate - the grammar
	err = validator.Validate(cfg)
	if err != nil {
		return nil, err
	}
	if err = cfg.Validate(); err != nil {
		fmt.Println("err", err)
		return nil, err
	}
	configInvariantCheck(cfg)

	// key invariants

	fmt.Println("cfg", fmt.Sprintf("%+v", cfg))
	return cfg, nil
}

func configInvariantCheck(cfg *Config) {
	if cfg.BasicConfig.ElectionTimeoutMinInMs >= cfg.BasicConfig.ElectionTimeoutMaxInMs {
		panic("election timeout min must be less than max")
	}
	if cfg.BasicConfig.ElectionTimeoutMinInMs < 2*cfg.BasicConfig.RPCRequestTimeoutInMs {
		panic("election timeout min must be at least 2 rpc timeouts")
	}
	distance := cfg.BasicConfig.ElectionTimeoutMaxInMs - cfg.BasicConfig.ElectionTimeoutMinInMs
	if distance < 5*cfg.BasicConfig.RPCRequestTimeoutInMs {
		panic("election timeout range must be at least 5 rpc timeouts")
	}
	// leader heartbeat period must be larger than the rpc timeout
	// leader heartbeat period must be smaller than the min election timeout
	if cfg.BasicConfig.LeaderHeartbeatPeriodInMs <= cfg.BasicConfig.RPCRequestTimeoutInMs {
		panic("leader heartbeat period must be larger than the rpc timeout")
	}
	if cfg.BasicConfig.LeaderHeartbeatPeriodInMs >= (cfg.BasicConfig.ElectionTimeoutMinInMs / 2) {
		panic("leader heartbeat period must be smaller than the min election timeout / 2")
	}
}

func GetDefaultConfig() *Config {
	return &Config{
		BasicConfig: *defaultBasicConfig,
		Monitoring:  *defaultMonitoring,
	}
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
	defaultMonitoring = &Monitoring{
		LogLevel: "dev",
	}
)

const (
	RAFT_NODE_REQUEST_BUFFER_SIZE = 500

	LEADER_BUFFER_SIZE = 1000
	// if this is close to election timeout range lower bound, the leader may not be able to send heartbeat to followers in time
	LEADER_HEARTBEAT_PERIOD_IN_MS = 150

	// paper: $5.6, the broadcast time should be an order of magnitude less thant the election timeout
	RPC_REUQEST_TIMEOUT_IN_MS = 100
	// reference: the Jeff-Dean's number everyone shall know
	RPC_DEADLINE_MARGIN_IN_MICRO_SEC = 500

	ELECTION_TIMEOUT_MIN_IN_MS = 4 * RPC_REUQEST_TIMEOUT_IN_MS
	ELECTION_TIMEOUT_MAX_IN_MS = 10 * RPC_REUQEST_TIMEOUT_IN_MS

	MIN_REMAINING_TIME_FOR_RPC_IN_MS = 150

	GRACEFUL_SHUTDOWN_IN_SEC = 3

	DEFAULT_DATA_DIR = "."
)

type (
	Config struct {
		BasicConfig BasicConfig    `yaml:"basic_config" json:"basic_config"`
		Membership  Membership     `yaml:"membership" json:"membership" validate:"nonzero"`
		GRPC        map[string]any `yaml:"grpc" json:"grpc"`
		Monitoring  Monitoring     `yaml:"monitoring" json:"monitoring"`
	}

	Monitoring struct {
		LogLevel string `yaml:"log_level" json:"log_level" validate:"nonzero"`
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
	randomMs := (rand.Intn(b.ElectionTimeoutMaxInMs-b.ElectionTimeoutMinInMs) + b.ElectionTimeoutMinInMs)
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
