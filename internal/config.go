package internal

import (
	"fmt"
	"os"
	"time"

	"example.com/dstate/internal/control"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
	HTTPAddr string `yaml:"http_addr"`
	// MongoAddr is the host:port the node's mongod serves replica set
	// traffic on; it is also the member host in the replica set config.
	MongoAddr string `yaml:"mongo_addr"`
	// KafkaID is the node's Kafka node.id. KafkaAddr and KafkaControllerAddr
	// are the host:port of its broker and controller listeners.
	KafkaID             int    `yaml:"kafka_id"`
	KafkaAddr           string `yaml:"kafka_addr"`
	KafkaControllerAddr string `yaml:"kafka_controller_addr"`
	// NTPAddr is the address the node's ntpd is reached at by the other
	// nodes. Defaults to the host of RaftAddr.
	NTPAddr string `yaml:"ntp_addr"`
}

// TimeConfig configures the time controller and its workers. The controller
// is enabled when StateFile is set.
type TimeConfig struct {
	// StateFile is where a node keeps its time state (epoch, last time mode,
	// time delta) between starts. "{node}" is replaced by the node ID.
	StateFile string `yaml:"state_file"`
	// External lists the external time servers, as host or host:port.
	External []string `yaml:"external"`
	// NTPConf is the ntp.conf of the Meinberg NTP installation, Service its
	// Windows service name. Defaults: the standard installation, "NTP".
	NTPConf string `yaml:"ntp_conf"`
	Service string `yaml:"service"`
	// DryRun leaves ntpd and the clock alone and only logs what would be
	// done to them.
	DryRun bool `yaml:"dry_run"`
	// CheckInterval is how often the external servers are asked (default
	// 2s). A server whose time moves by JumpThreshold within JumpWindow is
	// no longer followed (defaults 2s and 10s).
	CheckInterval time.Duration `yaml:"check_interval"`
	JumpThreshold time.Duration `yaml:"jump_threshold"`
	JumpWindow    time.Duration `yaml:"jump_window"`
	// DependsOn lists controllers that must be in given states before the
	// time controller starts.
	DependsOn []control.Requirement `yaml:"depends_on"`
}

// KafkaConfig configures the kafka controller and its workers. The controller
// is enabled when Home is set.
type KafkaConfig struct {
	// Home is the Kafka installation directory (the one holding libs/ and
	// config/). Kafka 3.9 or newer: the controller quorum is dynamic.
	Home string `yaml:"home"`
	// DataDir is the base directory; each node uses DataDir/<node id>.
	// Defaults to <temp>/kaft-kafka.
	DataDir string `yaml:"data_dir"`
	// ClusterID is the fixed cluster ID all nodes are formatted with.
	ClusterID string `yaml:"cluster_id"`
	// Members is how many nodes the cluster should have. Defaults to
	// expected_bootstrap.
	Members int `yaml:"members"`
	// Heap is the maximum heap of each Kafka process. Defaults to 512M.
	Heap string `yaml:"heap"`
	// DependsOn lists controllers that must be in given states before the
	// cluster is formed.
	DependsOn []control.Requirement `yaml:"depends_on"`
}

// MongoConfig configures the mongo controller and its workers. The controller
// is enabled when Bin is set.
type MongoConfig struct {
	// Bin is the path of the mongod executable.
	Bin string `yaml:"bin"`
	// DataDir is the base directory; each node uses DataDir/<node id>.
	// Defaults to <temp>/kaft-mongo.
	DataDir string `yaml:"data_dir"`
	// ReplSet is the replica set name. Defaults to rs0.
	ReplSet string `yaml:"replset"`
	// Members is how many nodes the replica set should have. Defaults to
	// expected_bootstrap.
	Members  int    `yaml:"members"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// Args are extra mongod command line arguments.
	Args []string `yaml:"args"`
	// DependsOn lists controllers that must be in given states before the
	// replica set is formed.
	DependsOn []control.Requirement `yaml:"depends_on"`
}

type ClusterConfig struct {
	ExpectedBootstrap int          `yaml:"expected_bootstrap"`
	Nodes             []NodeConfig `yaml:"nodes"`
	Mongo             MongoConfig  `yaml:"mongo"`
	Kafka             KafkaConfig  `yaml:"kafka"`
	Time              TimeConfig   `yaml:"time"`
}

func ParseConfig(filePath string) (*ClusterConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("couldn't read config file: %w", err)
	}

	return ParseConfigBytes(data)
}

func ParseConfigBytes(data []byte) (*ClusterConfig, error) {
	var cfg ClusterConfig

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml parse error: %w", err)
	}

	return &cfg, nil
}
