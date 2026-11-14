package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID       string `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
	HTTPAddr string `yaml:"http_addr"`
	// MongoAddr is the host:port the node's mongod serves replica set
	// traffic on; it is also the member host in the replica set config.
	MongoAddr string `yaml:"mongo_addr"`
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
}

type ClusterConfig struct {
	ExpectedBootstrap int          `yaml:"expected_bootstrap"`
	Nodes             []NodeConfig `yaml:"nodes"`
	Mongo             MongoConfig  `yaml:"mongo"`
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
