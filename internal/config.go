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
}

type ClusterConfig struct {
	ExpectedBootstrap int          `yaml:"expected_bootstrap"`
	Nodes             []NodeConfig `yaml:"nodes"`
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
