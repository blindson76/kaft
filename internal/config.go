package internal

import (
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"example.com/dstate/internal/control"

	"gopkg.in/yaml.v3"
)

// Node roles.
const (
	// RoleServer runs raft and the controllers: it holds a replica of the
	// cluster state and takes part in the quorum.
	RoleServer = "server"
	// RoleWorker runs the workers of the controllers listed in the node's
	// Controllers: it carries out the orders for its local services.
	RoleWorker = "worker"
)

type NodeConfig struct {
	ID string `yaml:"id"`
	// Roles is what the node runs: "server", "worker" or both. Defaults to
	// both.
	Roles []string `yaml:"roles"`
	// Controllers lists the controllers whose worker runs on this node, by
	// name (mongo, kafka, time). Only meaningful for a worker. Defaults to
	// every controller enabled in the config.
	Controllers []string `yaml:"controllers"`
	// RaftAddr is required for a server.
	RaftAddr string `yaml:"raft_addr"`
	// HTTPAddr is required for every node: servers take events there, and
	// the leader checks the liveness of worker-only nodes against it.
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
	// nodes. Defaults to the host of RaftAddr, or of HTTPAddr.
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

// Controller names, as used in NodeConfig.Controllers. They match the names
// the controllers register under.
const (
	ControllerMongo = "mongo"
	ControllerKafka = "kafka"
	ControllerTime  = "time"
)

// IsServer reports whether the node runs raft and the controllers.
func (n NodeConfig) IsServer() bool { return slices.Contains(n.Roles, RoleServer) }

// IsWorker reports whether the node runs workers.
func (n NodeConfig) IsWorker() bool { return slices.Contains(n.Roles, RoleWorker) }

// RunsWorker reports whether the node runs the worker of controller.
func (n NodeConfig) RunsWorker(controller string) bool {
	return n.IsWorker() && slices.Contains(n.Controllers, controller)
}

// Host returns the host the node is reached at, from RaftAddr or HTTPAddr.
func (n NodeConfig) Host() string {
	for _, addr := range []string{n.RaftAddr, n.HTTPAddr} {
		if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
			return host
		}
	}
	return ""
}

// Enabled returns the controllers this config turns on, in a fixed order.
func (c *ClusterConfig) Enabled() []string {
	var out []string
	if c.Mongo.Bin != "" {
		out = append(out, ControllerMongo)
	}
	if c.Kafka.Home != "" {
		out = append(out, ControllerKafka)
	}
	if c.Time.StateFile != "" {
		out = append(out, ControllerTime)
	}
	return out
}

// Node returns the config of node id.
func (c *ClusterConfig) Node(id string) (NodeConfig, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeConfig{}, false
}

// Servers returns the server nodes, in config order.
func (c *ClusterConfig) Servers() []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if n.IsServer() {
			out = append(out, n)
		}
	}
	return out
}

// WorkersOf returns the nodes that run the worker of controller, in config
// order.
func (c *ClusterConfig) WorkersOf(controller string) []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if n.RunsWorker(controller) {
			out = append(out, n)
		}
	}
	return out
}

// normalize fills in the defaults: a node without roles is both a server and
// a worker, and a worker without controllers runs every enabled one.
func (c *ClusterConfig) normalize() {
	enabled := c.Enabled()
	for i := range c.Nodes {
		n := &c.Nodes[i]
		if len(n.Roles) == 0 {
			n.Roles = []string{RoleServer, RoleWorker}
		}
		for j, r := range n.Roles {
			n.Roles[j] = strings.ToLower(strings.TrimSpace(r))
		}
		if n.IsWorker() && n.Controllers == nil {
			n.Controllers = slices.Clone(enabled)
		}
	}
}

// Validate checks the node roles and controller assignments.
func (c *ClusterConfig) Validate() error {
	var errs []error
	enabled := c.Enabled()
	seen := map[string]bool{}
	for _, n := range c.Nodes {
		if n.ID == "" {
			errs = append(errs, errors.New("a node has no id"))
			continue
		}
		if seen[n.ID] {
			errs = append(errs, fmt.Errorf("node %s: duplicate id", n.ID))
		}
		seen[n.ID] = true
		for _, r := range n.Roles {
			if r != RoleServer && r != RoleWorker {
				errs = append(errs, fmt.Errorf("node %s: unknown role %q (want %s or %s)", n.ID, r, RoleServer, RoleWorker))
			}
		}
		if n.HTTPAddr == "" {
			errs = append(errs, fmt.Errorf("node %s: http_addr is required", n.ID))
		}
		if n.IsServer() && n.RaftAddr == "" {
			errs = append(errs, fmt.Errorf("node %s: a server needs raft_addr", n.ID))
		}
		if !n.IsWorker() && len(n.Controllers) > 0 {
			errs = append(errs, fmt.Errorf("node %s: controllers are given but the node is not a worker", n.ID))
		}
		for _, name := range n.Controllers {
			if !slices.Contains(enabled, name) {
				errs = append(errs, fmt.Errorf("node %s: controller %q is unknown or not enabled (enabled: %s)", n.ID, name, strings.Join(enabled, ", ")))
			}
		}
		if n.RunsWorker(ControllerMongo) && n.MongoAddr == "" {
			errs = append(errs, fmt.Errorf("node %s: the mongo worker needs mongo_addr", n.ID))
		}
		if n.RunsWorker(ControllerKafka) && (n.KafkaAddr == "" || n.KafkaControllerAddr == "") {
			errs = append(errs, fmt.Errorf("node %s: the kafka worker needs kafka_addr and kafka_controller_addr", n.ID))
		}
	}
	servers := len(c.Servers())
	if servers == 0 {
		errs = append(errs, errors.New("no server node"))
	}
	if c.ExpectedBootstrap > servers {
		errs = append(errs, fmt.Errorf("expected_bootstrap is %d but there are only %d servers", c.ExpectedBootstrap, servers))
	}
	return errors.Join(errs...)
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
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}
