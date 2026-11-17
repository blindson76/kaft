package internal

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"example.com/dstate/internal/control"

	"gopkg.in/yaml.v3"
)

// Node roles.
const (
	// RoleServer makes the node a raft voter: it takes part in the quorum,
	// can become leader and then drives the controllers.
	RoleServer = "server"
	// RoleWorker runs the workers of the controllers listed in the node's
	// Controllers: it carries out the orders for its local services. A
	// worker that is not also a server is a raft non-voter: it keeps a
	// replica of the cluster state but has no say in the quorum.
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
	// RaftAddr is required for every node: servers are voters and worker-only
	// nodes non-voters. The leader also checks liveness against it.
	RaftAddr string `yaml:"raft_addr"`
	// HTTPAddr is required for every node: the leader takes events there.
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
	// IP is the address the node's DNS names resolve to. Defaults to the
	// host of RaftAddr, or of HTTPAddr.
	IP string `yaml:"ip"`
	// DNSListen overrides dns.listen for this node.
	DNSListen string `yaml:"dns_listen"`
	// DNSNames are extra names for this node in the DNS zone.
	DNSNames []string `yaml:"dns_names"`
}

// DNSConfig configures the DNS server every node runs. It is enabled when
// Listen is set.
type DNSConfig struct {
	// Listen is the address the server listens on, UDP and TCP, e.g. ":53".
	Listen string `yaml:"listen"`
	// Zone is the domain the names live in. Defaults to "service".
	Zone string `yaml:"zone"`
	// TTL of the answers. Defaults to 10s.
	TTL time.Duration `yaml:"ttl"`
	// Forward lists DNS servers (host or host:port) that names outside the
	// zone are passed on to, in order. Empty: such queries are refused.
	Forward []string `yaml:"forward"`
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
	// MinMembers is the smallest replica set: forming one waits for this
	// many mongo nodes, and dead members are removed only while at least
	// this many remain. MaxMembers is the largest: a running set grows into
	// spare mongo nodes up to it (at most 7). Defaults: MinMembers is
	// Members, else expected_bootstrap; MaxMembers is Members if set, else
	// every mongo node.
	MinMembers int `yaml:"min_members"`
	MaxMembers int `yaml:"max_members"`
	// Members sets both sizes at once: a replica set of fixed size.
	Members  int    `yaml:"members"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// Args are extra mongod command line arguments.
	Args []string `yaml:"args"`
	// DecisionWindow is how long forming the replica set waits for
	// candidates that join late (e.g. "30s"). It starts once MinMembers nodes
	// have reported and ends early once every mongo node has. Zero: decide as
	// soon as the nodes that are up have.
	DecisionWindow time.Duration `yaml:"decision_window"`
	// DependsOn lists controllers that must be in given states before the
	// replica set is formed.
	DependsOn []control.Requirement `yaml:"depends_on"`
}

// TaskConfig is a generic task: a command kept running as Instances processes
// on the worker nodes in Nodes. Each task is a controller of its own, named
// Name, so other controllers and tasks can depend on it (states: waiting,
// degraded, healthy).
type TaskConfig struct {
	// Name is the task's controller name: lower-case letters, digits and
	// '-', not the name of a built-in controller.
	Name string `yaml:"name"`
	// Command, Args, Dir and Env values may use {task}, {instance} (0, 1...),
	// {node}, {role}, {replica} and, with roles, {node:R} and {host:R}: the
	// node and host the instance of role R runs on (see Roles). The process
	// also gets KAFT_TASK, KAFT_INSTANCE, KAFT_NODE, KAFT_GEN, KAFT_ROLE and
	// KAFT_REPLICA in its environment.
	Command string            `yaml:"command"`
	Args    []string          `yaml:"args"`
	Env     map[string]string `yaml:"env"`
	Dir     string            `yaml:"dir"`
	// Instances is how many processes should run across the nodes. Default 1.
	// With Roles it is the sum of their counts, and may be left out.
	Instances int `yaml:"instances"`
	// Roles splits the instances into kinds that run with different
	// arguments, e.g. a master and a slave. Instances are numbered in role
	// order. An instance whose arguments or environment refer to another
	// role ({node:R}, {host:R}) starts only once that role's instance runs,
	// and is restarted with the new values when it moves. Use max_per_node: 1
	// to keep them on different nodes.
	Roles []TaskRole `yaml:"roles"`
	// Nodes are the worker nodes the task may run on. Default: every worker.
	Nodes []string `yaml:"nodes"`
	// MaxPerNode caps the instances one node runs. Default 0: no cap; the
	// instances are still spread, the least loaded node first.
	MaxPerNode int `yaml:"max_per_node"`
	// LogDir holds the instances' output and pid files; "{node}" is replaced
	// by the node ID. Default: <temp>/kaft-tasks/<node>.
	LogDir string `yaml:"log_dir"`
	// StopTimeout is how long a process gets to exit when stopped before it
	// is killed (where the OS can ask a process to exit). Default 10s.
	StopTimeout time.Duration `yaml:"stop_timeout"`
	// DependsOn lists controllers that must be in given states before any
	// instance starts, e.g. kafka in monitoring.
	DependsOn []control.Requirement `yaml:"depends_on"`
}

// TaskRole is one kind of instance of a task.
type TaskRole struct {
	Name string `yaml:"name"`
	// Count is how many instances have this role. Default 1.
	Count int `yaml:"count"`
	// Args come after the task's args; Env entries override the task's.
	Args []string          `yaml:"args"`
	Env  map[string]string `yaml:"env"`
}

// roleRef matches the placeholders that refer to another role's placement.
var roleRef = regexp.MustCompile(`\{(?:node|host):([^{}]+)\}`)

// RoleRefs returns the roles the instances of role refer to, sorted.
func (t TaskConfig) RoleRefs(role TaskRole) []string {
	texts := slices.Concat([]string{t.Command, t.Dir}, t.Args, role.Args)
	for _, env := range []map[string]string{t.Env, role.Env} {
		for _, v := range env {
			texts = append(texts, v)
		}
	}
	var out []string
	for _, text := range texts {
		for _, m := range roleRef.FindAllStringSubmatch(text, -1) {
			if !slices.Contains(out, m[1]) {
				out = append(out, m[1])
			}
		}
	}
	slices.Sort(out)
	return out
}

type ClusterConfig struct {
	ExpectedBootstrap int          `yaml:"expected_bootstrap"`
	Nodes             []NodeConfig `yaml:"nodes"`
	Mongo             MongoConfig  `yaml:"mongo"`
	Kafka             KafkaConfig  `yaml:"kafka"`
	Time              TimeConfig   `yaml:"time"`
	DNS               DNSConfig    `yaml:"dns"`
	Tasks             []TaskConfig `yaml:"tasks"`
}

// TaskNodes returns the worker nodes task may run on, in config order.
func (c *ClusterConfig) TaskNodes(task TaskConfig) []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if n.IsWorker() && (len(task.Nodes) == 0 || slices.Contains(task.Nodes, n.ID)) {
			out = append(out, n)
		}
	}
	return out
}

// taskName is what a task may be called: it is a controller name and shows up
// in file names.
var taskName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// validateTasks checks the tasks.
func (c *ClusterConfig) validateTasks() []error {
	var errs []error
	builtin := []string{ControllerMongo, ControllerKafka, ControllerTime}
	seen := map[string]bool{}
	for i, t := range c.Tasks {
		what := fmt.Sprintf("task %q", t.Name)
		switch {
		case t.Name == "":
			errs = append(errs, fmt.Errorf("task #%d has no name", i+1))
			continue
		case !taskName.MatchString(t.Name):
			errs = append(errs, fmt.Errorf("%s: a name has lower-case letters, digits and '-' only", what))
		case slices.Contains(builtin, t.Name):
			errs = append(errs, fmt.Errorf("%s: the name of a built-in controller", what))
		case seen[t.Name]:
			errs = append(errs, fmt.Errorf("%s: defined twice", what))
		}
		seen[t.Name] = true
		if t.Command == "" {
			errs = append(errs, fmt.Errorf("%s: command is required", what))
		}
		if t.Instances < 1 {
			errs = append(errs, fmt.Errorf("%s: instances must be at least 1", what))
		}
		if t.MaxPerNode < 0 {
			errs = append(errs, fmt.Errorf("%s: max_per_node cannot be negative", what))
		}
		errs = append(errs, t.validateRoles(what)...)
		for _, id := range t.Nodes {
			n, ok := c.Node(id)
			switch {
			case !ok:
				errs = append(errs, fmt.Errorf("%s: unknown node %q", what, id))
			case !n.IsWorker():
				errs = append(errs, fmt.Errorf("%s: node %s is not a worker", what, id))
			}
		}
		nodes := len(c.TaskNodes(t))
		switch {
		case nodes == 0:
			errs = append(errs, fmt.Errorf("%s: no worker node to run on", what))
		case t.MaxPerNode > 0 && t.Instances > t.MaxPerNode*nodes:
			errs = append(errs, fmt.Errorf("%s: %d instance(s) do not fit on %d node(s) with max_per_node %d", what, t.Instances, nodes, t.MaxPerNode))
		}
	}
	return errs
}

// Controller names, as used in NodeConfig.Controllers. They match the names
// the controllers register under.
const (
	ControllerMongo = "mongo"
	ControllerKafka = "kafka"
	ControllerTime  = "time"
)

// IsServer reports whether the node is a raft voter.
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
	for i := range c.Tasks {
		t := &c.Tasks[i]
		sum := 0
		for j := range t.Roles {
			if t.Roles[j].Count == 0 {
				t.Roles[j].Count = 1
			}
			sum += t.Roles[j].Count
		}
		if t.Instances == 0 {
			t.Instances = max(sum, 1)
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
		if n.RaftAddr == "" {
			errs = append(errs, fmt.Errorf("node %s: raft_addr is required", n.ID))
		}
		if !n.IsWorker() && len(n.Controllers) > 0 {
			errs = append(errs, fmt.Errorf("node %s: controllers are given but the node is not a worker", n.ID))
		}
		for _, name := range n.Controllers {
			if slices.ContainsFunc(c.Tasks, func(t TaskConfig) bool { return t.Name == name }) {
				errs = append(errs, fmt.Errorf("node %s: %q is a task; a task lists its nodes itself (tasks: nodes)", n.ID, name))
				continue
			}
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
	errs = append(errs, c.validateTasks()...)
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

// ParseConfigBytes parses a config document. Values may refer to environment
// variables; see expandEnv.
func ParseConfigBytes(data []byte) (*ClusterConfig, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("yaml parse error: %w", err)
	}
	if err := expandEnv(&doc, os.LookupEnv); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var cfg ClusterConfig
	if len(doc.Content) > 0 {
		if err := doc.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("yaml parse error: %w", err)
		}
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// validateRoles checks a task's roles and the references between them.
func (t TaskConfig) validateRoles(what string) []error {
	var errs []error
	count := map[string]int{}
	sum := 0
	for _, r := range t.Roles {
		switch {
		case !taskName.MatchString(r.Name):
			errs = append(errs, fmt.Errorf("%s: role %q: a name has lower-case letters, digits and '-' only", what, r.Name))
		case count[r.Name] > 0:
			errs = append(errs, fmt.Errorf("%s: role %q defined twice", what, r.Name))
		case r.Count < 1:
			errs = append(errs, fmt.Errorf("%s: role %q: count must be at least 1", what, r.Name))
		}
		count[r.Name] = max(r.Count, 1)
		sum += r.Count
	}
	if len(t.Roles) == 0 {
		if refs := t.RoleRefs(TaskRole{}); len(refs) > 0 {
			errs = append(errs, fmt.Errorf("%s: {node:%s} / {host:%s} refer to a role, but the task has no roles", what, refs[0], refs[0]))
		}
		return errs
	}
	if t.Instances != sum {
		errs = append(errs, fmt.Errorf("%s: instances is %d but the roles add up to %d", what, t.Instances, sum))
	}
	refs := map[string][]string{}
	for _, r := range t.Roles {
		refs[r.Name] = t.RoleRefs(r)
		for _, ref := range refs[r.Name] {
			switch {
			case ref == r.Name:
				errs = append(errs, fmt.Errorf("%s: role %q refers to its own placement", what, r.Name))
			case count[ref] == 0:
				errs = append(errs, fmt.Errorf("%s: role %q refers to unknown role %q", what, r.Name, ref))
			case count[ref] > 1:
				errs = append(errs, fmt.Errorf("%s: role %q refers to role %q, which has %d instances: only a role with one can be referred to", what, r.Name, ref, count[ref]))
			}
		}
	}
	// A cycle would leave every role in it waiting for another to run.
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(role string, path []string) error
	visit = func(role string, path []string) error {
		switch state[role] {
		case 1:
			return fmt.Errorf("%s: roles refer to each other in a cycle: %s", what, strings.Join(append(path, role), " -> "))
		case 2:
			return nil
		}
		state[role] = 1
		for _, ref := range refs[role] {
			if ref != role && count[ref] > 0 {
				if err := visit(ref, append(path, role)); err != nil {
					return err
				}
			}
		}
		state[role] = 2
		return nil
	}
	for _, r := range t.Roles {
		if err := visit(r.Name, nil); err != nil {
			errs = append(errs, err)
			break
		}
	}
	return errs
}
