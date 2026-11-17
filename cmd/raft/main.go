package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"example.com/dstate/internal"
	"example.com/dstate/internal/control"
	"example.com/dstate/internal/controllers/kafkactl"
	"example.com/dstate/internal/controllers/mongoctl"
	"example.com/dstate/internal/controllers/taskctl"
	"example.com/dstate/internal/controllers/timectl"
)

func main() {
	var nodeID string = ""
	flag.StringVar(&nodeID, "node-id", "node-1", "node id")
	configPath := flag.String("config", "config.yml", "config file")
	shutdownTimeout := flag.Duration("shutdown-timeout", time.Minute,
		"how long a shutdown may take; services still running then are killed")
	flag.Parse()
	cfg, err := internal.ParseConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// The node runs until interrupted: Ctrl+C, or SIGTERM, which is also what
	// Windows sends when the console is closed, the user logs off or the
	// machine shuts down.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cluster := internal.NewCluster(nodeID, cfg)
	// Each service is one controller (replicated decisions, on the servers)
	// plus one worker (node-local actions, on the workers listing it). Add
	// new services here. internal/controllers/demo is a minimal template.
	if cfg.Mongo.Bin != "" {
		registerMongo(cluster, nodeID, cfg)
	}
	if cfg.Kafka.Home != "" {
		registerKafka(cluster, nodeID, cfg)
	}
	if cfg.Time.StateFile != "" {
		registerTime(cluster, nodeID, cfg)
	}
	for _, t := range cfg.Tasks {
		registerTask(cluster, nodeID, t, cfg)
	}

	stopDNS, err := internal.StartDNS(cfg, nodeID)
	if err != nil {
		log.Fatalf("dns: %v", err)
	}
	if err := cluster.Start(ctx); err != nil {
		log.Printf("start: %v", err)
		stop() // shut down what did start
	}

	<-ctx.Done()
	// From here a second signal ends the process at once, without waiting
	// for the services to stop.
	stop()
	log.Printf("Shutting down (at most %v; interrupt again to quit at once)", *shutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	// DNS stops last: other nodes may need its answers while this one leaves.
	err = errors.Join(cluster.Shutdown(sctx), stopDNS(sctx))
	if err != nil {
		log.Printf("Shutdown incomplete: %v", err)
		os.Exit(1)
	}
	log.Printf("Shutdown complete")
}

// ids returns the IDs of nodes.
func ids(nodes []internal.NodeConfig) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

func registerMongo(cluster *internal.Cluster, nodeID string, cfg *internal.ClusterConfig) {
	m := cfg.Mongo
	if m.DataDir == "" {
		m.DataDir = filepath.Join(os.TempDir(), "kaft-mongo")
	}
	if m.ReplSet == "" {
		m.ReplSet = "rs0"
	}
	if m.User == "" {
		m.User, m.Password = "admin", "123"
	}

	workers := cfg.WorkersOf(internal.ControllerMongo)
	minSize, maxSize := mongoSizes(m, cfg.ExpectedBootstrap, len(workers))
	topology := mongoctl.Topology{Min: minSize, Max: maxSize, Addrs: map[string]string{}, Window: m.DecisionWindow}
	for _, n := range workers {
		topology.Addrs[n.ID] = n.MongoAddr
	}

	var worker control.Worker
	if cluster.RunsWorker(internal.ControllerMongo) {
		worker = mongoctl.NewWorker(mongoctl.WorkerConfig{
			Node:     nodeID,
			Addr:     topology.Addrs[nodeID],
			Bin:      m.Bin,
			DataDir:  filepath.Join(m.DataDir, nodeID),
			ReplSet:  m.ReplSet,
			User:     m.User,
			Password: m.Password,
			Args:     m.Args,
		})
	}
	cluster.Register(mongoctl.NewController(topology, m.DependsOn...), ids(workers), worker)
}

// defaultKafkaClusterID is used when the config gives none. Any fixed value
// works; all nodes must format with the same one.
const defaultKafkaClusterID = "MkU3OEVBNTcwNTJENDM2Qg"

func registerKafka(cluster *internal.Cluster, nodeID string, cfg *internal.ClusterConfig) {
	k := cfg.Kafka
	if k.DataDir == "" {
		k.DataDir = filepath.Join(os.TempDir(), "kaft-kafka")
	}
	if k.ClusterID == "" {
		k.ClusterID = defaultKafkaClusterID
	}
	if k.Members == 0 {
		k.Members = cfg.ExpectedBootstrap
	}

	workers := cfg.WorkersOf(internal.ControllerKafka)
	topology := kafkactl.Topology{Size: k.Members, IDs: map[string]int{}, Controllers: map[string]string{}}
	var bootstrap []string
	for _, n := range workers {
		topology.IDs[n.ID] = n.KafkaID
		topology.Controllers[n.ID] = n.KafkaControllerAddr
		bootstrap = append(bootstrap, n.KafkaControllerAddr)
	}

	var worker control.Worker
	if cluster.RunsWorker(internal.ControllerKafka) {
		self, _ := cfg.Node(nodeID)
		worker = kafkactl.NewWorker(kafkactl.WorkerConfig{
			Node:           nodeID,
			ID:             self.KafkaID,
			Addr:           self.KafkaAddr,
			ControllerAddr: self.KafkaControllerAddr,
			Bootstrap:      bootstrap,
			Home:           k.Home,
			DataDir:        filepath.Join(k.DataDir, nodeID),
			ClusterID:      k.ClusterID,
			Replication:    min(k.Members, 3),
			Heap:           k.Heap,
		})
	}
	cluster.Register(kafkactl.NewController(topology, k.DependsOn...), ids(workers), worker)
}

func registerTime(cluster *internal.Cluster, nodeID string, cfg *internal.ClusterConfig) {
	t := cfg.Time
	if t.NTPConf == "" {
		t.NTPConf = `C:\Program Files (x86)\NTP\etc\ntp.conf`
	}
	if t.Service == "" {
		t.Service = "NTP"
	}

	workers := cfg.WorkersOf(internal.ControllerTime)
	topology := timectl.Topology{Addrs: map[string]string{}, External: t.External}
	for _, n := range workers {
		addr := n.NTPAddr
		if addr == "" {
			addr = n.Host()
		}
		topology.Addrs[n.ID] = addr
	}

	var worker control.Worker
	if cluster.RunsWorker(internal.ControllerTime) {
		var system timectl.System = timectl.Meinberg{Conf: t.NTPConf, Service: t.Service}
		if t.DryRun {
			system = &timectl.DryRun{}
		}
		worker = timectl.NewWorker(timectl.WorkerConfig{
			Node:          nodeID,
			StateFile:     strings.ReplaceAll(t.StateFile, "{node}", nodeID),
			External:      t.External,
			Interval:      t.CheckInterval,
			JumpThreshold: t.JumpThreshold,
			JumpWindow:    t.JumpWindow,
			System:        system,
		})
	}
	cluster.Register(timectl.NewController(topology, t.DependsOn...), ids(workers), worker)
	for pattern, handler := range timectl.Routes(cluster.Submit) {
		cluster.Handle(pattern, handler)
	}
}

// registerTask adds a task's controller, and its worker if this node is one
// of the nodes the task may run on.
func registerTask(cluster *internal.Cluster, nodeID string, t internal.TaskConfig, cfg *internal.ClusterConfig) {
	nodes := cfg.TaskNodes(t)
	topology := taskctl.Topology{Instances: t.Instances, MaxPerNode: t.MaxPerNode, Nodes: ids(nodes), Hosts: map[string]string{}}
	for _, n := range nodes {
		topology.Hosts[n.ID] = cmp.Or(n.IP, n.Host())
	}
	roles := map[string]taskctl.RoleConfig{}
	for _, r := range t.Roles {
		roles[r.Name] = taskctl.RoleConfig{Args: r.Args, Env: r.Env}
		for i := range r.Count {
			topology.Slots = append(topology.Slots, taskctl.Slot{Role: r.Name, Replica: i, Refs: t.RoleRefs(r)})
		}
	}

	var worker control.Worker
	if slices.Contains(topology.Nodes, nodeID) {
		logDir := filepath.Join(os.TempDir(), "kaft-tasks", nodeID)
		if t.LogDir != "" {
			logDir = strings.ReplaceAll(t.LogDir, "{node}", nodeID)
		}
		worker = taskctl.NewWorker(taskctl.WorkerConfig{
			Task:        t.Name,
			Node:        nodeID,
			Command:     t.Command,
			Args:        t.Args,
			Env:         t.Env,
			Dir:         t.Dir,
			Roles:       roles,
			LogDir:      logDir,
			StopTimeout: t.StopTimeout,
		})
	}
	cluster.Register(taskctl.NewController(t.Name, topology, t.DependsOn...), topology.Nodes, worker)
}

// maxVoters is MongoDB's limit on voting members in a replica set.
const maxVoters = 7

// mongoSizes resolves the smallest and largest replica set from the config;
// see MongoConfig. candidates is the number of mongo nodes.
func mongoSizes(m internal.MongoConfig, expected, candidates int) (int, int) {
	minSize, maxSize := m.MinMembers, m.MaxMembers
	if minSize == 0 {
		minSize = cmp.Or(m.Members, expected)
	}
	if maxSize == 0 {
		maxSize = cmp.Or(m.Members, min(candidates, maxVoters))
	}
	switch {
	case minSize < 1:
		log.Fatalf("mongo: min_members must be at least 1")
	case maxSize < minSize:
		log.Fatalf("mongo: max_members (%d) is smaller than min_members (%d)", maxSize, minSize)
	case maxSize > maxVoters:
		log.Fatalf("mongo: max_members (%d) is above %d, MongoDB's limit of voting members", maxSize, maxVoters)
	case minSize > candidates:
		log.Fatalf("mongo: min_members (%d) is more than the %d mongo nodes in the config", minSize, candidates)
	}
	return minSize, maxSize
}
