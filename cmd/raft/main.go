package main

import (
	"cmp"
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"example.com/dstate/internal"
	"example.com/dstate/internal/control"
	"example.com/dstate/internal/controllers/kafkactl"
	"example.com/dstate/internal/controllers/mongoctl"
	"example.com/dstate/internal/controllers/timectl"
)

func main() {

	var nodeID string = ""
	flag.StringVar(&nodeID, "node-id", "node-1", "node id")
	configPath := flag.String("config", "config.yml", "config file")
	flag.Parse()
	cfg, err := internal.ParseConfig(*configPath)
	if err != nil {
		panic(err)
	}
	// log.Printf("Config:%+v\n", cfg)

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
	ctx := context.Background()
	if err := internal.StartDNS(ctx, cfg, nodeID); err != nil {
		log.Fatalf("dns: %v", err)
	}
	cluster.Start(ctx)
	select {}
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
