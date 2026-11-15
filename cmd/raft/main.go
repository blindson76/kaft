package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"example.com/dstate/internal"
	"example.com/dstate/internal/controllers/kafkactl"
	"example.com/dstate/internal/controllers/mongoctl"
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
	// Each service is one controller (replicated decisions) plus one worker
	// (node-local actions). Add new services here. internal/controllers/demo
	// is a minimal template.
	if cfg.Mongo.Bin != "" {
		registerMongo(cluster, nodeID, cfg)
	}
	if cfg.Kafka.Home != "" {
		registerKafka(cluster, nodeID, cfg)
	}
	cluster.Start(context.Background())
	select {}
}

func registerMongo(cluster *internal.Cluster, nodeID string, cfg *internal.ClusterConfig) {
	m := cfg.Mongo
	if m.DataDir == "" {
		m.DataDir = filepath.Join(os.TempDir(), "kaft-mongo")
	}
	if m.ReplSet == "" {
		m.ReplSet = "rs0"
	}
	if m.Members == 0 {
		m.Members = cfg.ExpectedBootstrap
	}
	if m.User == "" {
		m.User, m.Password = "admin", "123"
	}

	topology := mongoctl.Topology{Size: m.Members, Addrs: map[string]string{}}
	for _, n := range cfg.Nodes {
		if n.MongoAddr != "" {
			topology.Addrs[n.ID] = n.MongoAddr
		}
	}
	addr, ok := topology.Addrs[nodeID]
	if !ok {
		log.Fatalf("node %s has no mongo_addr in the config", nodeID)
	}

	cluster.Register(mongoctl.NewController(topology), mongoctl.NewWorker(mongoctl.WorkerConfig{
		Node:     nodeID,
		Addr:     addr,
		Bin:      m.Bin,
		DataDir:  filepath.Join(m.DataDir, nodeID),
		ReplSet:  m.ReplSet,
		User:     m.User,
		Password: m.Password,
		Args:     m.Args,
	}))
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

	topology := kafkactl.Topology{Size: k.Members, IDs: map[string]int{}, Controllers: map[string]string{}}
	var self internal.NodeConfig
	var bootstrap []string
	for _, n := range cfg.Nodes {
		if n.KafkaAddr == "" || n.KafkaControllerAddr == "" {
			continue
		}
		topology.IDs[n.ID] = n.KafkaID
		topology.Controllers[n.ID] = n.KafkaControllerAddr
		bootstrap = append(bootstrap, n.KafkaControllerAddr)
		if n.ID == nodeID {
			self = n
		}
	}
	if self.ID == "" {
		log.Fatalf("node %s has no kafka_addr / kafka_controller_addr in the config", nodeID)
	}

	cluster.Register(kafkactl.NewController(topology), kafkactl.NewWorker(kafkactl.WorkerConfig{
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
	}))
}
