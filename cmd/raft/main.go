package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"example.com/dstate/internal"
	"example.com/dstate/internal/controllers/mongoctl"
)

func main() {

	var nodeID string = ""
	flag.StringVar(&nodeID, "node-id", "node-1", "node id")
	flag.Parse()
	cfg, err := internal.ParseConfig("config.yml")
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
