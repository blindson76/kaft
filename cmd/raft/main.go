package main

import (
	"context"
	"flag"

	"example.com/dstate/internal"
	"example.com/dstate/internal/controllers/demo"
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
	// (node-local actions). Add new services here.
	cluster.Register(demo.NewController(cfg.ExpectedBootstrap), demo.NewWorker())
	cluster.Start(context.Background())
	select {}
}
