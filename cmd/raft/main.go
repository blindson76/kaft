package main

import (
	"context"
	"flag"

	"example.com/dstate/internal"
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
	cluster.Start(context.Background())
	select {}
}
