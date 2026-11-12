package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"time"

	timefsm "example.com/dstate/internal/fsm/time"
	"example.com/dstate/internal/util"
	"github.com/blindson76/gstate"
	"github.com/hashicorp/raft"
)

type ClusterFSM struct {
	timeMachine *gstate.Machine[string, string, timefsm.TimeMachine]
	timeActor   *gstate.Actor[string, string, timefsm.TimeMachine]
	stepCh      chan struct{}
}

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (fsm *ClusterFSM) Apply(l *raft.Log) interface{} {
	log.Printf("fsm Apply")
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case "FIRE":
		fsm.timeActor.Send(cmd.Value)
	default:
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}
	return fsm.timeActor.Snapshot()
}

func (fsm *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	snap := fsm.timeActor.Snapshot()
	log.Printf("Snapshot machine:%v", snap)
	bytes, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	return &ClusterSnapshot{data: bytes}, nil
}

func (fsm *ClusterFSM) Restore(rc io.ReadCloser) error {
	log.Println("Restoring machine")
	defer rc.Close()
	var snap gstate.Snapshot[string, timefsm.TimeMachine]
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return err
	}

	if fsm.timeActor != nil {
		fsm.timeActor.Stop()
	}

	// Disable automatic invoke/timer restarts on restore to preserve determinism
	fsm.timeActor = gstate.Hydrate(
		fsm.timeMachine,
		snap,
		fsm.timeMachine.WithHydrateServices(false),
	)
	return nil
}

// --- 2. Snapshot Serialization ---

type ClusterSnapshot struct {
	data []byte
}

func (s *ClusterSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *ClusterSnapshot) Release() {}

type Cluster struct {
	nodesMap  map[string]NodeConfig
	nodeID    string
	nodes     []string
	expected  int
	bsManager *ClusterBootstrapController
	r         *raft.Raft
}

func NewCluster(self string, cfg *ClusterConfig) *Cluster {
	nodes := make([]string, 0)
	nodesMap := map[string]NodeConfig{}
	for _, node := range cfg.Nodes {
		nodes = append(nodes, node.ID)
		nodesMap[node.ID] = node
	}
	slices.Sort(nodes)
	c := &Cluster{
		nodeID:   self,
		nodesMap: nodesMap,
		nodes:    nodes,
		expected: cfg.ExpectedBootstrap,
	}
	delayBootstrap := time.Second + time.Duration(slices.Index(nodes, c.nodeID)*int(time.Second))
	c.bsManager = NewClusterManager(nodes, cfg.ExpectedBootstrap, delayBootstrap, c)
	return c
}
func (c *Cluster) Bootstrap(ctx context.Context, nodes []string) error {
	log.Printf("Bootstraping cluster with %+v", nodes)
	servers := make([]raft.Server, 0)
	for _, node := range nodes {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(node),
			Address: raft.ServerAddress(c.nodesMap[node].RaftAddr),
		})
	}

	err := c.r.BootstrapCluster(raft.Configuration{

		Servers: servers,
	}).Error()
	log.Printf("Bootstrap resutl:%v", err)
	return err

}

func (c *Cluster) Join(node string) {
	log.Println("Adding new node", node, c.r.Leader())
	if c.r.Leader() == raft.ServerAddress(c.nodeID) {
		if err := c.r.AddVoter(raft.ServerID(node), raft.ServerAddress(c.nodesMap[node].RaftAddr), 0, time.Second*10); err != nil {
			log.Printf("Add voter error:%v", err)
		}
	}

}
func (c *Cluster) WaitNode(ctx context.Context, node string) {
	util.WaitTCP(ctx, c.nodesMap[node].RaftAddr, 2*time.Second)
	log.Println("Node ready", node)
}

func (c *Cluster) Start(ctx context.Context) {
	nodeID := c.nodeID
	raftAddr := c.nodesMap[nodeID].RaftAddr

	// 1. Setup Raft Configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.LogOutput = io.Discard

	// Fast election speeds for single node local testing
	// config.HeartbeatTimeout = 50 * time.Millisecond
	// config.ElectionTimeout = 50 * time.Millisecond
	// config.LeaderLeaseTimeout = 50 * time.Millisecond

	// 2. Pure In-Memory Log and Stable Storage (Replacing BoltDB)
	memStore := raft.NewInmemStore()

	// 3. Pure In-Memory Snapshot Store
	snapshotStore := raft.NewInmemSnapshotStore()

	// 4. In-Memory / Local Transport Loop
	address, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatalf("failed to resolve tcp address: %v", err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, address, 3, 10*time.Second, io.Discard)
	if err != nil {
		log.Fatalf("failed to build network transport: %v", err)
	}

	// 5. Initialize the volatile State Machine
	timeMachine := timefsm.NewMachine()
	timeActor := gstate.Start(timeMachine, timefsm.TimeMachine{}, timeMachine.WithServiceActivation(false), timeMachine.WithRaiseHandler(func(ctx context.Context, e gstate.RaisedEvent[string, string]) {
		log.Printf("Machine internal event:%v", e)
	}))
	fsm := &ClusterFSM{
		timeMachine: timeMachine,
		timeActor:   timeActor,
		stepCh:      make(chan struct{}, 1),
	}

	// 6. Instantiate Raft Node using pure in-memory parameters
	r, err := raft.NewRaft(config, fsm, memStore, memStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("failed to construct raft node: %v", err)
	}
	c.r = r

	eventChan := make(chan raft.Observation, 10)
	observer := raft.NewObserver(eventChan, true, func(o *raft.Observation) bool {
		return true
	})
	r.RegisterObserver(observer)
	go func() {
		for obs := range eventChan {
			// log.Printf("Raft obs:%v", obs)
			c.bsManager.NotifyBootstraped()
			switch data := obs.Data.(type) {
			case raft.RaftState:
				log.Printf("RaftState:%v", data)
				if data == raft.Leader {
					log.Printf("Activating machine")
					fsm.timeActor.Activate()
				}
			case raft.LeaderObservation:
				log.Printf("Leader obs:%v", data.Leader)
				// case raft.PeerObservation:
				// 	log.Printf("Peer obs:%v", data)
				// case raft.FailedHeartbeatObservation:
				// 	log.Printf("FailedHeartbeatObservation obs:%v", data)
				// case raft.ResumedHeartbeatObservation:
				// 	log.Printf("ResumedHeartbeatObservation obs:%v", data)
				// case raft.RequestPreVoteRequest:
				// 	log.Printf("RequestPreVoteRequest obs:%v", data)
				// default:
				// 	log.Printf("Obs: %+v", obs)
			}

		}
	}()
	go func() {

		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			if r.State() == raft.Leader {
				log.Println("send tick")
				fsm.timeActor.Send("tick")
				continue
			}

		}

	}()
	c.bsManager.Start(ctx)

}
