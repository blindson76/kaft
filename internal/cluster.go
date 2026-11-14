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
	send        func(string) error
}

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (fsm *ClusterFSM) Apply(l *raft.Log) interface{} {
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
	// log.Printf("Snapshot machine:%+v", snap)
	bytes, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	// log.Printf("SNAP:%s", string(bytes))
	return &ClusterSnapshot{data: bytes}, nil
}

func (fsm *ClusterFSM) Restore(rc io.ReadCloser) error {
	log.Println("Restoring machine")
	defer rc.Close()
	var snap gstate.Snapshot[string, timefsm.TimeMachine]
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		log.Printf("Restoring error:%+v", err)
		return err
	}

	if fsm.timeActor != nil {
		fsm.timeActor.Stop()
	}

	// Disable automatic invoke/timer restarts on restore to preserve determinism
	fsm.timeActor = gstate.Hydrate(
		fsm.timeMachine,
		snap,
		fsm.timeMachine.WithServiceActivation(false),
		fsm.timeMachine.WithInternalViaHandler(true),
		fsm.timeMachine.WithRaiseHandler(func(ctx context.Context, e gstate.RaisedEvent[string, string]) {
			// log.Printf("Machine internal event:%v", e)
			fsm.send(e.Event)
		}),
	)

	log.Printf("Restored machine:%+v", fsm.timeActor.Snapshot())
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
	delayBootstrap := time.Second*3 + time.Duration(slices.Index(nodes, c.nodeID)*int(time.Second))
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

const (
	reconcileInterval = time.Second
	probeTimeout      = 500 * time.Millisecond
	deadNodeTimeout   = 5 * time.Second
	membershipTimeout = 5 * time.Second
)

// reconcileMembership runs on every node but only acts while this node is the
// raft leader. Each tick it compares the raft configuration with what is
// actually reachable: configured nodes that are up but not members are added
// as voters, and members unreachable for deadNodeTimeout are removed so the
// quorum shrinks with the cluster. Membership never shrinks below `expected`.
// At most one change is made per tick, since raft applies configuration
// changes one at a time.
func (c *Cluster) reconcileMembership(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	downSince := map[raft.ServerID]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if c.r.State() != raft.Leader {
			clear(downSince)
			continue
		}
		cf := c.r.GetConfiguration()
		if err := cf.Error(); err != nil {
			continue
		}
		servers := cf.Configuration().Servers
		members := make(map[raft.ServerID]bool, len(servers))
		for _, s := range servers {
			members[s.ID] = true
		}

		changed := false
		for _, node := range c.nodes {
			if node == c.nodeID {
				continue
			}
			id := raft.ServerID(node)
			addr := c.nodesMap[node].RaftAddr

			if util.ProbeTCP(ctx, addr, probeTimeout) {
				delete(downSince, id)
				if members[id] || changed {
					continue
				}
				log.Println("Adding node", node)
				if err := c.r.AddVoter(id, raft.ServerAddress(addr), 0, membershipTimeout).Error(); err != nil {
					log.Printf("Add voter error:%v", err)
				}
				changed = true
				continue
			}

			if !members[id] {
				continue
			}
			since, ok := downSince[id]
			if !ok {
				downSince[id] = time.Now()
				continue
			}
			if changed || time.Since(since) < deadNodeTimeout || len(servers) <= c.expected {
				continue
			}
			log.Printf("Removing lost node %s (unreachable for %v)", node, time.Since(since).Round(time.Second))
			if err := c.r.RemoveServer(id, 0, membershipTimeout).Error(); err != nil {
				log.Printf("Remove server error:%v", err)
			}
			delete(downSince, id)
			changed = true
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
	// A removed node must keep running so it can be re-added once reachable.
	config.ShutdownOnRemove = false

	config.SnapshotInterval = time.Millisecond * 400
	config.SnapshotThreshold = 5
	config.TrailingLogs = 50

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
	timeActor := gstate.Start(timeMachine, timefsm.TimeMachine{},
		timeMachine.WithServiceActivation(false),
		timeMachine.WithInternalViaHandler(true),
		timeMachine.WithRaiseHandler(func(ctx context.Context, e gstate.RaisedEvent[string, string]) {
			// log.Printf("Machine internal event:%v", e)
			c.Send(e.Event)
		}),
	)
	fsm := &ClusterFSM{
		timeMachine: timeMachine,
		timeActor:   timeActor,
		send: func(event string) error {
			return c.Send(event)
		},
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
					go fsm.timeActor.Activate()
				}
			case raft.LeaderObservation:
				log.Printf("Leader obs:%v", data.Leader)
				// case raft.PeerObservation:
				// 	log.Printf("Peer obs:%v", data)
				// case raft.FailedHeartbeatObservation:
				// 	log.Printf("FailedHeartbeatObservation obs:%v", data)
				// if r.State() == raft.Leader {
				// 	log.Printf("Removing node:%v", data.PeerID)
				// 	r.RemoveServer(data.PeerID, 0, time.Second*3)
				// }
				// case raft.ResumedHeartbeatObservation:
				// 	log.Printf("ResumedHeartbeatObservation obs:%v", data)
				// case raft.RequestPreVoteRequest:
				// 	log.Printf("RequestPreVoteRequest obs:%v", data)
				// default:
				// 	log.Printf("Obs: %+v", obs)
			}

		}
	}()
	go c.reconcileMembership(ctx)
	c.bsManager.Start(ctx)

}

func (c *Cluster) Send(event string, val ...any) error {
	if c.r.State() != raft.Leader {
		return nil
	}
	command := Command{
		Op:    "FIRE",
		Value: event,
	}
	payload, err := json.Marshal(command)
	if err != nil {
		fmt.Printf("Failed to marshall command. %v : %w", command, err)
		return err
	}
	// log.Printf("Sending event %v", event)
	return c.r.Apply(payload, 2*time.Second).Error()
}
