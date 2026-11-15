package internal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"time"

	"example.com/dstate/internal/control"
	"example.com/dstate/internal/util"
	"github.com/hashicorp/raft"
)

// ClusterFSM is the raft state machine. All the logic lives in the control
// runtime; this type only translates between raft and control events.
type ClusterFSM struct {
	rt *control.Runtime
}

func (fsm *ClusterFSM) Apply(l *raft.Log) interface{} {
	var ev control.Event
	if err := json.Unmarshal(l.Data, &ev); err != nil {
		return err
	}
	fsm.rt.Apply(ev)
	return nil
}

func (fsm *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := fsm.rt.Snapshot()
	if err != nil {
		return nil, err
	}
	return &ClusterSnapshot{data: data}, nil
}

func (fsm *ClusterFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if err := fsm.rt.Restore(data); err != nil {
		log.Printf("Restore error:%v", err)
		return err
	}
	log.Println("Restored control state from snapshot")
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
	rt        *control.Runtime
	workers   []control.Worker
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
		rt:       control.NewRuntime(),
	}
	delayBootstrap := time.Second*3 + time.Duration(slices.Index(nodes, c.nodeID)*int(time.Second))
	c.bsManager = NewClusterManager(nodes, cfg.ExpectedBootstrap, delayBootstrap, c)
	return c
}

// Register adds a controller and its node-local worker. Every node must
// register the same controllers, before Start. worker may be nil for a
// controller that needs no local agent.
func (c *Cluster) Register(controller control.Controller, worker control.Worker) {
	c.rt.Register(controller)
	if worker != nil {
		c.workers = append(c.workers, worker)
	}
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
	applyTimeout      = 2 * time.Second
)

// reconcileMembership runs on every node but only acts while this node is the
// raft leader. Each tick it compares the raft configuration with what is
// actually reachable: configured nodes that are up but not members are added
// as voters, and members unreachable for deadNodeTimeout are removed so the
// quorum shrinks with the cluster. Membership never shrinks below `expected`.
// At most one change is made per tick, since raft applies configuration
// changes one at a time.
//
// The same probes feed the control layer: a liveness change is committed as a
// node.up / node.down event.
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

		c.syncLiveness(c.nodeID, true)

		changed := false
		for _, node := range c.nodes {
			if node == c.nodeID {
				continue
			}
			id := raft.ServerID(node)
			addr := c.nodesMap[node].RaftAddr

			if util.ProbeTCP(ctx, addr, probeTimeout) {
				delete(downSince, id)
				c.syncLiveness(node, true)
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

			since, ok := downSince[id]
			if !ok {
				downSince[id] = time.Now()
				continue
			}
			if time.Since(since) < deadNodeTimeout {
				continue
			}
			c.syncLiveness(node, false)
			if !members[id] || changed || len(servers) <= c.expected {
				continue
			}
			log.Printf("Removing lost node %s (unreachable for %v)", node, time.Since(since).Round(time.Second))
			if err := c.r.RemoveServer(id, 0, membershipTimeout).Error(); err != nil {
				log.Printf("Remove server error:%v", err)
			}
			changed = true
		}
	}
}

// syncLiveness commits a node.up / node.down event when the observed liveness
// of node differs from the replicated one. Comparing against replicated state
// keeps it correct across leader changes.
func (c *Cluster) syncLiveness(node string, up bool) {
	if c.rt.Alive(node) == up {
		return
	}
	name := control.EvNodeDown
	if up {
		name = control.EvNodeUp
	}
	log.Printf("Liveness: %s %s", node, name)
	if err := c.apply(control.Event{Name: name, Params: map[string]any{"node": node}}); err != nil {
		log.Printf("Liveness event error:%v", err)
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

	// 5. The state machine is the control runtime with its registered controllers
	fsm := &ClusterFSM{rt: c.rt}

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
					log.Printf("Activating controllers")
					go c.rt.Activate()
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
	go c.forwardRaised(ctx)
	if err := c.rt.Validate(); err != nil {
		log.Fatalf("invalid controller setup: %v", err)
	}
	go c.serveHTTP(ctx)
	control.NewAgent(nodeID, c.rt, c.Submit, c.workers...).Run(ctx)
	c.bsManager.Start(ctx)

}

// forwardRaised commits the events controllers raised outside the committed
// stream (timers, invokes). Every replica raises them; only the leader's copy
// reaches the log.
func (c *Cluster) forwardRaised(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-c.rt.Outbox():
			if c.r.State() != raft.Leader {
				continue
			}
			if err := c.apply(ev); err != nil {
				log.Printf("Raised event %s/%s not committed: %v", ev.Target, ev.Name, err)
			}
		}
	}
}

var ErrNoLeader = errors.New("no raft leader")

// apply commits ev through the local raft node; it fails unless this node is
// the leader.
func (c *Cluster) apply(ev control.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return c.r.Apply(payload, applyTimeout).Error()
}

// Submit commits ev from any node: directly when this node is the leader,
// otherwise by handing it to the leader over HTTP.
func (c *Cluster) Submit(ctx context.Context, ev control.Event) error {
	if c.r.State() == raft.Leader {
		return c.apply(ev)
	}
	_, leader := c.r.LeaderWithID()
	node, ok := c.nodesMap[string(leader)]
	if !ok {
		return ErrNoLeader
	}
	return postEvent(ctx, node.HTTPAddr, ev)
}
