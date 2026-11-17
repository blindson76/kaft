package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
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
	nodesMap map[string]NodeConfig
	nodeID   string
	self     NodeConfig
	// nodes are all node IDs, servers the server ones; both sorted.
	nodes     []string
	servers   []string
	expected  int
	bsManager *ClusterBootstrapController
	// r is nil on a worker-only node.
	r       *raft.Raft
	rt      *control.Runtime
	workers []control.Worker
	routes  map[string]http.HandlerFunc
	// leader reaches the raft leader from a worker-only node.
	leader *leaderClient

	// What Start runs, for Shutdown. cancel ends the context everything runs
	// on; wg tracks the cluster's own loops, observing the raft observer and
	// what it starts.
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	observing sync.WaitGroup
	srv       *http.Server
	agent     *control.Agent
	remote    *control.RemoteView
}

func NewCluster(self string, cfg *ClusterConfig) *Cluster {
	nodeCfg, ok := cfg.Node(self)
	if !ok {
		log.Fatalf("node %s is not in the config", self)
	}
	nodes := make([]string, 0)
	servers := make([]string, 0)
	nodesMap := map[string]NodeConfig{}
	for _, node := range cfg.Nodes {
		nodes = append(nodes, node.ID)
		if node.IsServer() {
			servers = append(servers, node.ID)
		}
		nodesMap[node.ID] = node
	}
	slices.Sort(nodes)
	slices.Sort(servers)
	c := &Cluster{
		nodeID:   self,
		self:     nodeCfg,
		nodesMap: nodesMap,
		nodes:    nodes,
		servers:  servers,
		expected: cfg.ExpectedBootstrap,
		rt:       control.NewRuntime(),
	}
	if nodeCfg.IsServer() {
		delayBootstrap := time.Second*3 + time.Duration(slices.Index(servers, c.nodeID)*int(time.Second))
		c.bsManager = NewClusterManager(servers, cfg.ExpectedBootstrap, delayBootstrap, c)
	} else {
		addrs := make([]string, 0, len(servers))
		for _, id := range servers {
			addrs = append(addrs, nodesMap[id].HTTPAddr)
		}
		c.leader = &leaderClient{addrs: addrs}
	}
	return c
}

// IsServer reports whether this node runs raft and the controllers.
func (c *Cluster) IsServer() bool { return c.self.IsServer() }

// RunsWorker reports whether this node runs the worker of controller.
func (c *Cluster) RunsWorker(controller string) bool { return c.self.RunsWorker(controller) }

// Handle adds a route to the node's HTTP endpoint, for a controller's operator
// API. pattern is a net/http pattern such as "POST /time". Call before Start.
func (c *Cluster) Handle(pattern string, handler http.HandlerFunc) {
	if c.routes == nil {
		c.routes = map[string]http.HandlerFunc{}
	}
	c.routes[pattern] = handler
}

// Register adds a controller and this node's worker for it, before Start.
//
// The controller is kept only on a server; every server must register the
// same controllers with the same nodes. nodes are the nodes that run the
// controller's worker: the controller only hears about their liveness.
//
// worker is the local worker, or nil when this node does not run one.
func (c *Cluster) Register(controller control.Controller, nodes []string, worker control.Worker) {
	if c.IsServer() {
		c.rt.Register(controller, nodes...)
	}
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

// reconcileMembership runs on every server but only acts while this node is
// the raft leader. Each tick it compares the raft configuration with what is
// actually reachable: configured servers that are up but not members are added
// as voters, and members unreachable for deadNodeTimeout are removed so the
// quorum shrinks with the cluster. Membership never shrinks below `expected`.
// At most one change is made per tick, since raft applies configuration
// changes one at a time. Worker-only nodes are never raft members.
//
// The same probes feed the control layer: a liveness change of any node,
// server or worker, is committed as a node.up / node.down event. Servers are
// probed on their raft address, worker-only nodes on their HTTP address.
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
			cfg := c.nodesMap[node]
			server := cfg.IsServer()
			addr, probeAddr := cfg.RaftAddr, cfg.RaftAddr
			if !server {
				probeAddr = cfg.HTTPAddr
			}

			if util.ProbeTCP(ctx, probeAddr, probeTimeout) {
				delete(downSince, id)
				c.syncLiveness(node, true)
				if !server || members[id] || changed {
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
			if !server || !members[id] || changed || len(servers) <= c.expected {
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

// Start runs the node in its roles: raft and the controllers on a server, the
// agent with its workers on a worker. It returns once everything is running;
// it all keeps running in the background until ctx ends or Shutdown is
// called. Call Shutdown in any case, also after Start failed: it stops
// whatever had been started.
func (c *Cluster) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	log.Printf("Node %s roles: %s; workers: %s", c.nodeID, strings.Join(c.self.Roles, ", "), strings.Join(c.workerNames(), ", "))
	var view control.View = c.rt
	if c.IsServer() {
		if err := c.rt.Validate(); err != nil {
			return fmt.Errorf("invalid controller setup: %w", err)
		}
		if err := c.startRaft(ctx); err != nil {
			return err
		}
	} else {
		c.remote = control.NewRemoteView(c.nodeID, c.fetchAgentState)
		c.remote.Run(ctx)
		view = c.remote
	}
	if err := c.startHTTP(ctx); err != nil {
		return err
	}
	if len(c.workers) > 0 {
		c.agent = control.NewAgent(c.nodeID, view, c.Submit, c.workers...)
		c.agent.Run(ctx)
	}
	if c.IsServer() {
		c.wg.Go(func() {
			if err := c.bsManager.Start(ctx); err != nil && ctx.Err() == nil {
				log.Printf("Bootstrap error:%v", err)
			}
		})
	}
	return nil
}

// Shutdown stops the node, in an order that leaves the cluster in the best
// shape it can:
//
//  1. A raft leader hands leadership over, so the others have a new leader at
//     once instead of after an election timeout.
//  2. Everything driven by the node's context stops: the agent (an order in
//     progress is interrupted and stays pending, to run again after the
//     restart), the membership and forwarding loops, the web views. The HTTP
//     endpoint stops taking requests.
//  3. The workers close, in reverse registration order, so a service stops
//     before the ones it depends on: each stops its local service cleanly.
//  4. Raft shuts down, then the controllers.
//
// ctx bounds the whole of it; when it ends, the remaining steps are cut short
// (a service that does not stop in time is killed) and the error says what
// was left unfinished.
func (c *Cluster) Shutdown(ctx context.Context) error {
	var errs []error
	fail := func(what string, err error) {
		if err != nil {
			log.Printf("Shutdown: %s: %v", what, err)
			errs = append(errs, fmt.Errorf("%s: %w", what, err))
		}
	}

	if c.r != nil && c.r.State() == raft.Leader {
		c.handOverLeadership(ctx)
	}

	if c.cancel != nil {
		c.cancel()
	}
	if c.srv != nil {
		fail("http", c.srv.Shutdown(ctx))
	}
	fail("background loops", waitFor(ctx, func() error {
		if c.agent != nil {
			c.agent.Wait()
		}
		if c.remote != nil {
			c.remote.Wait()
		}
		c.wg.Wait()
		return nil
	}))

	for _, w := range slices.Backward(c.workers) {
		if closer, ok := w.(control.Closer); ok {
			log.Printf("Shutdown: closing the %s worker", w.Name())
			fail(w.Name()+" worker", closer.Close(ctx))
		}
	}

	if c.r != nil {
		log.Printf("Shutdown: stopping raft")
		fail("raft", waitFor(ctx, func() error { return c.r.Shutdown().Error() }))
		fail("raft observer", waitFor(ctx, func() error {
			c.observing.Wait()
			return nil
		}))
	}
	if c.IsServer() {
		fail("controllers", c.rt.Stop(ctx))
	}
	return errors.Join(errs...)
}

// waitFor runs fn and waits for it until ctx ends. fn keeps running in the
// background after that; it is for steps that cannot be interrupted.
func waitFor(ctx context.Context, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handOverLeadership makes another server the raft leader, the first one in
// the configuration that answers. A member that is down stays in the
// configuration while it holds the quorum size up, so raft's own pick could be
// a dead one. It is best effort: without it the others elect a leader after
// the election timeout.
func (c *Cluster) handOverLeadership(ctx context.Context) {
	cf := c.r.GetConfiguration()
	if cf.Error() != nil {
		return
	}
	for _, s := range cf.Configuration().Servers {
		if string(s.ID) == c.nodeID || !util.ProbeTCP(ctx, string(s.Address), probeTimeout) {
			continue
		}
		log.Printf("Shutdown: handing raft leadership over to %s", s.ID)
		err := waitFor(ctx, func() error { return c.r.LeadershipTransferToServer(s.ID, s.Address).Error() })
		if err == nil {
			return
		}
		log.Printf("Shutdown: leadership transfer to %s: %v", s.ID, err)
	}
	log.Printf("Shutdown: no other server took raft leadership; the others elect one")
}

func (c *Cluster) workerNames() []string {
	names := make([]string, 0, len(c.workers))
	for _, w := range c.workers {
		names = append(names, w.Name())
	}
	if len(names) == 0 {
		return []string{"none"}
	}
	return names
}

// startRaft starts this server's raft node and the leader-side loops, which
// run until ctx ends.
func (c *Cluster) startRaft(ctx context.Context) error {
	nodeID := c.nodeID
	raftAddr := c.nodesMap[nodeID].RaftAddr

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.LogOutput = io.Discard
	// A removed node must keep running so it can be re-added once reachable.
	config.ShutdownOnRemove = false

	config.SnapshotInterval = time.Millisecond * 400
	config.SnapshotThreshold = 5
	config.TrailingLogs = 50

	// Pure in-memory log, stable and snapshot storage (deliberate: see the
	// project notes); a restarted node rejoins and catches up.
	memStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	address, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return fmt.Errorf("raft address %s: %w", raftAddr, err)
	}
	transport, err := raft.NewTCPTransport(raftAddr, address, 3, 10*time.Second, io.Discard)
	if err != nil {
		return fmt.Errorf("raft transport on %s: %w", raftAddr, err)
	}

	// The state machine is the control runtime with its registered controllers.
	fsm := &ClusterFSM{rt: c.rt}
	r, err := raft.NewRaft(config, fsm, memStore, memStore, snapshotStore, transport)
	if err != nil {
		_ = transport.Close()
		return fmt.Errorf("raft: %w", err)
	}
	c.r = r

	eventChan := make(chan raft.Observation, 10)
	observer := raft.NewObserver(eventChan, true, func(o *raft.Observation) bool {
		return true
	})
	r.RegisterObserver(observer)
	c.observing.Go(func() { c.observe(ctx, observer, eventChan) })
	c.wg.Go(func() { c.reconcileMembership(ctx) })
	c.wg.Go(func() { c.forwardRaised(ctx) })
	return nil
}

// observe follows raft's observations until ctx ends. The observer is a
// blocking one, so raft waits for every observation to be read; after ctx ends
// it keeps reading until the observer is deregistered, or raft would hang on
// the next observation, shutdown included.
func (c *Cluster) observe(ctx context.Context, observer *raft.Observer, ch <-chan raft.Observation) {
	for {
		select {
		case <-ctx.Done():
			deregistered := make(chan struct{})
			go func() {
				c.r.DeregisterObserver(observer)
				close(deregistered)
			}()
			for {
				select {
				case <-ch:
				case <-deregistered:
					return
				}
			}
		case obs := <-ch:
			c.bsManager.NotifyBootstraped()
			switch data := obs.Data.(type) {
			case raft.RaftState:
				log.Printf("RaftState:%v", data)
				if data == raft.Leader && ctx.Err() == nil {
					log.Printf("Activating controllers")
					c.observing.Go(c.rt.Activate)
				}
			case raft.LeaderObservation:
				log.Printf("Leader obs:%v", data.Leader)
			}
		}
	}
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
	if c.r == nil {
		return c.leader.call(ctx, func(ctx context.Context, addr string) error {
			return postEvent(ctx, addr, ev)
		})
	}
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
