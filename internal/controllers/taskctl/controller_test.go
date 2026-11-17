package taskctl

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"example.com/dstate/internal/control"
)

const task = "svc"

// rig drives the real controller through the runtime. Each node has a fake
// worker: a process table that orders change, reported the way the agent
// does (the report first, then the order's result).
type rig struct {
	t     *testing.T
	rt    *control.Runtime
	c     control.Controller
	nodes []string
	procs map[string]map[int]Proc
	boots map[string]int
}

func newRig(t *testing.T, instances, maxPerNode int, nodes ...string) *rig {
	topo := Topology{Instances: instances, MaxPerNode: maxPerNode, Nodes: nodes}
	r := &rig{t: t, rt: control.NewRuntime(), c: NewController(task, topo), nodes: nodes,
		procs: map[string]map[int]Proc{}, boots: map[string]int{}}
	r.rt.Register(r.c, nodes...)
	return r
}

func (r *rig) apply(name string, params map[string]any) {
	r.rt.Apply(control.Event{Target: task, Name: name, Params: params})
}

func (r *rig) report(node string) {
	rep := Report{Boot: string(rune('a' + r.boots[node]))}
	for _, slot := range slices.Sorted(maps.Keys(r.procs[node])) {
		rep.Instances = append(rep.Instances, r.procs[node][slot])
	}
	r.apply(control.EvReport, map[string]any{"node": node, "report": rep})
}

// up brings a node's worker up, empty, as after a (re)start.
func (r *rig) up(node string) {
	r.boots[node]++
	r.procs[node] = map[int]Proc{}
	r.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": node}})
	r.rt.Apply(control.Event{Name: control.EvNodeReady, Params: map[string]any{"node": node, "boot": "b"}})
	r.report(node)
}

func (r *rig) down(node string) {
	r.rt.Apply(control.Event{Name: control.EvNodeDown, Params: map[string]any{"node": node}})
}

// work lets every live node's worker carry out its pending orders.
func (r *rig) work() {
	for range 5 { // orders can lead to more orders
		did := false
		for _, node := range r.nodes {
			if !r.rt.Alive(node) {
				continue
			}
			for _, o := range r.rt.PendingOrders(task, node) {
				slot, gen := int(number(o.Args["slot"])), uint64(number(o.Args["gen"]))
				switch o.Kind {
				case OrderRun:
					if p, ok := r.procs[node][slot]; !ok || p.Gen != gen {
						r.procs[node][slot] = Proc{Slot: slot, Gen: gen, PID: 100 + slot, Running: true}
					}
				case OrderStop:
					if p, ok := r.procs[node][slot]; ok && p.Gen == gen {
						delete(r.procs[node], slot)
					}
				}
				r.report(node)
				r.rt.Apply(control.Event{Name: control.EvOrderDone, Params: map[string]any{"id": o.ID, "node": node}})
				did = true
			}
		}
		if !did {
			return
		}
	}
}

// crash makes the process of slot on node exit.
func (r *rig) crash(node string, slot int) {
	p := r.procs[node][slot]
	p.Running, p.Exit = false, "exited with code 1"
	r.procs[node][slot] = p
	r.report(node)
}

func (r *rig) retry() { r.apply("retry", nil) }

// settle delivers the end of the first placement's wait for workers.
func (r *rig) settle() { r.apply("settle", nil) }

func (r *rig) state(want string) {
	r.t.Helper()
	if got := r.c.States(); !slices.Equal(got, []string{want}) {
		r.t.Fatalf("state %v, want %s", got, want)
	}
}

// placement returns, per node, how many instances run there.
func (r *rig) placement() map[string]int {
	out := map[string]int{}
	for node, procs := range r.procs {
		for _, p := range procs {
			if p.Running && r.rt.Alive(node) {
				out[node]++
			}
		}
	}
	return out
}

func (r *rig) want(want map[string]int) {
	r.t.Helper()
	if got := r.placement(); !maps.Equal(got, want) {
		r.t.Fatalf("instances per node %v, want %v", got, want)
	}
}

func TestPlacesAndSpreadsInstances(t *testing.T) {
	r := newRig(t, 4, 0, "n1", "n2", "n3")
	r.up("n1")
	r.up("n2")
	r.up("n3")
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 2, "n2": 1, "n3": 1})
}

func TestStartsWithTheWorkersThatAreUp(t *testing.T) {
	r := newRig(t, 2, 0, "n1", "n2")
	r.up("n1")
	r.work()
	r.state(StateWaiting) // for n2, a while
	r.settle()
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 2})
	// A worker that comes up later does not take over running instances.
	r.up("n2")
	r.work()
	r.want(map[string]int{"n1": 2})
}

func TestMaxPerNodeLeavesInstancesPending(t *testing.T) {
	r := newRig(t, 3, 1, "n1", "n2", "n3")
	r.up("n1")
	r.up("n2")
	r.settle()
	r.work()
	r.state(StateDegraded)
	r.want(map[string]int{"n1": 1, "n2": 1})
	r.up("n3")
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 1, "n2": 1, "n3": 1})
}

func TestNodeDownMovesItsInstancesAtOnce(t *testing.T) {
	r := newRig(t, 3, 0, "n1", "n2", "n3")
	for _, n := range r.nodes {
		r.up(n)
	}
	r.work()
	r.want(map[string]int{"n1": 1, "n2": 1, "n3": 1})

	r.down("n2")
	r.state(StateDegraded)
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 2, "n3": 1})

	// n2 comes back still running the old instance (it was only cut off):
	// it is told to stop it, the moved one stays.
	r.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": "n2"}})
	r.report("n2")
	if got := r.rt.PendingOrders(task, "n2"); len(got) != 1 || got[0].Kind != OrderStop {
		t.Fatalf("orders for n2 %v, want one stop", got)
	}
	r.work()
	r.want(map[string]int{"n1": 2, "n3": 1})
	r.state(StateHealthy)
}

func TestNodeDownWithdrawsItsOrders(t *testing.T) {
	r := newRig(t, 2, 0, "n1", "n2")
	r.up("n1")
	r.up("n2")
	// The run orders are out, not yet carried out, when n2 goes down.
	if len(r.rt.PendingOrders(task, "n2")) != 1 {
		t.Fatal("no run order for n2")
	}
	r.down("n2")
	if got := r.rt.PendingOrders(task, "n2"); len(got) != 0 {
		t.Fatalf("orders for the dead node still pending: %v", got)
	}
	r.work()
	r.want(map[string]int{"n1": 2})
}

func TestFailedProcessMovesToAnotherNodeAfterTheRetryDelay(t *testing.T) {
	r := newRig(t, 2, 0, "n1", "n2", "n3")
	for _, n := range r.nodes {
		r.up(n)
	}
	r.work()
	r.want(map[string]int{"n1": 1, "n2": 1})

	r.crash("n1", 0)
	r.state(StateDegraded)
	// Not placed again before the retry tick: a command that fails
	// everywhere must not spin.
	r.work()
	r.want(map[string]int{"n2": 1})
	if _, ok := r.procs["n1"][0]; ok {
		t.Fatal("the exited process was not forgotten on n1")
	}

	r.retry()
	r.work()
	r.state(StateHealthy)
	// n1 is avoided; n3 is the least loaded of the others.
	r.want(map[string]int{"n2": 1, "n3": 1})
	d := data(t, r)
	if d.Instances[0].Failures != 1 || d.Instances[0].Node != "n3" {
		t.Fatalf("instance 0: %+v", d.Instances[0])
	}
}

func TestFailingOnTheOnlyNodeRestartsThere(t *testing.T) {
	r := newRig(t, 1, 0, "n1")
	r.up("n1")
	r.work()
	r.crash("n1", 0)
	r.retry()
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 1})
}

func TestRunOrderFailureCountsAsFailure(t *testing.T) {
	r := newRig(t, 1, 0, "n1", "n2")
	r.up("n1")
	r.up("n2")
	o := r.rt.PendingOrders(task, "n1")[0]
	r.rt.Apply(control.Event{Name: control.EvOrderFailed, Params: map[string]any{"id": o.ID, "node": "n1", "error": "file not found"}})
	if d := data(t, r); d.Instances[0].Node != "" || !d.Instances[0].Retry {
		t.Fatalf("instance after a failed start: %+v", d.Instances[0])
	}
	r.retry()
	r.work()
	r.want(map[string]int{"n2": 1})
}

func TestRestartedWorkerGetsItsInstanceAgain(t *testing.T) {
	r := newRig(t, 1, 0, "n1")
	r.up("n1")
	r.work()
	r.up("n1") // the worker restarted: its process is gone
	r.state(StateDegraded)
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 1})
}

func TestDecideIsStable(t *testing.T) {
	topo := Topology{Instances: 3, Nodes: []string{"n1", "n2"}}
	d := newData(topo)
	for _, n := range topo.Nodes {
		d.Alive[n] = true
		d.Reports[n] = Report{Boot: "x"}
	}
	d, _, changed := decide(d, topo, false)
	if !changed || len(d.Orders) != 3 {
		t.Fatalf("first decision: changed=%v orders=%v", changed, d.Orders)
	}
	if _, _, changed := decide(d, topo, false); changed {
		t.Fatal("deciding again on its own result changed something")
	}
}

// data reads the controller's replicated data through a snapshot.
func data(t *testing.T, r *rig) Data {
	t.Helper()
	raw, err := r.c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snap struct {
		Data Data `json:"data"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	return snap.Data
}

// A task that depends on another one being healthy does nothing, not even
// probe, until it is.
func TestTaskWaitsForTheTaskItDependsOn(t *testing.T) {
	rt := control.NewRuntime()
	topo := Topology{Instances: 1, Nodes: []string{"n1"}}
	rt.Register(NewController("db", topo), "n1")
	rt.Register(NewController("api", topo, control.Requirement{Controller: "db", States: []string{StateHealthy}}), "n1")
	if err := rt.Validate(); err != nil {
		t.Fatal(err)
	}
	rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": "n1"}})
	rt.Apply(control.Event{Target: "db", Name: control.EvReport, Params: map[string]any{"node": "n1", "report": Report{Boot: "b"}}})
	if rt.Started("api") {
		t.Fatal("api started before db is healthy")
	}
	o := rt.PendingOrders("db", "n1")
	if len(o) != 1 {
		t.Fatalf("db orders %v, want one run", o)
	}
	rt.Apply(control.Event{Target: "db", Name: control.EvReport, Params: map[string]any{"node": "n1",
		"report": Report{Boot: "b", Instances: []Proc{{Slot: 0, Gen: 1, Running: true}}}}})
	rt.Apply(control.Event{Name: control.EvOrderDone, Params: map[string]any{"id": o[0].ID, "node": "n1"}})
	if !rt.Started("api") {
		t.Fatal("api not started once db is healthy")
	}
}

// After a restore from a snapshot the order arguments have been through JSON;
// results must still be matched to them.
func TestOrderResultsAfterSnapshotRestore(t *testing.T) {
	r := newRig(t, 2, 0, "n1", "n2")
	r.up("n1")
	r.up("n2")
	snap, err := r.rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r.rt = control.NewRuntime()
	r.c = NewController(task, Topology{Instances: 2, Nodes: r.nodes})
	r.rt.Register(r.c, r.nodes...)
	if err := r.rt.Restore(snap); err != nil {
		t.Fatal(err)
	}
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 1, "n2": 1})
	if d := data(t, r); len(d.Pending) != 0 {
		t.Fatalf("orders still counted as out: %v", d.Pending)
	}
}

// A master and a slave on different nodes; the slave's command line holds
// the master's host.
func newMasterSlaveRig(t *testing.T) *rig {
	topo := Topology{
		Slots: []Slot{
			{Role: "master"},
			{Role: "slave", Refs: []string{"master"}},
		},
		MaxPerNode: 1,
		Nodes:      []string{"n1", "n2", "n3"},
		Hosts:      map[string]string{"n1": "10.0.0.1", "n2": "10.0.0.2", "n3": "10.0.0.3"},
	}
	r := &rig{t: t, rt: control.NewRuntime(), c: NewController(task, topo), nodes: topo.Nodes,
		procs: map[string]map[int]Proc{}, boots: map[string]int{}}
	r.rt.Register(r.c, topo.Nodes...)
	return r
}

// runOrder returns the pending run order of slot, if any.
func (r *rig) runOrder(slot int) (string, control.Order, bool) {
	for _, n := range r.nodes {
		for _, o := range r.rt.PendingOrders(task, n) {
			if o.Kind == OrderRun && int(number(o.Args["slot"])) == slot {
				return n, o, true
			}
		}
	}
	return "", control.Order{}, false
}

func TestSlaveStartsAfterItsMasterRuns(t *testing.T) {
	r := newMasterSlaveRig(t)
	for _, n := range r.nodes {
		r.up(n)
	}
	node, o, ok := r.runOrder(0)
	if !ok || node != "n1" || o.Args["role"] != "master" {
		t.Fatalf("master run order: %s %+v", node, o)
	}
	if _, _, ok := r.runOrder(1); ok {
		t.Fatal("slave ordered to run before the master runs")
	}
	r.work()
	r.state(StateHealthy)
	r.want(map[string]int{"n1": 1, "n2": 1})
	if d := data(t, r); d.Instances[1].Vars["host:master"] != "10.0.0.1" || d.Instances[1].Vars["node:master"] != "n1" {
		t.Fatalf("slave started with %v", d.Instances[1].Vars)
	}
}

func TestSlaveFollowsAMasterThatMoves(t *testing.T) {
	r := newMasterSlaveRig(t)
	for _, n := range r.nodes {
		r.up(n)
	}
	r.work()
	slaveGen := data(t, r).Instances[1].Gen

	// The master's node dies: the master moves to the only node without an
	// instance; the slave stays where it is, until the master runs again.
	r.down("n1")
	node, o, ok := r.runOrder(0)
	if !ok || node != "n3" {
		t.Fatalf("master moved to %s, want n3", node)
	}
	if _, _, ok := r.runOrder(1); ok {
		t.Fatal("slave restarted before the new master runs")
	}
	_ = o
	r.work()
	r.state(StateHealthy)
	d := data(t, r)
	if d.Instances[1].Node != "n2" || d.Instances[1].Gen == slaveGen {
		t.Fatalf("slave %+v: want restarted on n2", d.Instances[1])
	}
	if d.Instances[1].Vars["host:master"] != "10.0.0.3" {
		t.Fatalf("slave restarted with %v, want the new master's host", d.Instances[1].Vars)
	}
	r.want(map[string]int{"n2": 1, "n3": 1})
}

func TestSlaveFailureLeavesTheMasterAlone(t *testing.T) {
	r := newMasterSlaveRig(t)
	for _, n := range r.nodes {
		r.up(n)
	}
	r.work()
	masterGen := data(t, r).Instances[0].Gen
	r.crash("n2", 1)
	r.retry()
	r.work()
	r.state(StateHealthy)
	d := data(t, r)
	if d.Instances[0].Gen != masterGen || d.Instances[0].Node != "n1" {
		t.Fatalf("master touched by the slave's failure: %+v", d.Instances[0])
	}
	if d.Instances[1].Node != "n3" {
		t.Fatalf("slave on %s, want n3 (not its failed node, not the master's)", d.Instances[1].Node)
	}
}
