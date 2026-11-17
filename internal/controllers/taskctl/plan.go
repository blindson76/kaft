// Package taskctl runs a generic task: a command kept running as a number of
// instances on a set of worker nodes. One controller per task, named after
// it, decides where each instance runs; the Worker on each node starts and
// stops the processes.
//
// An instance is placed on an eligible worker that is up and reporting,
// preferring the least loaded. When its process fails, it moves to another
// worker (after a short delay, so a command that fails everywhere does not
// spin); when its worker goes down, it moves at once. A worker that comes back
// with instances that have moved elsewhere in the meantime is told to stop
// them.
package taskctl

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"example.com/dstate/internal/control"
)

// Order kinds handled by the Worker. Args: "slot" (the instance number, from
// 0) and "gen" (the placement it belongs to).
const (
	// OrderRun starts the instance's process, unless it already runs that
	// placement. A process of an older placement of the slot is stopped first.
	OrderRun = "task.run"
	// OrderStop stops the instance's process of that placement, or forgets it
	// if it has already exited.
	OrderStop = "task.stop"
)

// Instance states, as the controller sees them.
const (
	// InstPending: not placed. No eligible worker can take it, or it waits
	// for the next retry after a failure.
	InstPending = "pending"
	// InstStarting: placed; the run order is out.
	InstStarting = "starting"
	// InstRunning: its worker reports the process running.
	InstRunning = "running"
)

// Report is a worker's report: the processes it has.
type Report struct {
	// Boot is unique per worker start: a new one means the processes of the
	// previous start are gone.
	Boot      string `json:"boot"`
	Instances []Proc `json:"instances,omitempty"`
}

// Proc is one process of the task on a worker. A process that exited stays
// listed, with why, until the controller has it forgotten (OrderStop).
type Proc struct {
	Slot    int    `json:"slot"`
	Gen     uint64 `json:"gen"`
	PID     int    `json:"pid,omitempty"`
	Running bool   `json:"running"`
	Exit    string `json:"exit,omitempty"`
}

// Instance is one of the task's instances.
type Instance struct {
	// Role and Replica (its number within the role) never change.
	Role    string `json:"role,omitempty"`
	Replica int    `json:"replica,omitempty"`
	// Node runs it; empty while pending. Gen numbers each placement, so that
	// what a worker reports is matched to the placement it belongs to.
	Node  string `json:"node,omitempty"`
	Gen   uint64 `json:"gen,omitempty"`
	State string `json:"state"`
	// Avoid is the node it last failed or was lost on; placing it prefers
	// any other.
	Avoid string `json:"avoid,omitempty"`
	// Retry: it failed, and is placed again at the next retry tick.
	Retry     bool   `json:"retry,omitempty"`
	Failures  int    `json:"failures,omitempty"`
	LastError string `json:"last_error,omitempty"`
	// Vars are the values of the other roles' placements ({node:R},
	// {host:R}) the running process was started with. When they change, it
	// is restarted with the new ones.
	Vars map[string]string `json:"vars,omitempty"`
}

// Data is the controller's replicated data.
type Data struct {
	Alive     map[string]bool   `json:"alive"`
	Reports   map[string]Report `json:"reports"`
	Instances []Instance        `json:"instances"`
	// Gen is the last placement number handed out.
	Gen uint64 `json:"gen"`
	// Pending holds the orders out (orderKey), so none is sent twice.
	Pending map[string]bool `json:"pending,omitempty"`
	// DepsReady is true while the controllers this task depends on are in
	// the states it requires.
	DepsReady bool `json:"deps_ready"`

	// Orders are the orders of the decision just taken, raised with it; Down
	// is the node whose orders are withdrawn as it goes down.
	Orders []control.OrderSpec `json:"orders,omitempty"`
	Down   string              `json:"down,omitempty"`
}

func (d Data) Clone() Data {
	d.Alive = maps.Clone(d.Alive)
	d.Reports = maps.Clone(d.Reports)
	d.Instances = slices.Clone(d.Instances)
	for i := range d.Instances {
		d.Instances[i].Vars = maps.Clone(d.Instances[i].Vars)
	}
	d.Pending = maps.Clone(d.Pending)
	d.Orders = slices.Clone(d.Orders)
	return d
}

// Topology is the static part of the picture: identical on every node.
type Topology struct {
	// Instances is how many instances should run, all alike. Ignored when
	// Slots is given.
	Instances int
	// Slots describes each instance, for a task with roles.
	Slots []Slot
	// MaxPerNode caps the instances one worker runs; 0 means no cap.
	MaxPerNode int
	// Nodes are the eligible workers, in config order: the order breaks ties.
	Nodes []string
	// Hosts maps node ID to the host the node is reached at, for {host:R}.
	Hosts map[string]string
}

// Slot is one instance of a task with roles.
type Slot struct {
	Role    string
	Replica int
	// Refs are the roles whose placement the instance's command line or
	// environment refers to ({node:R}, {host:R}). Each has one instance.
	Refs []string
}

func (t Topology) slots() []Slot {
	if len(t.Slots) > 0 {
		return t.Slots
	}
	return make([]Slot, t.Instances)
}

func newData(t Topology) Data {
	d := Data{Alive: map[string]bool{}, Reports: map[string]Report{}, Pending: map[string]bool{}}
	for _, s := range t.slots() {
		d.Instances = append(d.Instances, Instance{Role: s.Role, Replica: s.Replica, State: InstPending})
	}
	return d
}

// vars returns what the placements of the roles slot refers to make of its
// placeholders, and whether every one of them runs: an instance starts only
// once the instances it refers to run.
func (d Data) vars(t Topology, slot int) (map[string]string, bool) {
	refs := t.slots()[slot].Refs
	if len(refs) == 0 {
		return nil, true
	}
	out := map[string]string{}
	for _, role := range refs {
		i := slices.IndexFunc(d.Instances, func(in Instance) bool { return in.Role == role })
		if i < 0 || d.Instances[i].State != InstRunning {
			return nil, false
		}
		node := d.Instances[i].Node
		out["node:"+role] = node
		out["host:"+role] = cmp.Or(t.Hosts[node], node)
	}
	return out, true
}

// orderKey identifies an order. The node comes last: it is the only part
// that could hold a slash.
func orderKey(kind, node string, slot int, gen uint64) string {
	return fmt.Sprintf("%s/%d/%d/%s", kind, slot, gen, node)
}

// orderOf reads the identity of an order from an order result event's params.
func orderOf(params map[string]any) (kind, node string, slot int, gen uint64) {
	kind, _ = params["kind"].(string)
	node, _ = params["node"].(string)
	args, _ := params["args"].(map[string]any)
	return kind, node, int(number(args["slot"])), uint64(number(args["gen"]))
}

// number reads a numeric argument, whose type depends on whether it went
// through JSON (a snapshot) or not.
func number(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

// healthy reports whether every instance runs.
func (d Data) healthy() bool {
	for _, in := range d.Instances {
		if in.State != InstRunning {
			return false
		}
	}
	return true
}

// allReported reports whether every eligible worker has reported.
func (d Data) allReported(t Topology) bool {
	for _, n := range t.Nodes {
		if _, ok := d.Reports[n]; !ok || !d.Alive[n] {
			return false
		}
	}
	return true
}

// needsRetry reports whether an instance waits for the retry tick.
func (d Data) needsRetry() bool {
	return slices.ContainsFunc(d.Instances, func(in Instance) bool { return in.Retry && in.Node == "" })
}

// unplace takes an instance off its node. A failure also counts against the
// instance and delays placing it again.
func (d *Data) unplace(slot int, why string, failure bool) {
	in := &d.Instances[slot]
	in.Avoid, in.Node, in.Gen, in.State, in.Vars = in.Node, "", 0, InstPending, nil
	in.LastError = why
	if failure {
		in.Failures++
		in.Retry = true
	}
}

// decide is the scheduler. It is pure and deterministic, run on every replica
// after every event: it matches what the workers report against the
// placements, takes instances off failed processes and dead workers, and
// places whatever is not placed. retry is true at the retry tick, which is
// when instances that failed are placed again.
//
// It returns the data with the orders to raise in Orders, notes for the log,
// and whether anything changed. Run again on its own result it changes
// nothing, so it can drive an Always transition.
func decide(d Data, t Topology, retry bool) (Data, []string, bool) {
	d = d.Clone()
	d.Orders = nil
	var notes []string
	changed := false
	order := func(kind, node string, slot int, gen uint64) {
		key := orderKey(kind, node, slot, gen)
		if d.Pending[key] {
			return
		}
		d.Pending[key] = true
		args := map[string]any{"slot": slot, "gen": gen}
		if kind == OrderRun {
			in := d.Instances[slot]
			args["role"], args["replica"] = in.Role, in.Replica
			if len(in.Vars) > 0 {
				args["vars"] = maps.Clone(in.Vars)
			}
		}
		d.Orders = append(d.Orders, control.OrderSpec{Node: node, Kind: kind, Args: args})
		changed = true
	}
	current := func(node string, slot int, gen uint64) bool {
		return slot >= 0 && slot < len(d.Instances) && d.Instances[slot].Node == node && d.Instances[slot].Gen == gen
	}

	// 1. What the workers have.
	for _, node := range t.Nodes {
		r, ok := d.Reports[node]
		if !ok || !d.Alive[node] {
			continue
		}
		for _, p := range r.Instances {
			switch {
			case current(node, p.Slot, p.Gen) && p.Running:
				if d.Instances[p.Slot].State != InstRunning {
					d.Instances[p.Slot].State = InstRunning
					notes = append(notes, fmt.Sprintf("instance %d running on %s (pid %d)", p.Slot, node, p.PID))
					changed = true
				}
			case current(node, p.Slot, p.Gen):
				notes = append(notes, fmt.Sprintf("instance %d failed on %s: %s; moving it", p.Slot, node, p.Exit))
				d.unplace(p.Slot, fmt.Sprintf("failed on %s: %s", node, p.Exit), true)
				changed = true
				order(OrderStop, node, p.Slot, p.Gen) // forget the exited process
			default:
				// An older placement, e.g. on a worker that was cut off while
				// its instance moved elsewhere.
				order(OrderStop, node, p.Slot, p.Gen)
			}
		}
	}

	// 2. Placed instances whose worker is gone or no longer has them.
	for slot, in := range d.Instances {
		if in.Node == "" {
			continue
		}
		r, reported := d.Reports[in.Node]
		switch {
		case !d.Alive[in.Node]:
			notes = append(notes, fmt.Sprintf("instance %d: node %s is down; moving it", slot, in.Node))
			d.unplace(slot, "node "+in.Node+" down", false)
			changed = true
		case d.Pending[orderKey(OrderRun, in.Node, slot, in.Gen)] || !reported:
			// Starting, or the worker restarted and has not reported yet.
		case !slices.ContainsFunc(r.Instances, func(p Proc) bool { return p.Slot == slot && p.Gen == in.Gen }):
			notes = append(notes, fmt.Sprintf("instance %d: no longer on %s (worker restarted); placing it again", slot, in.Node))
			d.unplace(slot, "lost on "+in.Node, false)
			changed = true
		}
	}

	// 3. Restart, where they are, instances whose roles of reference have
	// moved: e.g. a slave whose master now runs elsewhere. While the
	// instance referred to is not running, it is left alone.
	for slot := range d.Instances {
		in := &d.Instances[slot]
		if in.Node == "" || d.Pending[orderKey(OrderRun, in.Node, slot, in.Gen)] {
			continue
		}
		vars, ready := d.vars(t, slot)
		if !ready || maps.Equal(vars, in.Vars) {
			continue
		}
		d.Gen++
		notes = append(notes, fmt.Sprintf("instance %d: restarting on %s with %v", slot, in.Node, vars))
		in.Gen, in.State, in.Vars = d.Gen, InstStarting, vars
		order(OrderRun, in.Node, slot, in.Gen)
		changed = true
	}

	// 4. Place what is not placed.
	load := map[string]int{}
	for _, in := range d.Instances {
		if in.Node != "" {
			load[in.Node]++
		}
	}
	for slot := range d.Instances {
		in := &d.Instances[slot]
		if in.Node != "" || (in.Retry && !retry) {
			continue
		}
		vars, ready := d.vars(t, slot)
		if !ready {
			continue // after the instances it refers to
		}
		node := pick(d, t, load, in.Avoid)
		if node == "" {
			continue
		}
		d.Gen++
		in.Node, in.Gen, in.State, in.Retry, in.Vars = node, d.Gen, InstStarting, false, vars
		load[node]++
		notes = append(notes, fmt.Sprintf("instance %d placed on %s", slot, node))
		order(OrderRun, node, slot, in.Gen)
		changed = true
	}
	return d, notes, changed
}

// pick chooses the worker for an instance: one that is up, has reported and
// is below MaxPerNode; preferring any node but avoid, then the least loaded,
// then config order. "" if none can take it.
func pick(d Data, t Topology, load map[string]int, avoid string) string {
	best, bestIdx := "", -1
	key := func(node string, idx int) []int {
		a := 0
		if node == avoid {
			a = 1
		}
		return []int{a, load[node], idx}
	}
	for i, node := range t.Nodes {
		if _, ok := d.Reports[node]; !ok || !d.Alive[node] {
			continue
		}
		if t.MaxPerNode > 0 && load[node] >= t.MaxPerNode {
			continue
		}
		if best == "" || slices.Compare(key(node, i), key(best, bestIdx)) < 0 {
			best, bestIdx = node, i
		}
	}
	return best
}

// dropOrders forgets the orders out to node, which have been withdrawn.
func (d *Data) dropOrders(node string) {
	maps.DeleteFunc(d.Pending, func(key string, _ bool) bool {
		parts := strings.SplitN(key, "/", 4)
		return len(parts) == 4 && parts[3] == node
	})
}
