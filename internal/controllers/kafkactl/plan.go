// Package kafkactl orchestrates a Kafka cluster in KRaft mode with a dynamic
// controller quorum: the Worker runs the node-local Kafka process, the
// controller decides which nodes form the cluster and replaces failed ones.
//
// Every node starts empty: the probe deletes the Kafka log directory and
// formats it. So there is no data to compare when choosing nodes, and a node
// that restarted is, for Kafka, a new node.
package kafkactl

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"example.com/dstate/internal/control"
)

// Name is the controller / worker name.
const Name = "kafka"

// Order kinds handled by the Worker.
const (
	// OrderStart starts Kafka. With args "bootstrap": true the node is
	// formatted again as a single-node quorum first, which creates the cluster.
	OrderStart = "kafka.start"
	// OrderStop stops Kafka and probes again: wipe and format.
	OrderStop = "kafka.stop"
	// OrderAddVoter makes this running node a voter of the controller quorum.
	// Args: "controllers" (addresses to reach the quorum through).
	OrderAddVoter = "kafka.addvoter"
	// OrderRemoveVoter removes voters from the quorum. Runs on any healthy
	// member. Args: "voters" ([{id, dir}]), "controllers".
	OrderRemoveVoter = "kafka.removevoter"
	// OrderReassign moves all partitions onto the given brokers. Runs on any
	// healthy member. Args: "brokers" (comma-separated broker IDs).
	OrderReassign = "kafka.reassign"
)

// Probe states.
const (
	// StateReady: the log directory is empty and formatted.
	StateReady = "ready"
	// StateBroken: the directory could not be prepared.
	StateBroken = "broken"
)

// Report is the worker's report to the controller.
type Report struct {
	// ProbeID is unique per probe, so every probe produces a new report.
	ProbeID string `json:"probe_id"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	// DirID is the directory ID Kafka generated when the log directory was
	// formatted. Together with the node ID it identifies a quorum voter, and
	// it changes with every format: a wiped node is a new voter.
	DirID string `json:"dir_id,omitempty"`
	// Running is true while the Kafka process is up and serving.
	Running bool `json:"running"`
}

// Plan is a queue of steps towards a cluster made of Members. Each step is a
// set of orders that must all succeed before the next one starts.
type Plan struct {
	Members []string              `json:"members,omitempty"`
	Steps   [][]control.OrderSpec `json:"steps,omitempty"`
	// Current is the step in progress.
	Current []control.OrderSpec `json:"current,omitempty"`
}

// Data is the controller's replicated data.
type Data struct {
	Alive   map[string]bool   `json:"alive"`
	Reports map[string]Report `json:"reports"`
	// Members is the established cluster, as node IDs. Empty until the first
	// formation completes.
	Members []string `json:"members,omitempty"`
	// Voters holds the directory ID each member joined the quorum with. It is
	// what a remove needs once the node itself is gone or reformatted.
	Voters map[string]string `json:"voters,omitempty"`
	Plan   Plan              `json:"plan"`
	// Await holds the orders ("kind@node") the current step waits for.
	Await  map[string]bool `json:"await,omitempty"`
	Failed bool            `json:"failed,omitempty"`
}

func (d Data) Clone() Data {
	d.Alive = maps.Clone(d.Alive)
	d.Reports = maps.Clone(d.Reports)
	d.Members = slices.Clone(d.Members)
	d.Voters = maps.Clone(d.Voters)
	d.Await = maps.Clone(d.Await)
	d.Plan.Members = slices.Clone(d.Plan.Members)
	d.Plan.Steps = slices.Clone(d.Plan.Steps)
	d.Plan.Current = slices.Clone(d.Plan.Current)
	return d
}

// Topology is the static part of the picture: identical on every node.
type Topology struct {
	// Size is how many nodes the cluster should have.
	Size int
	// IDs maps node ID to its Kafka node.id.
	IDs map[string]int
	// Controllers maps node ID to its controller listener host:port.
	Controllers map[string]string
}

func (t Topology) controllers(nodes []string) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, t.Controllers[n])
	}
	return out
}

func (t Topology) brokers(nodes []string) string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, strconv.Itoa(t.IDs[n]))
	}
	return strings.Join(ids, ",")
}

func majority(n int) int { return n/2 + 1 }

func one(node, kind string, args map[string]any) []control.OrderSpec {
	return []control.OrderSpec{{Node: node, Kind: kind, Args: args}}
}

// usable reports whether node can be given work: alive, with a good probe.
func (d Data) usable(node string) bool {
	r := d.Reports[node]
	return d.Alive[node] && r.ProbeID != "" && r.State == StateReady
}

// candidates returns the usable nodes, sorted by ID.
func (d Data) candidates() []string {
	var out []string
	for node := range d.Reports {
		if d.usable(node) {
			out = append(out, node)
		}
	}
	slices.Sort(out)
	return out
}

// allReported reports whether every alive node has a probe report.
func (d Data) allReported() bool {
	for node, alive := range d.Alive {
		if alive && d.Reports[node].ProbeID == "" {
			return false
		}
	}
	return true
}

// formPlan creates the cluster. All nodes are empty, so there is nothing to
// compare: the healthy nodes with the lowest IDs are taken. The first one
// creates the cluster as a single-node quorum; the others start as observers
// and are added to the quorum one at a time.
func formPlan(d Data, t Topology) (Plan, bool) {
	nodes := d.candidates()
	if t.Size < 1 || len(nodes) < t.Size {
		return Plan{}, false
	}
	p := Plan{Members: nodes[:t.Size]}
	first, rest := p.Members[0], p.Members[1:]

	p.Steps = append(p.Steps, one(first, OrderStart, map[string]any{"bootstrap": true}))
	if len(rest) > 0 {
		var starts []control.OrderSpec
		for _, n := range rest {
			starts = append(starts, control.OrderSpec{Node: n, Kind: OrderStart})
		}
		p.Steps = append(p.Steps, starts)
	}
	for i, n := range rest {
		// Reach the quorum through the members that are already voters.
		via := t.controllers(p.Members[:i+1])
		p.Steps = append(p.Steps, one(n, OrderAddVoter, map[string]any{"controllers": via}))
	}
	return p, true
}

// healthy returns the members that are alive with Kafka running.
func (d Data) healthy() []string {
	var out []string
	for _, n := range d.Members {
		if d.Alive[n] && d.Reports[n].Running {
			out = append(out, n)
		}
	}
	return out
}

// quorumLost reports whether too few members are healthy for the controller
// quorum to have a leader. Nothing can be changed through it then; the cluster
// is created again from scratch.
func (d Data) quorumLost() bool {
	return len(d.Members) > 0 && len(d.healthy()) < majority(len(d.Members))
}

// repairPlan brings a running cluster back to full strength. For every member
// that is not healthy it picks one of:
//
//   - restart: Kafka stopped but the node still has the directory it joined
//     the quorum with. Starting it again is enough.
//   - replace with itself: the node is back but was reformatted, so it is a
//     new voter. The old voter is removed and the node added again.
//   - replace with a spare: the node is gone. The old voter is removed and a
//     spare added; partitions are then moved off the lost broker.
//
// It also stops Kafka on nodes that are running it without being members.
func repairPlan(d Data, t Topology) (Plan, bool) {
	if len(d.Members) == 0 || d.quorumLost() {
		return Plan{}, false
	}
	healthy := d.healthy()

	var spares []string
	var stops []control.OrderSpec
	for _, n := range d.candidates() {
		if slices.Contains(d.Members, n) {
			continue
		}
		if d.Reports[n].Running {
			stops = append(stops, control.OrderSpec{Node: n, Kind: OrderStop})
		} else {
			spares = append(spares, n)
		}
	}

	members := slices.Clone(d.Members)
	var starts []control.OrderSpec
	var removed []map[string]any
	var added []string
	for i, n := range d.Members {
		if slices.Contains(healthy, n) {
			continue
		}
		replacement := ""
		switch {
		case d.usable(n) && d.Reports[n].DirID == d.Voters[n]:
			starts = append(starts, control.OrderSpec{Node: n, Kind: OrderStart})
			continue
		case d.usable(n):
			replacement = n
		case len(spares) > 0:
			replacement, spares = spares[0], spares[1:]
		default:
			continue // nothing to replace it with; stays a dead member
		}
		members[i] = replacement
		starts = append(starts, control.OrderSpec{Node: replacement, Kind: OrderStart})
		removed = append(removed, map[string]any{"id": t.IDs[n], "dir": d.Voters[n]})
		added = append(added, replacement)
	}
	if len(starts) == 0 && len(stops) == 0 {
		return Plan{}, false
	}

	p := Plan{Members: members}
	via := map[string]any{"controllers": t.controllers(healthy)}
	if first := append(stops, starts...); len(first) > 0 {
		p.Steps = append(p.Steps, first)
	}
	if len(removed) > 0 {
		p.Steps = append(p.Steps, one(healthy[0], OrderRemoveVoter, map[string]any{
			"voters": removed, "controllers": via["controllers"],
		}))
	}
	for _, n := range added {
		p.Steps = append(p.Steps, one(n, OrderAddVoter, via))
	}
	if !slices.Equal(members, d.Members) {
		p.Steps = append(p.Steps, one(healthy[0], OrderReassign, map[string]any{"brokers": t.brokers(members)}))
	}
	return p, true
}
