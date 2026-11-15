// Package timectl keeps the system time of the nodes consistent through the
// node-local ntpd (Meinberg NTP): the Worker inspects the node and rewrites
// the ntpd configuration, the controller decides the time mode.
//
// There are two time modes:
//
//   - auto: an external time source is reachable and every node follows it.
//   - internal: there is none. One node, the master, serves its own clock and
//     the others follow it. An operator can correct the time by hand.
//
// A running auto system that loses its source, or whose source jumps, does not
// change mode. It goes orphan: the nodes stop following the source and keep
// each other in step, and they record "internal" so that the next bootstrap
// comes up in internal mode.
//
// Every decision is a new epoch. Each node stores the epoch it last applied in
// its state file, so after a restart the nodes with the highest epoch are the
// ones that know what was decided last.
package timectl

import (
	"encoding/json"
	"maps"
	"slices"

	"example.com/dstate/internal/control"
)

// Name is the controller / worker name.
const Name = "time"

// OrderConfigure makes a node apply a time configuration. Args: a Config.
const OrderConfigure = "time.configure"

// Time modes.
const (
	ModeAuto     = "auto"
	ModeInternal = "internal"
	// ModeOrphan is never stored on a node: a node in orphan mode stores
	// ModeInternal.
	ModeOrphan = "orphan"
)

// Operator events, committed through the HTTP API.
const (
	// EvSetTime sets the time by hand, in internal mode. Params: "now_ms"
	// (the clock of the node that took the request) and "time_ms" (what the
	// operator says it is), both Unix milliseconds.
	EvSetTime = "time.set"
	// EvUseAuto leaves internal or orphan mode for auto. It is ignored while
	// no node reaches an external source.
	EvUseAuto = "time.auto"
)

// State is what a node keeps in its state file between starts.
type State struct {
	// Epoch is the last configuration the node applied.
	Epoch uint64 `json:"epoch"`
	// Mode is the mode to come up in at the next bootstrap: auto or internal.
	Mode string `json:"mode,omitempty"`
	// DeltaMS is a manual time correction waiting for the next bootstrap, in
	// milliseconds: the clock is to be set back by this much.
	DeltaMS int64  `json:"time_delta_ms"`
	Master  string `json:"master,omitempty"`
	Updated string `json:"updated,omitempty"`
}

// Report is the worker's report to the controller.
type Report struct {
	State State `json:"state"`
	// External lists the external time servers this node reaches.
	External []string `json:"external,omitempty"`
	// Jump is set once an external server's time has moved by more than the
	// allowed amount within the watch window. It stays set until the node is
	// configured again.
	Jump string `json:"jump,omitempty"`
	// Error tells why the state file could not be read; State is then empty.
	Error string `json:"error,omitempty"`
}

// Config is the argument of OrderConfigure: what one node has to apply.
type Config struct {
	Epoch uint64 `json:"epoch"`
	// Mode is how ntpd runs from now on; Persist is the mode the node stores
	// for the next bootstrap.
	Mode    string `json:"mode"`
	Persist string `json:"persist"`
	Master  string `json:"master,omitempty"`
	// Local makes ntpd serve the node's own clock (the internal master).
	Local bool `json:"local,omitempty"`
	// Servers are followed; Peers are the other nodes, kept in step with each
	// other when no server is left.
	Servers []string `json:"servers,omitempty"`
	Peers   []string `json:"peers,omitempty"`
	// DeltaMS is stored for the next bootstrap.
	DeltaMS int64 `json:"delta_ms,omitempty"`
	// ShiftMS sets the clock back now, but only on a node whose state file is
	// still at epoch ShiftFrom, so a repeated order does not shift twice.
	ShiftMS   int64  `json:"shift_ms,omitempty"`
	ShiftFrom uint64 `json:"shift_from,omitempty"`
}

func (c Config) args() map[string]any {
	raw, _ := json.Marshal(c)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

// Topology is the static part of the picture: identical on every node.
type Topology struct {
	// Addrs maps node ID to the address its ntpd is reached at.
	Addrs map[string]string
	// External lists the external time servers.
	External []string
}

// Data is the controller's replicated data.
type Data struct {
	Alive   map[string]bool   `json:"alive"`
	Reports map[string]Report `json:"reports"`
	// Mode is the decided time mode; empty until the first decision.
	Mode  string `json:"mode,omitempty"`
	Epoch uint64 `json:"epoch"`
	// Master is the node serving the time in internal mode.
	Master string `json:"master,omitempty"`
	// Delta is the manual correction the nodes hold for the next bootstrap.
	Delta int64 `json:"delta_ms"`
	// Shift is a stored correction being applied now, to the nodes that
	// reported epoch ShiftFrom.
	Shift     int64  `json:"shift_ms,omitempty"`
	ShiftFrom uint64 `json:"shift_from,omitempty"`
	// Reason tells why the mode was chosen.
	Reason string `json:"reason,omitempty"`
	// Await holds the nodes with a configure order outstanding, Failed the
	// ones whose order failed and waits for a retry.
	Await  map[string]bool `json:"await,omitempty"`
	Failed map[string]bool `json:"failed,omitempty"`
	// Batch is the set of nodes being ordered by the transition in progress.
	Batch []string `json:"batch,omitempty"`
}

func (d Data) Clone() Data {
	d.Alive = maps.Clone(d.Alive)
	d.Reports = maps.Clone(d.Reports)
	d.Await = maps.Clone(d.Await)
	d.Failed = maps.Clone(d.Failed)
	d.Batch = slices.Clone(d.Batch)
	return d
}

// reported returns the alive nodes that have reported, sorted.
func (d Data) reported() []string {
	var out []string
	for node := range d.Reports {
		if d.Alive[node] {
			out = append(out, node)
		}
	}
	slices.Sort(out)
	return out
}

// allReported reports whether every alive node has reported.
func (d Data) allReported() bool {
	for node, alive := range d.Alive {
		if _, ok := d.Reports[node]; alive && !ok {
			return false
		}
	}
	return true
}

// external reports whether some node reaches an external source it trusts.
func (d Data) external() bool {
	for _, node := range d.reported() {
		if r := d.Reports[node]; len(r.External) > 0 && r.Jump == "" {
			return true
		}
	}
	return false
}

// jump returns the first reported jump of an external source, if any.
func (d Data) jump() string {
	for _, node := range d.reported() {
		if j := d.Reports[node].Jump; j != "" {
			return node + ": " + j
		}
	}
	return ""
}

// newest returns the highest epoch reported and the nodes that are at it.
func (d Data) newest() (uint64, []string) {
	var epoch uint64
	var nodes []string
	for _, node := range d.reported() {
		switch e := d.Reports[node].State.Epoch; {
		case e > epoch || nodes == nil:
			epoch, nodes = e, []string{node}
		case e == epoch:
			nodes = append(nodes, node)
		}
	}
	return epoch, nodes
}

// next starts a new epoch: everything in flight belongs to the old one.
func (d Data) next() Data {
	d.Epoch++
	d.Await, d.Failed, d.Batch = nil, nil, nil
	d.Shift, d.ShiftFrom = 0, 0
	return d
}

// decide picks the time mode at bootstrap, from the reports:
//
//   - the nodes last ran in internal mode: internal again, whatever is
//     reachable now. Only an operator leaves internal mode.
//   - otherwise, an external source is reachable: auto.
//   - otherwise nothing is decided yet, to give the source time to show up;
//     with force (the collect timeout) it is internal.
//
// "Last ran" is read from the nodes with the highest epoch; a node that was
// down when the mode changed has an older epoch and is not asked.
func decide(d Data, force bool) (Data, bool) {
	epoch, top := d.newest()
	if len(top) == 0 {
		return d, false
	}
	last := d.Reports[top[0]].State

	d.Epoch = epoch
	d = d.next()
	d.Master, d.Delta = "", 0
	switch {
	case last.Mode == ModeInternal:
		d.Reason = "last state is internal"
	case d.external():
		d.Mode, d.Reason = ModeAuto, "external time source available"
		return d, true
	case force:
		d.Reason = "no external time source"
	default:
		return d, false
	}
	// The master is one of the nodes that know the latest state: their clocks
	// are the ones the correction below was measured against.
	d.Mode, d.Master = ModeInternal, top[0]
	if last.Mode == ModeInternal && last.DeltaMS != 0 {
		d.Shift, d.ShiftFrom = last.DeltaMS, epoch
	}
	return d, true
}

// newMaster picks a replacement for a master that is gone: the lowest node
// already following the current configuration, else the lowest one alive.
func (d Data) newMaster() (string, bool) {
	nodes := d.reported()
	for _, node := range nodes {
		if d.Reports[node].State.Epoch == d.Epoch {
			return node, true
		}
	}
	if len(nodes) > 0 {
		return nodes[0], true
	}
	return "", false
}

// newer reports whether a node is ahead of the controller. That happens when
// it was down at bootstrap and comes back with a state from a later epoch.
func (d Data) newer() bool {
	epoch, _ := d.newest()
	return epoch > d.Epoch
}

// todo returns the nodes that still have to be given the configuration.
func (d Data) todo() []string {
	var out []string
	for _, node := range d.reported() {
		if d.Reports[node].State.Epoch < d.Epoch && !d.Await[node] && !d.Failed[node] {
			out = append(out, node)
		}
	}
	return out
}

// settled reports whether no alive node is still being configured.
func (d Data) settled() bool {
	for node := range d.Await {
		if d.Alive[node] {
			return false
		}
	}
	return len(d.todo()) == 0
}

// config builds the configuration of one node for the current epoch.
func (d Data) config(t Topology, node string) Config {
	c := Config{Epoch: d.Epoch, Mode: d.Mode, Persist: ModeInternal, Master: d.Master, DeltaMS: d.Delta}
	if d.Shift != 0 && d.Reports[node].State.Epoch == d.ShiftFrom {
		c.ShiftMS, c.ShiftFrom = d.Shift, d.ShiftFrom
	}

	if d.Mode == ModeInternal {
		if node == d.Master {
			c.Local = true
		} else {
			c.Servers = []string{t.Addrs[d.Master]}
		}
		return c
	}
	// auto and orphan: all nodes are peers of each other. That is what keeps
	// them together in orphan mode, and in auto mode it carries the time to
	// a node that does not reach the source itself.
	for _, other := range slices.Sorted(maps.Keys(t.Addrs)) {
		if other != node {
			c.Peers = append(c.Peers, t.Addrs[other])
		}
	}
	if d.Mode == ModeAuto {
		c.Persist = ModeAuto
		c.Servers = slices.Clone(t.External)
	}
	return c
}

// orders builds the configure orders for nodes.
func (d Data) orders(t Topology, nodes []string) []control.OrderSpec {
	out := make([]control.OrderSpec, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, control.OrderSpec{Node: node, Kind: OrderConfigure, Args: d.config(t, node).args()})
	}
	return out
}

// num reads a number that may have gone through JSON.
func num(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}
