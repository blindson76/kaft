// Package timectl keeps the system time of the nodes consistent through a
// node-local ntpd that the Worker runs as a standalone process (not as a
// service): the Worker writes the ntpd configuration, supervises the process
// and reports what ntpd makes of its sources; the controller decides the time
// mode and the sources each node follows.
//
// There are two time modes:
//
//   - auto: external time sources are reachable and every node follows them.
//     The controller keeps the list of sources to the ones that work: a source
//     that fails (stops answering, jumps, or is lost or distrusted by ntpd) is
//     removed from the nodes' configuration, and put back once it works again.
//   - internal: there is none. One node, the master, serves its own clock and
//     the others follow it. A master that fails (its node goes down, its ntpd
//     does not run, or every node following it has lost it) is replaced. An
//     operator can correct the time by hand: every node sets its clock by the
//     correction as soon as it learns of it.
//
// A running auto system that has no working source left does not change mode.
// It goes orphan: the nodes keep each other in step, and they record
// "internal" so that the next bootstrap comes up in internal mode.
//
// Every decision is a new epoch. Each node stores the epoch it last applied in
// its state file, so after a restart the nodes with the highest epoch are the
// ones that know what was decided last.
package timectl

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"

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

// Operator events, committed through the HTTP API of any node: POST /event
// with {"target": "time", "name": ..., "params": ...}, or the routes of Routes.
const (
	// EvSetTime sets the time by hand, in internal mode. Params: "now_ms"
	// (the clock of the node that took the request; the HTTP API fills it
	// in) and what the operator says the time is: "time_ms" (Unix
	// milliseconds) or "time" (RFC 3339).
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
	// DeltaMS is a manual time correction not applied yet, in milliseconds:
	// the clock is to be set back by this much. The worker applies it at its
	// next probe and clears it.
	DeltaMS int64 `json:"time_delta_ms"`
	// AppliedDeltaMS and Applied are the last correction applied, and when.
	AppliedDeltaMS int64  `json:"applied_delta_ms,omitempty"`
	Applied        string `json:"applied,omitempty"`
	Master         string `json:"master,omitempty"`
	Updated        string `json:"updated,omitempty"`
}

// Report is the worker's report to the controller.
type Report struct {
	State State `json:"state"`
	// External lists the external time servers this node reaches and that
	// are not held back.
	External []string `json:"external,omitempty"`
	// Faults lists the external servers held back, and why: one whose time
	// moved by more than the allowed amount within the watch window, or one
	// ntpd lost or calls a falseticker. A server is held back for a while
	// after the fault was last seen.
	Faults map[string]string `json:"faults,omitempty"`
	// NTPD is the state of the local ntpd.
	NTPD NTPDStatus `json:"ntpd"`
	// Error tells why the state file could not be read; State is then empty.
	Error string `json:"error,omitempty"`
	// ShiftError tells why the pending correction could not be applied.
	ShiftError string `json:"shift_error,omitempty"`
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
	// DeltaMS is a manual correction: the node stores it and sets its clock
	// back by it at its next probe.
	DeltaMS int64 `json:"delta_ms,omitempty"`
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
	// Sources are the external servers the nodes follow in auto mode.
	Sources []string `json:"sources,omitempty"`
	// Delta is the manual correction handed out in this epoch.
	Delta int64 `json:"delta_ms,omitempty"`
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
	d.Sources = slices.Clone(d.Sources)
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

// usable returns the external servers to follow, sorted: those some alive
// node reaches, less those any alive node holds back. A fault such as a jump
// lies with the server, so one node seeing it is enough.
func (d Data) usable() []string {
	ok, bad := map[string]bool{}, map[string]bool{}
	for _, node := range d.reported() {
		r := d.Reports[node]
		for _, s := range r.External {
			ok[s] = true
		}
		for s := range r.Faults {
			bad[s] = true
		}
	}
	var out []string
	for s := range ok {
		if !bad[s] {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out
}

// external reports whether there is an external server to follow.
func (d Data) external() bool { return len(d.usable()) > 0 }

// sourcesChanged reports whether, in auto mode, the servers to follow are no
// longer the ones the nodes are configured with. An empty list is not a
// change: that is the way to orphan.
func (d Data) sourcesChanged() bool {
	u := d.usable()
	return len(u) > 0 && !slices.Equal(u, d.Sources)
}

// faults describes the servers held back, for the log.
func (d Data) faults() string {
	var out []string
	for _, node := range d.reported() {
		for _, s := range slices.Sorted(maps.Keys(d.Reports[node].Faults)) {
			out = append(out, node+": "+s+" "+d.Reports[node].Faults[s])
		}
	}
	return strings.Join(out, "; ")
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
	d.Delta = 0
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
	d.Master, d.Sources = "", nil
	switch {
	case last.Mode == ModeInternal:
		d.Reason = "last state is internal"
	case d.external():
		d.Mode, d.Sources, d.Reason = ModeAuto, d.usable(), "external time source available"
		return d, true
	case force:
		d.Reason = "no external time source"
	default:
		return d, false
	}
	// The master is one of the nodes that know the latest state.
	d.Mode, d.Master = ModeInternal, top[0]
	return d, true
}

// masterFailed tells why the internal master can no longer serve the time,
// or "" if it can:
//
//   - its node is down;
//   - its ntpd does not run although it applied the current configuration,
//     or it failed to apply it;
//   - every node that follows it under the current configuration and whose
//     ntpd lists it reports it unreachable.
func (d Data) masterFailed(t Topology) string {
	if d.Mode != ModeInternal || d.Master == "" {
		return ""
	}
	if !d.Alive[d.Master] {
		return "master " + d.Master + " is down"
	}
	if d.Failed[d.Master] {
		return "master " + d.Master + " could not be configured"
	}
	if r, ok := d.Reports[d.Master]; ok && r.State.Epoch == d.Epoch && !r.NTPD.Running {
		return "ntpd of master " + d.Master + " is not running"
	}
	followers, lost := 0, 0
	for _, node := range d.reported() {
		r := d.Reports[node]
		if node == d.Master || r.State.Epoch != d.Epoch {
			continue
		}
		if p, ok := r.NTPD.peer(t.Addrs[d.Master]); ok {
			followers++
			if p.State == PeerUnreachable {
				lost++
			}
		}
	}
	if followers > 0 && lost == followers {
		return "master " + d.Master + " unreachable from its followers"
	}
	return ""
}

// healthy reports whether a node can be made master: its ntpd runs, or it
// has not been configured yet, and its last order did not fail.
func (d Data) healthy(node string) bool {
	r := d.Reports[node]
	return !d.Failed[node] && (r.NTPD.Running || r.State.Epoch != d.Epoch)
}

// newMaster picks a replacement for a master that failed: the lowest healthy
// node already following the current configuration, else the lowest healthy
// one alive. The failed master is never picked.
func (d Data) newMaster() (string, bool) {
	var nodes []string
	for _, node := range d.reported() {
		if node != d.Master && d.healthy(node) {
			nodes = append(nodes, node)
		}
	}
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
		c.Servers = slices.Clone(d.Sources)
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
