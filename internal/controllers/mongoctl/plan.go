package mongoctl

import (
	"cmp"
	"maps"
	"slices"
	"time"
)

// Plan modes: how the replica set config is brought to Plan.Members once the
// nodes in Plan.Start are running.
const (
	// ModeInitiate: no replica set exists; Target creates one.
	ModeInitiate = "initiate"
	// ModeAdopt: an existing replica set still has a majority of its
	// configured members among the selected nodes. They elect a primary by
	// themselves, which is then reconfigured to Plan.Members.
	ModeAdopt = "adopt"
	// ModeForce: an existing replica set lost the majority of its configured
	// members. Target, the most up-to-date node, is force-reconfigured.
	ModeForce = "force"
	// ModeReconfig: a running replica set is changed through its primary.
	ModeReconfig = "reconfig"
	// ModeNone: nothing to configure; nodes only need to be started.
	ModeNone = "none"
)

// Plan is a set of steps towards a replica set made of Members. Steps run in
// this order: wipe, start, configure.
type Plan struct {
	// Members is the target member set, as node IDs. For formation plans the
	// most up-to-date node comes first.
	Members []string `json:"members,omitempty"`
	// Wipe lists the nodes whose data must be deleted first.
	Wipe []string `json:"wipe,omitempty"`
	// Start lists the nodes whose mongod must be started.
	Start []string `json:"start,omitempty"`
	// Stop lists the nodes that leave the replica set. Not waited for: such
	// a node is often unreachable.
	Stop []string `json:"stop,omitempty"`
	Mode string   `json:"mode,omitempty"`
	// Target receives the configure order for ModeInitiate and ModeForce.
	Target string `json:"target,omitempty"`
}

// Data is the controller's replicated data.
type Data struct {
	Alive   map[string]bool   `json:"alive"`
	Reports map[string]Report `json:"reports"`
	// Members is the established replica set, as node IDs. Empty until the
	// first formation completes.
	Members []string `json:"members,omitempty"`
	// Plan is the plan being carried out.
	Plan Plan `json:"plan"`
	// Await holds the orders ("kind@node") the current step waits for.
	Await map[string]bool `json:"await,omitempty"`
	// Failed is set when an awaited order failed.
	Failed bool `json:"failed,omitempty"`
	// DepsReady is true while the controllers this one depends on are in the
	// states it requires. It gates forming the cluster, nothing else.
	DepsReady bool `json:"deps_ready"`
	// Round counts the decision windows opened. WindowClosed is set once the
	// current one has passed; Forced once the decision is not to wait for
	// any further report either.
	Round        uint64 `json:"round"`
	WindowClosed bool   `json:"window_closed"`
	Forced       bool   `json:"forced,omitempty"`
	// Rejected holds spare nodes that failed to join the running set, with
	// the probe they failed with. They are not tried again until they report
	// a new probe or the retry delay has passed.
	Rejected map[string]string `json:"rejected,omitempty"`
	// Stopping holds the non-members found running mongod and ordered to
	// stop, with the probe they were found with, so each one is ordered only
	// once per probe. StrayStops are the ones ordered last.
	Stopping   map[string]string `json:"stopping,omitempty"`
	StrayStops []string          `json:"stray_stops,omitempty"`
}

// alive returns the nodes of nodes that are alive, in the same order.
func (d Data) alive(nodes []string) []string {
	var out []string
	for _, n := range nodes {
		if d.Alive[n] {
			out = append(out, n)
		}
	}
	return out
}

// strays returns the alive nodes outside the established replica set whose
// mongod runs, typically a member removed while it was unreachable that has
// come back, except the ones already ordered to stop for their current probe.
// Sorted by ID.
func (d Data) strays() []string {
	if len(d.Members) == 0 {
		return nil
	}
	var out []string
	for _, n := range d.reported() {
		r := d.Reports[n]
		running := r.Status.Role != "" && r.Status.Role != RoleStopped
		if slices.Contains(d.Members, n) || !running || d.Stopping[n] == r.Boot.ProbeID {
			continue
		}
		out = append(out, n)
	}
	return out
}

func (d Data) Clone() Data {
	d.Alive = maps.Clone(d.Alive)
	d.Reports = maps.Clone(d.Reports)
	d.Members = slices.Clone(d.Members)
	d.Await = maps.Clone(d.Await)
	d.Rejected = maps.Clone(d.Rejected)
	d.Stopping = maps.Clone(d.Stopping)
	d.StrayStops = slices.Clone(d.StrayStops)
	d.Plan.Members = slices.Clone(d.Plan.Members)
	d.Plan.Wipe = slices.Clone(d.Plan.Wipe)
	d.Plan.Start = slices.Clone(d.Plan.Start)
	d.Plan.Stop = slices.Clone(d.Plan.Stop)
	return d
}

// Topology is the static part of the picture: identical on every node.
type Topology struct {
	// Min is the smallest replica set: forming one waits for this many
	// reports, and dead members are removed from a running set only as long
	// as at least Min members remain.
	Min int
	// Max is the largest replica set: a running set grows into spare nodes
	// until it has Max members. At most 7, MongoDB's limit of voting members.
	Max int
	// Addrs maps node ID to its mongod host:port. These are the candidates.
	Addrs map[string]string
	// Window is how long forming a replica set waits for candidates that join
	// late, so the decision is made with as many reports as possible. It
	// starts once Min candidates have reported and is cut short once every
	// candidate has. Zero: decide as soon as every node that is up has
	// reported.
	Window time.Duration
}

func (t Topology) nodeOf(host string) string {
	for _, node := range slices.Sorted(maps.Keys(t.Addrs)) {
		if t.Addrs[node] == host {
			return node
		}
	}
	return ""
}

func majority(n int) int { return n/2 + 1 }

// reported returns the alive nodes that have a probe report, sorted by ID.
func (d Data) reported() []string {
	var out []string
	for node, r := range d.Reports {
		if d.Alive[node] && r.Boot.ProbeID != "" {
			out = append(out, node)
		}
	}
	slices.Sort(out)
	return out
}

// allReported reports whether every alive node has a probe report.
func (d Data) allReported() bool {
	for node, alive := range d.Alive {
		if alive && d.Reports[node].Boot.ProbeID == "" {
			return false
		}
	}
	return true
}

// allCandidatesReported reports whether every candidate is up and has
// reported: nobody is left to wait for.
func (d Data) allCandidatesReported(t Topology) bool {
	return len(d.reported()) >= len(t.Addrs)
}

// needsWipe decides, from oplog positions alone, whether a node can join the
// replica set of best as it is. It cannot when it:
//
//   - is broken;
//   - holds the data of a different replica set;
//   - is so far behind that best's oplog no longer contains the entries it
//     is missing (its newest entry is older than best's oldest one).
//
// Otherwise mongod catches up from the oplog by itself, rolling back first if
// the node had diverged. A fresh node never needs a wipe: it has no data.
func needsWipe(node, best Boot) bool {
	switch node.State {
	case StateFresh:
		return false
	case StateMember:
		if node.ReplSetID != best.ReplSetID {
			return true
		}
		return node.LastOp.IsZero() || node.LastOp.CompareTS(best.FirstOp) < 0
	default:
		return true
	}
}

// formPlan decides which nodes form the replica set, from the probe reports of
// the alive nodes. It is a pure function of its arguments.
//
// It needs at least Min reports and takes up to Max members. The replica set
// whose data is newest wins: its most up-to-date nodes are selected first,
// then fresh nodes, and only then nodes that must be wiped.
func formPlan(d Data, t Topology) (Plan, bool) {
	nodes := d.reported()
	if len(nodes) < t.Min || t.Min < 1 {
		return Plan{}, false
	}
	boot := func(n string) Boot { return d.Reports[n].Boot }

	// The most up-to-date replica set member, if any node has data.
	best := ""
	for _, n := range nodes {
		if boot(n).State != StateMember || boot(n).ReplSetID == "" {
			continue
		}
		if best == "" || boot(n).LastOp.Compare(boot(best).LastOp) > 0 {
			best = n // ties keep the lower node ID
		}
	}

	var usable, fresh, wipe []string
	for _, n := range nodes {
		b := boot(n)
		switch {
		case b.State == StateFresh:
			fresh = append(fresh, n)
		case best != "" && !needsWipe(b, boot(best)):
			usable = append(usable, n)
		default:
			wipe = append(wipe, n)
		}
	}
	// Newest data first; equal positions keep node ID order.
	slices.SortStableFunc(usable, func(a, b string) int {
		return -boot(a).LastOp.Compare(boot(b).LastOp)
	})

	p := Plan{}
	for _, group := range [][]string{usable, fresh} {
		for _, n := range group {
			if len(p.Members) < t.Max {
				p.Members = append(p.Members, n)
			}
		}
	}
	for _, n := range wipe {
		if len(p.Members) < t.Max {
			p.Members = append(p.Members, n)
			p.Wipe = append(p.Wipe, n)
		}
	}
	p.Start = slices.Clone(p.Members)

	if best == "" {
		p.Mode, p.Target = ModeInitiate, p.Members[0]
		return p, true
	}

	// Can the existing replica set elect a primary on its own? Only if a
	// majority of the members in its stored config are selected with their
	// data intact.
	kept := 0
	for _, host := range boot(best).Members {
		if n := t.nodeOf(host); slices.Contains(p.Members, n) && slices.Contains(usable, n) {
			kept++
		}
	}
	if kept >= majority(len(boot(best).Members)) {
		p.Mode = ModeAdopt
	} else {
		p.Mode, p.Target = ModeForce, best
	}
	return p, true
}

// aliveMembers returns the established members that are alive.
func (d Data) aliveMembers() []string {
	var out []string
	for _, n := range d.Members {
		if d.Alive[n] {
			out = append(out, n)
		}
	}
	return out
}

// quorumLost reports whether too few members are alive for the replica set to
// have a primary. There is nothing to reconfigure through then; the set is
// formed again from scratch.
func (d Data) quorumLost() bool {
	return len(d.Members) > 0 && len(d.aliveMembers()) < majority(len(d.Members))
}

// primary returns the member that reports itself primary, as node ID.
func (d Data) primary(members []string) string {
	for _, n := range members {
		if d.Alive[n] && d.Reports[n].Status.Role == RolePrimary {
			return n
		}
	}
	return ""
}

// spares returns the alive nodes that have reported and are not members,
// fresh ones first (they need no wipe), then by ID. Rejected nodes are left
// out until they report a new probe.
func (d Data) spares() []string {
	var out []string
	for _, n := range d.reported() {
		if slices.Contains(d.Members, n) {
			continue
		}
		if probe, ok := d.Rejected[n]; ok && probe == d.Reports[n].Boot.ProbeID {
			continue
		}
		out = append(out, n)
	}
	slices.SortStableFunc(out, func(a, b string) int {
		fa, fb := d.Reports[a].Boot.State == StateFresh, d.Reports[b].Boot.State == StateFresh
		switch {
		case fa && !fb:
			return -1
		case fb && !fa:
			return 1
		}
		return cmp.Compare(a, b)
	})
	return out
}

// resizePlan keeps a running replica set between Min and Max members:
//
//   - spare nodes are added while the set has fewer than Max members, which
//     both replaces dead members and grows a healthy set;
//   - dead members are removed, but only as long as at least Min members
//     remain; the ones kept stay in the config and can come back.
//
// Healthy members are never removed. It applies while the set has a majority,
// so the change can go through the primary. A spare that is not fresh is
// wiped: unlike formation there is no fixed oplog position to compare
// against, because the running set keeps writing.
func resizePlan(d Data, t Topology) (Plan, bool) {
	if len(d.Members) == 0 || d.quorumLost() {
		return Plan{}, false
	}
	p := Plan{Members: d.aliveMembers(), Mode: ModeReconfig}
	for _, n := range d.spares() {
		if len(p.Members) >= t.Max {
			break
		}
		p.Members = append(p.Members, n)
		p.Start = append(p.Start, n)
		if d.Reports[n].Boot.State != StateFresh {
			p.Wipe = append(p.Wipe, n)
		}
	}
	for _, n := range d.Members {
		if !d.Alive[n] && len(p.Members) < t.Min {
			p.Members = append(p.Members, n)
		}
	}
	for _, n := range d.Members {
		if !slices.Contains(p.Members, n) {
			p.Stop = append(p.Stop, n)
		}
	}
	if len(p.Start) == 0 && len(p.Stop) == 0 {
		return Plan{}, false
	}
	return p, true
}

// rejoinPlan restarts members whose mongod is not running although the node
// is alive, which is what a restarted node looks like after its probe. A member
// that came back broken is wiped first and syncs from the others.
func rejoinPlan(d Data) (Plan, bool) {
	p := Plan{Members: slices.Clone(d.Members), Mode: ModeNone}
	for _, n := range d.aliveMembers() {
		r := d.Reports[n]
		if r.Boot.ProbeID == "" || r.Status.Role != RoleStopped {
			continue
		}
		p.Start = append(p.Start, n)
		if r.Boot.State == StateBroken {
			p.Wipe = append(p.Wipe, n)
		}
	}
	return p, len(p.Start) > 0
}
