package mongoctl

import (
	"cmp"
	"maps"
	"slices"
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
}

func (d Data) Clone() Data {
	d.Alive = maps.Clone(d.Alive)
	d.Reports = maps.Clone(d.Reports)
	d.Members = slices.Clone(d.Members)
	d.Await = maps.Clone(d.Await)
	d.Plan.Members = slices.Clone(d.Plan.Members)
	d.Plan.Wipe = slices.Clone(d.Plan.Wipe)
	d.Plan.Start = slices.Clone(d.Plan.Start)
	d.Plan.Stop = slices.Clone(d.Plan.Stop)
	return d
}

// Topology is the static part of the picture: identical on every node.
type Topology struct {
	// Size is how many members the replica set should have.
	Size int
	// Addrs maps node ID to its mongod host:port.
	Addrs map[string]string
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
// The replica set whose data is newest wins: its most up-to-date nodes are
// selected first, then fresh nodes, and only then nodes that must be wiped.
func formPlan(d Data, t Topology) (Plan, bool) {
	nodes := d.reported()
	if len(nodes) < t.Size || t.Size < 1 {
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
			if len(p.Members) < t.Size {
				p.Members = append(p.Members, n)
			}
		}
	}
	for _, n := range wipe {
		if len(p.Members) < t.Size {
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

// repairPlan replaces dead members of a running replica set with spare nodes.
// It applies while the set still has a majority, so the change can go through
// the primary.
//
// A spare that is not fresh is wiped. Unlike formation there is no fixed oplog
// position to compare against, because the running set keeps writing.
func repairPlan(d Data, t Topology) (Plan, bool) {
	alive := d.aliveMembers()
	if len(alive) == len(d.Members) || d.quorumLost() {
		return Plan{}, false
	}
	var spares []string
	for _, n := range d.reported() {
		if !slices.Contains(d.Members, n) {
			spares = append(spares, n)
		}
	}
	if len(spares) == 0 {
		return Plan{}, false
	}
	// Fresh spares first: they need no wipe.
	slices.SortStableFunc(spares, func(a, b string) int {
		fa, fb := d.Reports[a].Boot.State == StateFresh, d.Reports[b].Boot.State == StateFresh
		switch {
		case fa && !fb:
			return -1
		case fb && !fa:
			return 1
		}
		return cmp.Compare(a, b)
	})

	p := Plan{Members: alive, Mode: ModeReconfig}
	for _, n := range spares {
		if len(p.Members) >= t.Size {
			break
		}
		p.Members = append(p.Members, n)
		p.Start = append(p.Start, n)
		if d.Reports[n].Boot.State != StateFresh {
			p.Wipe = append(p.Wipe, n)
		}
	}
	for _, n := range d.Members {
		if !slices.Contains(p.Members, n) {
			p.Stop = append(p.Stop, n)
		}
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
