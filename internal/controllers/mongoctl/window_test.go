package mongoctl

import (
	"slices"
	"testing"
	"time"

	"example.com/dstate/internal/control"
)

// windowRig drives the real controller through the runtime, with four mongo
// candidates for a three-member set.
type windowRig struct {
	t  *testing.T
	rt *control.Runtime
	c  control.Controller
}

func newWindowRig(t *testing.T, window time.Duration) *windowRig {
	topo := Topology{Size: 3, Window: window, Addrs: map[string]string{
		"n1": "h1:1", "n2": "h2:1", "n3": "h3:1", "n4": "h4:1",
	}}
	r := &windowRig{t: t, rt: control.NewRuntime(), c: NewController(topo)}
	r.rt.Register(r.c)
	return r
}

func (r *windowRig) join(node string, rep Report) {
	r.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": node}})
	r.rt.Apply(control.Event{Target: Name, Name: control.EvReport, Params: map[string]any{"node": node, "report": rep}})
}

func (r *windowRig) down(node string) {
	r.rt.Apply(control.Event{Name: control.EvNodeDown, Params: map[string]any{"node": node}})
}

// timer delivers a timer event of the given decision window, as committed
// through the log (numbers arrive as float64).
func (r *windowRig) timer(name string, round int) {
	r.rt.Apply(control.Event{Target: Name, Name: name, Params: map[string]any{"round": float64(round)}})
}

func (r *windowRig) state(want string) {
	r.t.Helper()
	if got := r.c.States(); !slices.Equal(got, []string{want}) {
		r.t.Fatalf("state %v, want %s", got, want)
	}
}

// ordered returns the nodes with a pending order of kind.
func (r *windowRig) ordered(kind string) []string {
	var out []string
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		for _, o := range r.rt.PendingOrders(Name, n) {
			if o.Kind == kind {
				out = append(out, n)
			}
		}
	}
	return out
}

func TestWindowOpensOnlyOnceEnoughNodesReported(t *testing.T) {
	r := newWindowRig(t, 30*time.Second)
	r.join("n1", fresh())
	r.join("n2", fresh())
	r.state("collecting")
	// Nothing runs yet: a timer event cannot close a window.
	r.timer("collect.window", 0)
	r.timer("collect.timeout", 0)
	r.state("collecting")

	r.join("n3", fresh())
	r.state("deciding")
	r.timer("collect.window", 1)
	r.state("starting")
	if got := r.ordered(OrderStart); !slices.Equal(got, []string{"n1", "n2", "n3"}) {
		t.Fatalf("started %v, want n1 n2 n3", got)
	}
}

func TestDecisionWaitsForLateCandidates(t *testing.T) {
	r := newWindowRig(t, 30*time.Second)
	hosts := []string{"h1:1", "h2:1", "h4:1"}
	r.join("n1", member("A", 1, 10, 50, hosts...))
	r.join("n2", member("A", 1, 10, 50, hosts...))
	r.join("n3", fresh())
	// Enough nodes for a set, all up nodes reported: still waiting.
	r.state("deciding")

	// n4 joins late with the newest data of the set: every candidate has
	// reported, so the decision does not wait for the window.
	r.join("n4", member("A", 1, 10, 90, hosts...))
	r.state("starting")
	if got := r.ordered(OrderStart); !slices.Contains(got, "n4") || slices.Contains(got, "n3") {
		t.Fatalf("started %v, want the late node n4 in and the fresh n3 out", got)
	}
}

func TestWindowStillWaitsForNodesThatAreUp(t *testing.T) {
	r := newWindowRig(t, 30*time.Second)
	r.join("n1", fresh())
	r.join("n2", fresh())
	r.join("n3", fresh())
	r.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": "n4"}})
	r.timer("collect.window", 1)
	// n4 is up but has not reported: wait for it, up to collect.timeout.
	r.state("deciding")
	r.timer("collect.timeout", 1)
	r.state("starting")
}

func TestWindowCancelledWhenTooFewNodesLeft(t *testing.T) {
	r := newWindowRig(t, 30*time.Second)
	r.join("n1", fresh())
	r.join("n2", fresh())
	r.join("n3", fresh())
	r.state("deciding")

	r.down("n3")
	r.state("collecting")
	r.join("n3", fresh())
	r.state("deciding")
	// A timer of the first window, committed late, does not close the new one.
	r.timer("collect.window", 1)
	r.state("deciding")
	r.timer("collect.window", 2)
	r.state("starting")
}

func TestNoWindowDecidesAtOnce(t *testing.T) {
	r := newWindowRig(t, 0)
	r.join("n1", fresh())
	r.join("n2", fresh())
	r.state("collecting") // too few
	r.join("n3", fresh())
	r.state("starting")
}

func TestResetOpensANewWindow(t *testing.T) {
	r := newWindowRig(t, 30*time.Second)
	r.join("n1", fresh())
	r.join("n2", fresh())
	r.join("n3", fresh())
	r.timer("collect.window", 1)
	r.state("starting")

	r.rt.Apply(control.Event{Target: Name, Name: "start.timeout"}) // formation fails: reset
	r.state("resetting")
	r.rt.Apply(control.Event{Target: Name, Name: "reset.timeout"})
	r.state("collecting")
	for _, n := range []string{"n1", "n2", "n3"} {
		r.join(n, fresh())
	}
	r.state("deciding")
	r.timer("collect.window", 2)
	r.state("starting")
}
