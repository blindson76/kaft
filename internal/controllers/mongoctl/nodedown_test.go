package mongoctl

import (
	"slices"
	"testing"

	"example.com/dstate/internal/control"
)

// establish forms a replica set of n1..n3 and leaves n4 as a stopped spare.
func establish(t *testing.T) *windowRig {
	r := newWindowRig(t, 0)
	for _, n := range []string{"n1", "n2", "n3"} {
		r.join(n, fresh())
	}
	r.state("starting")
	r.finish()
	r.state("configuring")
	r.finish()
	r.state("monitoring")
	spare := fresh()
	spare.Status.Role = RoleStopped
	r.join("n4", spare)
	r.state("monitoring")
	return r
}

// finish reports every pending order as done.
func (r *windowRig) finish() {
	pending, _ := r.rt.OrderLog()
	for _, o := range pending {
		r.rt.Apply(control.Event{Name: control.EvOrderDone, Params: map[string]any{"id": o.ID, "node": o.Node}})
	}
}

// A member that goes down is replaced, but not ordered to stop: it could not
// carry the order out.
func TestDeadMemberIsNotOrderedToStop(t *testing.T) {
	r := establish(t)
	r.down("n2")
	r.state("starting")
	if got := r.ordered(OrderStart); !slices.Equal(got, []string{"n4"}) {
		t.Fatalf("started %v, want the spare n4", got)
	}
	if got := r.ordered(OrderStop); len(got) > 0 {
		t.Fatalf("stop ordered on %v", got)
	}
}

// A step waiting for a node that goes down fails at once instead of waiting
// for its timeout.
func TestStepFailsWhenItsNodeGoesDown(t *testing.T) {
	r := establish(t)
	r.down("n2")
	r.state("starting")
	r.down("n4") // the spare being started
	r.state("monitoring")
	if pending, _ := r.rt.OrderLog(); len(pending) > 0 {
		t.Fatalf("pending %+v", pending)
	}
}

// A removed member that comes back with mongod running is stopped, once per
// probe.
func TestStrayMongodIsStopped(t *testing.T) {
	r := establish(t)
	r.down("n2")
	r.finish() // n4 started
	r.state("electing")
	primary := fresh()
	primary.Status.Role = RolePrimary
	r.rt.Apply(control.Event{Target: Name, Name: control.EvReport, Params: map[string]any{"node": "n1", "report": primary}})
	r.state("configuring")
	secondary := fresh()
	secondary.Status.Role = RoleSecondary
	r.rt.Apply(control.Event{Target: Name, Name: control.EvReport, Params: map[string]any{"node": "n4", "report": secondary}})
	r.finish()
	r.state("monitoring")

	stray := fresh()
	stray.Status.Role = RoleOther // still running, removed from the set
	r.join("n2", stray)
	r.state("monitoring")
	if got := r.ordered(OrderStop); !slices.Equal(got, []string{"n2"}) {
		t.Fatalf("stop ordered on %v, want n2", got)
	}
	// The same report again does not order a second stop.
	r.rt.Apply(control.Event{Target: Name, Name: control.EvReport, Params: map[string]any{"node": "n2", "report": stray}})
	if pending, _ := r.rt.OrderLog(); len(pending) != 1 {
		t.Fatalf("pending %+v, want one stop", pending)
	}
}
