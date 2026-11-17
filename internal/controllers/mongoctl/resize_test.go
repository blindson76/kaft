package mongoctl

import (
	"slices"
	"testing"

	"example.com/dstate/internal/control"
)

// elastic is a replica set of 3 to 5 members over six candidates.
var elastic = Topology{Min: 3, Max: 5, Addrs: topo.Addrs}

func running(d Data, members ...string) Data {
	d.Members = members
	for _, n := range members {
		r := d.Reports[n]
		r.Status.Role = RoleSecondary
		d.Reports[n] = r
	}
	return d
}

func TestFormPlanTakesUpToMax(t *testing.T) {
	d := data(map[string]Report{"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh(), "n5": fresh(), "n6": fresh()})
	got, ok := formPlan(d, elastic)
	if !ok || !slices.Equal(got.Members, []string{"n1", "n2", "n3", "n4", "n5"}) {
		t.Fatalf("got %+v ok=%v, want n1..n5", got, ok)
	}

	d = data(map[string]Report{"n1": fresh(), "n2": fresh()})
	if _, ok := formPlan(d, elastic); ok {
		t.Fatal("formed with fewer than Min reports")
	}
	d = data(map[string]Report{"n1": fresh(), "n2": fresh(), "n3": fresh()})
	if got, ok := formPlan(d, elastic); !ok || len(got.Members) != 3 {
		t.Fatalf("got %+v ok=%v, want a 3-member set from Min reports", got, ok)
	}
}

func TestResizePlanGrowsToMax(t *testing.T) {
	d := running(data(map[string]Report{
		"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh(), "n5": broken(), "n6": fresh(),
	}), "n1", "n2", "n3")

	got, ok := resizePlan(d, elastic)
	want := Plan{Members: []string{"n1", "n2", "n3", "n4", "n6"}, Start: []string{"n4", "n6"}, Mode: ModeReconfig}
	if !ok || !samePlan(got, want) {
		t.Fatalf("grow with fresh spares first:\n got %+v\nwant %+v", got, want)
	}

	d = running(d, "n1", "n2", "n3", "n4", "n6")
	if got, ok := resizePlan(d, elastic); ok {
		t.Fatalf("planned %+v at Max members", got)
	}
}

func TestResizePlanRemovesDeadMembersDownToMin(t *testing.T) {
	d := running(data(map[string]Report{
		"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh(), "n5": fresh(),
	}), "n1", "n2", "n3", "n4", "n5")

	// Two of five dead, no spare: removed, 3 remain.
	d.Alive["n4"], d.Alive["n5"] = false, false
	got, ok := resizePlan(d, elastic)
	want := Plan{Members: []string{"n1", "n2", "n3"}, Stop: []string{"n4", "n5"}, Mode: ModeReconfig}
	if !ok || !samePlan(got, want) {
		t.Fatalf("\n got %+v\nwant %+v", got, want)
	}

	// Now at Min: one more dead member stays in the config.
	d = running(d, "n1", "n2", "n3")
	d.Alive["n3"] = false
	if got, ok := resizePlan(d, elastic); ok {
		t.Fatalf("planned %+v although removing would go below Min", got)
	}

	// Four members, two dead.
	d = running(data(map[string]Report{"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh()}), "n1", "n2", "n3", "n4")
	d.Alive["n2"], d.Alive["n4"] = false, false
	got, ok = resizePlan(d, Topology{Min: 3, Max: 5, Addrs: topo.Addrs})
	// Without a majority (2 of 4) nothing can be changed through a primary.
	if ok {
		t.Fatalf("planned %+v without a majority", got)
	}
}

func TestResizePlanKeepsSomeDeadMembersAtMin(t *testing.T) {
	d := running(data(map[string]Report{
		"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh(), "n5": fresh(),
	}), "n1", "n2", "n3", "n4", "n5")
	d.Alive["n3"], d.Alive["n5"] = false, false
	got, ok := resizePlan(d, Topology{Min: 4, Max: 5, Addrs: topo.Addrs})
	want := Plan{Members: []string{"n1", "n2", "n4", "n3"}, Stop: []string{"n5"}, Mode: ModeReconfig}
	if !ok || !samePlan(got, want) {
		t.Fatalf("\n got %+v\nwant %+v", got, want)
	}
}

func TestRejectedSpareWaitsForANewProbe(t *testing.T) {
	d := running(data(map[string]Report{"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh()}), "n1", "n2", "n3")
	d.Rejected = map[string]string{"n4": "p"}
	if got, ok := resizePlan(d, elastic); ok {
		t.Fatalf("rejected spare used again: %+v", got)
	}
	r := d.Reports["n4"]
	r.Boot.ProbeID = "p2"
	d.Reports["n4"] = r
	if _, ok := resizePlan(d, elastic); !ok {
		t.Fatal("spare with a new probe not used")
	}
}

// A spare that cannot join is left out instead of being retried in a loop.
func TestFailedGrowthIsNotRetriedAtOnce(t *testing.T) {
	r := &windowRig{t: t, rt: control.NewRuntime(), c: NewController(Topology{Min: 3, Max: 4, Addrs: map[string]string{
		"n1": "h1:1", "n2": "h2:1", "n3": "h3:1", "n4": "h4:1",
	}})}
	r.rt.Register(r.c)
	for _, n := range []string{"n1", "n2", "n3"} {
		r.join(n, fresh())
	}
	r.state("starting")
	// Pretend the set came up: complete the orders, n1 becomes primary.
	finish := func(kind string) {
		for _, n := range r.ordered(kind) {
			for _, o := range r.rt.PendingOrders(Name, n) {
				r.rt.Apply(control.Event{Name: control.EvOrderDone, Params: map[string]any{"id": o.ID, "node": n}})
			}
		}
	}
	finish(OrderStart)
	r.state("configuring")
	finish(OrderInitiate)
	r.state("monitoring")

	// n4 shows up: the set grows into it.
	primary := fresh()
	primary.Status.Role = RolePrimary
	r.join("n1", primary)
	r.join("n4", fresh())
	r.state("starting")
	for _, o := range r.rt.PendingOrders(Name, "n4") {
		r.rt.Apply(control.Event{Name: control.EvOrderFailed, Params: map[string]any{"id": o.ID, "node": "n4", "error": "cannot start"}})
	}
	r.state("monitoring")
	if got := r.ordered(OrderStart); len(got) != 0 {
		t.Fatalf("start ordered again at once on %v", got)
	}

	// After the retry delay it is tried again.
	r.rt.Apply(control.Event{Target: Name, Name: "spares.retry"})
	r.state("starting")
	if got := r.ordered(OrderStart); !slices.Equal(got, []string{"n4"}) {
		t.Fatalf("retry started %v, want n4", got)
	}
}
