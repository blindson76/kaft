package timectl

import (
	"encoding/json"
	"slices"
	"testing"

	"example.com/dstate/internal/control"
)

var topo = Topology{
	Addrs:    map[string]string{"n1": "h1", "n2": "h2", "n3": "h3"},
	External: []string{"ext1", "ext2"},
}

// harness drives the real controller through the runtime, playing the nodes.
type harness struct {
	t       *testing.T
	rt      *control.Runtime
	c       control.Controller
	reports map[string]Report
}

func newHarness(t *testing.T, reports map[string]Report) *harness {
	h := &harness{t: t, rt: control.NewRuntime(), c: NewController(topo), reports: map[string]Report{}}
	h.rt.Register(h.c)
	for _, node := range []string{"n1", "n2", "n3"} {
		if r, ok := reports[node]; ok {
			h.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": node}})
			h.report(node, r)
		}
	}
	return h
}

func (h *harness) send(name string, params map[string]any) {
	h.rt.Apply(control.Event{Target: Name, Name: name, Params: params})
}

func (h *harness) report(node string, r Report) {
	h.reports[node] = r
	h.send(control.EvReport, map[string]any{"node": node, "report": r})
}

func (h *harness) down(node string) {
	h.rt.Apply(control.Event{Name: control.EvNodeDown, Params: map[string]any{"node": node}})
}

func (h *harness) state(want string) {
	h.t.Helper()
	if got := h.c.States(); !slices.Equal(got, []string{want}) {
		h.t.Fatalf("state %v, want %s", got, want)
	}
}

// pending returns the configuration node has been ordered to apply, if any.
func (h *harness) pending(node string) (Config, uint64, bool) {
	orders := h.rt.PendingOrders(Name, node)
	if len(orders) == 0 {
		return Config{}, 0, false
	}
	if len(orders) > 1 {
		h.t.Fatalf("%d orders pending for %s, want at most 1", len(orders), node)
	}
	var c Config
	raw, _ := json.Marshal(orders[0].Args)
	if err := json.Unmarshal(raw, &c); err != nil {
		h.t.Fatal(err)
	}
	return c, orders[0].ID, true
}

// apply makes the nodes carry out their pending orders the way a worker
// would, and returns what each was told.
func (h *harness) apply(nodes ...string) map[string]Config {
	h.t.Helper()
	out := map[string]Config{}
	for _, node := range nodes {
		c, id, ok := h.pending(node)
		if !ok {
			h.t.Fatalf("no order pending for %s", node)
		}
		out[node] = c
		r := h.reports[node]
		r.State = State{Epoch: c.Epoch, Mode: c.Persist, DeltaMS: c.DeltaMS, Master: c.Master}
		r.NTPD.Running = true
		h.report(node, r)
		h.rt.Apply(control.Event{Name: control.EvOrderDone, Params: map[string]any{"id": id, "node": node}})
	}
	return out
}

func stored(epoch uint64, mode string, delta int64) Report {
	return Report{State: State{Epoch: epoch, Mode: mode, DeltaMS: delta}}
}

func TestAutoWhenAnyNodeReachesAnExternalSource(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {}, "n2": {External: []string{"ext2"}}, "n3": {}})
	h.state("configuring")

	got := h.apply("n1", "n2", "n3")
	h.state(ModeAuto)
	// Only the source some node reaches is configured.
	want := Config{Epoch: 1, Mode: ModeAuto, Persist: ModeAuto, Servers: []string{"ext2"}, Peers: []string{"h2", "h3"}}
	if c := got["n1"]; !slices.Equal(c.Servers, want.Servers) || !slices.Equal(c.Peers, want.Peers) ||
		c.Epoch != want.Epoch || c.Mode != want.Mode || c.Persist != want.Persist || c.Local {
		t.Fatalf("n1 told %+v, want %+v", c, want)
	}

	// The source disappears: orphan now, internal at the next bootstrap.
	r := h.reports["n2"]
	r.External = nil
	h.report("n2", r)
	h.state("configuring")
	got = h.apply("n1", "n2", "n3")
	h.state(ModeOrphan)
	if c := got["n3"]; c.Mode != ModeOrphan || c.Persist != ModeInternal || len(c.Servers) != 0 ||
		!slices.Equal(c.Peers, []string{"h1", "h2"}) || c.Epoch != 2 {
		t.Fatalf("n3 told %+v, want orphan with peers only, storing internal", c)
	}

	// The source coming back changes nothing by itself...
	r.External = []string{"ext2"}
	h.report("n2", r)
	h.state(ModeOrphan)
	// ...an operator has to ask for it.
	h.send(EvUseAuto, nil)
	if c := h.apply("n1", "n2", "n3")["n1"]; c.Mode != ModeAuto || c.Persist != ModeAuto {
		t.Fatalf("n1 told %+v, want auto", c)
	}
	h.state(ModeAuto)
}

func TestJumpingSourceLeadsToOrphan(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {External: []string{"ext1"}}, "n2": {External: []string{"ext1"}}})
	h.apply("n1", "n2")
	h.state(ModeAuto)

	r := h.reports["n1"]
	r.External, r.Faults = nil, map[string]string{"ext1": "jumped: moved 3s within 10s"}
	h.report("n1", r)
	h.state("configuring")
	if c := h.apply("n1", "n2")["n2"]; c.Mode != ModeOrphan || c.Persist != ModeInternal || len(c.Servers) != 0 {
		t.Fatalf("n2 told %+v, want orphan", c)
	}
	h.state(ModeOrphan)
}

func TestInternalLastStateWins(t *testing.T) {
	// n3 was down when the mode changed. The external source is back, but
	// the system stays internal.
	h := newHarness(t, map[string]Report{
		"n1": {State: State{Epoch: 4, Mode: ModeInternal}, External: []string{"ext1"}},
		"n2": stored(4, ModeInternal, 0),
		"n3": stored(3, ModeAuto, 0),
	})
	got := h.apply("n1", "n2", "n3")
	h.state(ModeInternal)

	if c := got["n1"]; !c.Local || len(c.Servers) != 0 || c.Epoch != 5 || c.Master != "n1" {
		t.Fatalf("n1 told %+v, want master at epoch 5", c)
	}
	for _, node := range []string{"n2", "n3"} {
		if c := got[node]; c.Local || !slices.Equal(c.Servers, []string{"h1"}) || c.DeltaMS != 0 {
			t.Fatalf("%s told %+v, want to follow h1", node, c)
		}
	}
}

func TestMasterIsANodeWithTheLatestState(t *testing.T) {
	h := newHarness(t, map[string]Report{
		"n1": stored(3, ModeAuto, 0),
		"n2": stored(4, ModeInternal, 0),
		"n3": stored(4, ModeInternal, 0),
	})
	if c := h.apply("n1", "n2", "n3")["n1"]; c.Master != "n2" {
		t.Fatalf("master %s, want n2", c.Master)
	}
}

func TestNoSourceMeansInternalAfterTheTimeout(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {}, "n2": {}, "n3": {}})
	h.state("collecting")
	h.send("collect.timeout", nil)
	if c := h.apply("n1", "n2", "n3")["n3"]; c.Mode != ModeInternal || c.Master != "n1" || c.Persist != ModeInternal {
		t.Fatalf("n3 told %+v, want internal under n1", c)
	}
	h.state(ModeInternal)
}

func TestOperatorSetsTimeInInternalMode(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": stored(1, ModeInternal, 0), "n2": stored(1, ModeInternal, 0)})
	h.apply("n1", "n2")
	h.state(ModeInternal)

	// It is 12:00:10 by the clock; the operator says it is 12:00:00.
	h.send(EvSetTime, map[string]any{"now_ms": float64(1_700_000_010_000), "time_ms": float64(1_700_000_000_000)})
	h.state("configuring")
	for node, c := range h.apply("n1", "n2") {
		if c.DeltaMS != 10_000 || c.Mode != ModeInternal {
			t.Fatalf("%s told %+v, want a 10s correction", node, c)
		}
	}
	h.state(ModeInternal)

	// The same through the generic event API, which gives the time as text.
	h.send(EvSetTime, map[string]any{"now_ms": float64(1_700_000_000_500), "time": "2023-11-14T22:13:20Z"})
	for node, c := range h.apply("n1", "n2") {
		if c.DeltaMS != 500 {
			t.Fatalf("%s told %+v, want a 500ms correction", node, c)
		}
	}

	// A correction is handed out once: the next configuration has none.
	h.down("n1")
	if c := h.apply("n2")["n2"]; c.DeltaMS != 0 || !c.Local {
		t.Fatalf("n2 told %+v, want master without a correction", c)
	}
}

func TestSetTimeWithoutATimeIsIgnored(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": stored(1, ModeInternal, 0)})
	h.apply("n1")
	h.send(EvSetTime, map[string]any{"now_ms": 2000.0, "time": "yesterday"})
	h.send(EvSetTime, map[string]any{"time_ms": 1000.0})
	h.state(ModeInternal)
	if _, _, ok := h.pending("n1"); ok {
		t.Fatal("an order was placed for an invalid manual time")
	}
}

func TestSetTimeIsIgnoredOutsideInternalMode(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {External: []string{"ext1"}}})
	h.apply("n1")
	h.send(EvSetTime, map[string]any{"now_ms": 2000.0, "time_ms": 1000.0})
	h.state(ModeAuto)
	if _, _, ok := h.pending("n1"); ok {
		t.Fatal("an order was placed for a manual time in auto mode")
	}
}

func TestLostMasterIsReplaced(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {}, "n2": {}, "n3": {}})
	h.send("collect.timeout", nil)
	h.apply("n1", "n2", "n3")

	h.down("n1")
	h.state("configuring")
	got := h.apply("n2", "n3")
	h.state(ModeInternal)
	if !got["n2"].Local || !slices.Equal(got["n3"].Servers, []string{"h2"}) {
		t.Fatalf("after losing n1: n2 %+v, n3 %+v; want n2 as master", got["n2"], got["n3"])
	}
}

func TestRestartedAndFailedNodesAreConfiguredAgain(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {External: []string{"ext1"}}, "n2": {}})
	h.apply("n1", "n2")
	h.state(ModeAuto)

	// n2 comes back with a state file from before.
	h.report("n2", stored(0, "", 0))
	c, id, ok := h.pending("n2")
	if !ok || c.Epoch != 1 {
		t.Fatalf("restarted node not given the current configuration: %+v", c)
	}
	h.state(ModeAuto)

	// The order fails: no new order until the retry.
	h.rt.Apply(control.Event{Name: control.EvOrderFailed, Params: map[string]any{"id": id, "node": "n2", "error": "access denied"}})
	if _, _, ok := h.pending("n2"); ok {
		t.Fatal("failed order placed again at once")
	}
	h.send("retry", nil)
	if _, _, ok := h.pending("n2"); !ok {
		t.Fatal("failed node not ordered again after the retry interval")
	}
}

func TestNodeAheadOfTheControllerIsAdopted(t *testing.T) {
	h := newHarness(t, map[string]Report{"n1": {External: []string{"ext1"}}, "n2": {}})
	h.apply("n1", "n2")

	// n3 was down at bootstrap and knows a later epoch.
	h.rt.Apply(control.Event{Name: control.EvNodeUp, Params: map[string]any{"node": "n3"}})
	h.report("n3", stored(7, ModeAuto, 0))
	for node, c := range h.apply("n1", "n2", "n3") {
		if c.Epoch != 8 || c.Mode != ModeAuto {
			t.Fatalf("%s told %+v, want auto at epoch 8", node, c)
		}
	}
	h.state(ModeAuto)
}

func TestFailedSourcesAreRemovedAndPutBack(t *testing.T) {
	both := []string{"ext1", "ext2"}
	h := newHarness(t, map[string]Report{"n1": {External: both}, "n2": {External: both}})
	if c := h.apply("n1", "n2")["n1"]; !slices.Equal(c.Servers, both) {
		t.Fatalf("n1 told %+v, want both sources", c)
	}
	h.state(ModeAuto)

	// ext2 jumps, as n2 sees it: no node is to follow it.
	r2 := h.reports["n2"]
	r2.External, r2.Faults = []string{"ext1"}, map[string]string{"ext2": "jumped"}
	h.report("n2", r2)
	h.state("configuring")
	for node, c := range h.apply("n1", "n2") {
		if !slices.Equal(c.Servers, []string{"ext1"}) || c.Mode != ModeAuto || c.Epoch != 2 {
			t.Fatalf("%s told %+v, want ext1 only at epoch 2", node, c)
		}
	}
	h.state(ModeAuto)

	// n1 loses ext1, but n2 still reaches it: nothing changes.
	r1 := h.reports["n1"]
	r1.External = []string{"ext2"}
	h.report("n1", r1)
	h.state(ModeAuto)
	if _, _, ok := h.pending("n1"); ok {
		t.Fatal("reconfigured although the source still works for a node")
	}

	// The hold-down of ext2 is over: it is put back.
	r2 = h.reports["n2"]
	r2.External, r2.Faults = both, nil
	h.report("n2", r2)
	if c := h.apply("n1", "n2")["n2"]; !slices.Equal(c.Servers, both) || c.Epoch != 3 {
		t.Fatalf("n2 told %+v, want both sources back at epoch 3", c)
	}

	// Every source fails: orphan.
	for _, node := range []string{"n1", "n2"} {
		r := h.reports[node]
		r.External = nil
		h.report(node, r)
	}
	if c := h.apply("n1", "n2")["n1"]; c.Mode != ModeOrphan || len(c.Servers) != 0 {
		t.Fatalf("n1 told %+v, want orphan", c)
	}
	h.state(ModeOrphan)
}

func internalUnderN1(t *testing.T) *harness {
	h := newHarness(t, map[string]Report{"n1": {}, "n2": {}, "n3": {}})
	h.send("collect.timeout", nil)
	h.apply("n1", "n2", "n3")
	h.state(ModeInternal)
	return h
}

func TestMasterWhoseNtpdStoppedIsReplaced(t *testing.T) {
	h := internalUnderN1(t)

	r := h.reports["n1"]
	r.NTPD = NTPDStatus{Error: "ntpd exit status 1"}
	h.report("n1", r)
	h.state("configuring")
	got := h.apply("n1", "n2", "n3")
	if !got["n2"].Local || got["n1"].Local || !slices.Equal(got["n1"].Servers, []string{"h2"}) ||
		!slices.Equal(got["n3"].Servers, []string{"h2"}) {
		t.Fatalf("got %+v, want n2 as master and n1 following it", got)
	}
	h.state(ModeInternal)
}

func TestMasterLostByAllFollowersIsReplaced(t *testing.T) {
	h := internalUnderN1(t)
	lost := NTPDStatus{Running: true, Peers: []Peer{{Addr: "h1", State: PeerUnreachable}}}

	// One follower losing it is not enough: that may be its own problem.
	r := h.reports["n3"]
	r.NTPD = NTPDStatus{Running: true, Peers: []Peer{{Addr: "h1", State: PeerSync}}}
	h.report("n3", r)
	r = h.reports["n2"]
	r.NTPD = lost
	h.report("n2", r)
	h.state(ModeInternal)

	r = h.reports["n3"]
	r.NTPD = lost
	h.report("n3", r)
	h.state("configuring")
	got := h.apply("n1", "n2", "n3")
	if !got["n2"].Local || !slices.Equal(got["n1"].Servers, []string{"h2"}) {
		t.Fatalf("got %+v, want n2 as master", got)
	}
}
