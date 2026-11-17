package control

import (
	"context"
	"slices"
	"testing"
	"time"
)

// nodeRecorder records the node events it receives.
type nodeRecorder struct {
	*fakeController
	nodes []string
}

func (n *nodeRecorder) Deliver(ev Event) {
	switch ev.Name {
	case EvNodeUp, EvNodeDown, EvNodeReady:
		n.nodes = append(n.nodes, ev.Name+":"+ev.Params["node"].(string))
	}
	n.fakeController.Deliver(ev)
}

// A controller only hears about the nodes running its worker, both live and
// when it starts late and is told the liveness it missed.
func TestNodeEventsAreScoped(t *testing.T) {
	scoped := &nodeRecorder{fakeController: fake("scoped")}
	all := &nodeRecorder{fakeController: fake("all")}
	late := &nodeRecorder{fakeController: fake("late", Requirement{Controller: "all", States: []string{"up"}})}

	rt := NewRuntime()
	rt.Register(scoped, "w1")
	rt.Register(all)
	rt.Register(late, "w2")

	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "w1"}})
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "w2"}})
	rt.Apply(Event{Name: EvNodeReady, Params: map[string]any{"node": "w2", "boot": "b"}})
	goTo(rt, "all", "up") // starts "late"

	if want := []string{"node.up:w1"}; !slices.Equal(scoped.nodes, want) {
		t.Errorf("scoped got %v, want %v", scoped.nodes, want)
	}
	if want := []string{"node.up:w1", "node.up:w2", "node.ready:w2"}; !slices.Equal(all.nodes, want) {
		t.Errorf("all got %v, want %v", all.nodes, want)
	}
	if want := []string{"node.up:w2"}; !slices.Equal(late.nodes, want) {
		t.Errorf("late got %v, want %v", late.nodes, want)
	}
}

// A worker-only node's agent works from a RemoteView of a server's runtime and
// behaves as on a server: it waits for its own boot, then runs its orders once.
func TestAgentOnRemoteView(t *testing.T) {
	rt := NewRuntime()
	rt.Register(fake("svc"), "w1")
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "w1"}}) // starts svc
	rt.PlaceOrders("svc", []OrderSpec{{Node: "w1", Kind: "start"}})

	view := NewRemoteView("w1", func(_ context.Context, node string) (AgentState, error) {
		return rt.AgentState(node), nil
	})
	view.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	view.Run(ctx)

	submit := func(_ context.Context, ev Event) error { rt.Apply(ev); return nil }
	w := &fakeWorker{}
	a := NewAgent("w1", view, submit, w)
	a.interval = 5 * time.Millisecond
	a.Run(ctx)

	waitFor(t, "order to be settled", func() bool { return len(rt.PendingOrders("svc", "w1")) == 0 })
	time.Sleep(50 * time.Millisecond)
	if n := w.executed.Load(); n != 1 {
		t.Fatalf("order executed %d times, want 1", n)
	}
	if rt.Report("svc", "w1") == nil {
		t.Fatal("report not filed through the remote view")
	}
}
