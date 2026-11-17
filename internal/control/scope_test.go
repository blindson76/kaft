package control

import (
	"slices"
	"testing"
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
