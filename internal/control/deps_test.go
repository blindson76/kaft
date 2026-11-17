package control

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// fakeController is a controller reduced to what dependencies look at: a
// current state, moved by "goto" events, and a list of requirements.
type fakeController struct {
	name     string
	state    string
	states   []string
	requires []Requirement
	// onReady, if set, is the state entered on EvDepsReady.
	onReady string
	got     []string // dependency events received, in order
}

func (f *fakeController) Name() string               { return f.name }
func (f *fakeController) Start(Host)                 {}
func (f *fakeController) Activate()                  {}
func (f *fakeController) Stop(context.Context) error { return nil }
func (f *fakeController) States() []string           { return []string{f.state} }
func (f *fakeController) Diagram() string            { return "" }
func (f *fakeController) Requires() []Requirement    { return f.requires }
func (f *fakeController) HasState(s string) bool     { return slices.Contains(f.states, s) }
func (f *fakeController) Restore(json.RawMessage) error {
	return nil
}
func (f *fakeController) Snapshot() (json.RawMessage, error) {
	return json.Marshal(f.state)
}

func (f *fakeController) Deliver(ev Event) {
	switch ev.Name {
	case "goto":
		f.state, _ = ev.Params["state"].(string)
	case EvDepsReady:
		f.got = append(f.got, "ready")
		if f.onReady != "" {
			f.state = f.onReady
		}
	case EvDepsLost:
		f.got = append(f.got, "lost")
	}
}

func fake(name string, requires ...Requirement) *fakeController {
	return &fakeController{name: name, state: "init", states: []string{"init", "up", "down"}, requires: requires}
}

func goTo(rt *Runtime, controller, state string) {
	rt.Apply(Event{Target: controller, Name: "goto", Params: map[string]any{"state": state}})
}

func TestDependencyEvents(t *testing.T) {
	a := fake("a")
	b := fake("b", Requirement{Controller: "a", States: []string{"up"}})
	rt := NewRuntime()
	rt.Register(b) // registration order must not matter
	rt.Register(a)

	rt.Apply(Event{Name: "anything"})
	if !slices.Equal(a.got, []string{"ready"}) {
		t.Fatalf("controller without requirements got %v, want one ready", a.got)
	}
	if len(b.got) != 0 {
		t.Fatalf("b got %v before its requirement held", b.got)
	}

	goTo(rt, "a", "up")
	if !slices.Equal(b.got, []string{"ready"}) {
		t.Fatalf("b got %v, want ready once a is up", b.got)
	}

	// Nothing changed for b: no repeated event.
	rt.Apply(Event{Name: "anything"})
	goTo(rt, "a", "up")
	if !slices.Equal(b.got, []string{"ready"}) {
		t.Fatalf("b got %v, want no repeat while the requirement keeps holding", b.got)
	}

	goTo(rt, "a", "down")
	goTo(rt, "a", "up")
	if !slices.Equal(b.got, []string{"ready", "lost", "ready"}) {
		t.Fatalf("b got %v, want ready, lost, ready", b.got)
	}
	if len(a.got) != 1 {
		t.Fatalf("a got %v, want nothing more", a.got)
	}
}

// One event can satisfy a whole chain: a comes up, which readies b, which
// moves b up, which readies c.
func TestDependencyChainSettlesInOneApply(t *testing.T) {
	a := fake("a")
	b := fake("b", Requirement{Controller: "a", States: []string{"up"}})
	b.onReady = "up"
	c := fake("c", Requirement{Controller: "b", States: []string{"up"}})
	rt := NewRuntime()
	for _, x := range []*fakeController{c, b, a} {
		rt.Register(x)
	}
	if err := rt.Validate(); err != nil {
		t.Fatal(err)
	}

	goTo(rt, "a", "up")
	if !slices.Equal(b.got, []string{"ready"}) || !slices.Equal(c.got, []string{"ready"}) {
		t.Fatalf("after one event: b got %v, c got %v; want both ready", b.got, c.got)
	}
}

// A requirement listing several states holds in any of them, and a controller
// with several requirements needs all of them.
func TestDependencyAnyStateAllRequirements(t *testing.T) {
	a, b := fake("a"), fake("b")
	c := fake("c",
		Requirement{Controller: "a", States: []string{"up", "down"}},
		Requirement{Controller: "b", States: []string{"up"}})
	rt := NewRuntime()
	for _, x := range []*fakeController{a, b, c} {
		rt.Register(x)
	}

	goTo(rt, "a", "down")
	if len(c.got) != 0 {
		t.Fatalf("c got %v with only one of two requirements met", c.got)
	}
	goTo(rt, "b", "up")
	if !slices.Equal(c.got, []string{"ready"}) {
		t.Fatalf("c got %v, want ready", c.got)
	}
}

func TestValidateRequirements(t *testing.T) {
	tests := []struct {
		name    string
		build   func() []*fakeController
		wantErr string
	}{
		{"valid chain", func() []*fakeController {
			return []*fakeController{fake("a"), fake("b", Requirement{"a", []string{"up"}})}
		}, ""},
		{"unknown controller", func() []*fakeController {
			return []*fakeController{fake("b", Requirement{"nope", []string{"up"}})}
		}, `unknown controller "nope"`},
		{"unknown state", func() []*fakeController {
			return []*fakeController{fake("a"), fake("b", Requirement{"a", []string{"running"}})}
		}, `state "running"`},
		{"no states", func() []*fakeController {
			return []*fakeController{fake("a"), fake("b", Requirement{"a", nil})}
		}, "in no state"},
		{"cycle", func() []*fakeController {
			return []*fakeController{
				fake("a", Requirement{"c", []string{"up"}}),
				fake("b", Requirement{"a", []string{"up"}}),
				fake("c", Requirement{"b", []string{"up"}}),
			}
		}, "cycle: a -> c -> b -> a"},
		{"self", func() []*fakeController {
			return []*fakeController{fake("a", Requirement{"a", []string{"up"}})}
		}, "cycle: a -> a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewRuntime()
			for _, c := range tc.build() {
				rt.Register(c)
			}
			err := rt.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// recorder notes every event it is given, as "name" or "name:node".
type recorder struct {
	*fakeController
	seen []string
}

func (r *recorder) Deliver(ev Event) {
	entry := ev.Name
	if node, ok := ev.Params["node"].(string); ok {
		entry += ":" + node
	}
	r.seen = append(r.seen, entry)
	r.fakeController.Deliver(ev)
}

// A controller with unmet requirements is not started: no event reaches it.
// When it starts it is told the liveness it missed, as it is now, and then
// that its dependencies are ready.
func TestControllerNotStartedUntilDependenciesHold(t *testing.T) {
	a := fake("a")
	b := &recorder{fakeController: fake("b", Requirement{Controller: "a", States: []string{"up"}})}
	rt := NewRuntime()
	rt.Register(a)
	rt.Register(b)

	node := func(name, n string) Event { return Event{Name: name, Params: map[string]any{"node": n}} }
	rt.Apply(node(EvNodeUp, "n2"))
	rt.Apply(node(EvNodeUp, "n1"))
	rt.Apply(node(EvNodeUp, "n3"))
	rt.Apply(node(EvNodeDown, "n3"))
	rt.Apply(Event{Target: "b", Name: "goto", Params: map[string]any{"state": "up"}})

	if rt.Started("b") || len(b.seen) != 0 || b.state != "init" {
		t.Fatalf("b before its dependency: started=%v state=%s events=%v; want untouched", rt.Started("b"), b.state, b.seen)
	}
	if !rt.Started("a") {
		t.Fatal("a has no requirements and should have started")
	}

	goTo(rt, "a", "up")
	want := []string{"node.up:n1", "node.up:n2", "node.down:n3", "deps.ready"}
	if !rt.Started("b") || !slices.Equal(b.seen, want) {
		t.Fatalf("b on start: started=%v events=%v\nwant %v", rt.Started("b"), b.seen, want)
	}

	// From here on it gets events like any other controller, and losing the
	// dependency does not stop it.
	goTo(rt, "a", "down")
	rt.Apply(node(EvNodeUp, "n3"))
	want = append(want, "deps.lost", "node.up:n3")
	if !rt.Started("b") || !slices.Equal(b.seen, want) {
		t.Fatalf("b after start: started=%v events=%v\nwant %v", rt.Started("b"), b.seen, want)
	}
}
