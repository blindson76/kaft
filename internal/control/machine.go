package control

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/blindson76/gstate"
)

// syncEvent is a marker sent behind every delivered event. No machine handles
// it; seeing it reach the front of the mailbox proves the previous event has
// been fully processed.
const syncEvent = "control.sync"

// deliveryKey marks the context of a committed event, so the raise handler can
// tell deterministic raises (reactions to the log) from leader-only ones
// (timers, invokes).
type deliveryKey struct{}

// machine adapts a gstate machine to the Controller interface.
type machine[D gstate.Cloner[D]] struct {
	name    string
	def     *gstate.Machine[string, string, D]
	initial D
	host    Host
	synced  chan struct{}
	deps    []Requirement

	mu     sync.RWMutex
	actor  *gstate.Actor[string, string, D]
	active bool
}

// NewMachine wraps a gstate machine as a Controller.
//
// Rules for the machine, all of which keep the replicas identical:
//   - Guards and actions must be pure functions of the data and the event: no
//     clock, no randomness, no I/O, no ranging over a map to pick something.
//   - Orders are placed by raising RaiseOrder from a transition triggered by
//     an event (On / Always), never from After or an invoke.
//   - Any other raised event, including After timers, is committed to the
//     raft log by the leader and comes back to this machine as a normal event.
//
// requires declares dependencies on other controllers. The machine is told
// about them through EvDepsReady / EvDepsLost and decides what they gate.
func NewMachine[D gstate.Cloner[D]](name string, def *gstate.Machine[string, string, D], initial D, requires ...Requirement) Controller {
	return &machine[D]{name: name, def: def, initial: initial, synced: make(chan struct{}, 1), deps: requires}
}

func (m *machine[D]) Requires() []Requirement { return m.deps }

func (m *machine[D]) HasState(state string) bool {
	_, ok := m.def.States[state]
	return ok
}

func (m *machine[D]) Name() string { return m.name }

func (m *machine[D]) options() []gstate.Option[string, string, D] {
	return []gstate.Option[string, string, D]{
		m.def.WithServiceActivation(false),
		m.def.WithInternalViaHandler(true),
		m.def.WithRaiseHandler(m.onRaise),
		m.def.WithObservers(gstate.ObserverFuncs[string, string, D]{
			EventReceivedFunc: func(_ context.Context, n *gstate.EventNotice[string, string, D]) {
				if n.Event == syncEvent {
					select {
					case m.synced <- struct{}{}:
					default:
					}
				}
			},
		}),
	}
}

func (m *machine[D]) Start(host Host) {
	m.host = host
	m.actor = gstate.Start(m.def, m.initial, m.options()...)
}

func (m *machine[D]) current() *gstate.Actor[string, string, D] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.actor
}

func (m *machine[D]) Deliver(ev Event) {
	a := m.current()
	ctx := context.WithValue(context.Background(), deliveryKey{}, true)
	err := a.SendCtxEvent(ctx, gstate.Event[string]{Name: ev.Name, Params: ev.Params, Internal: ev.Internal})
	if err != nil {
		log.Printf("[%s] deliver %s: %v", m.name, ev.Name, err)
		return
	}
	if err := a.SendCtx(context.Background(), syncEvent); err != nil {
		return
	}
	select {
	case <-m.synced:
	case <-a.Done():
	}
}

func (m *machine[D]) onRaise(ctx context.Context, e gstate.RaisedEvent[string, string]) {
	switch e.Event {
	case RaiseOrder, RaiseCancel:
		if ctx.Value(deliveryKey{}) == nil {
			log.Printf("[%s] %s raised outside a committed event (state %s); ignored", m.name, e.Event, e.State)
			return
		}
		if e.Event == RaiseCancel {
			node, _ := e.Params["node"].(string)
			m.host.CancelOrders(m.name, node)
			return
		}
		specs, _ := e.Params["orders"].([]OrderSpec)
		m.host.PlaceOrders(m.name, specs)
	default:
		m.host.Forward(Event{Target: m.name, Name: e.Event, Params: e.Params, Internal: e.Internal})
	}
}

func (m *machine[D]) Activate() {
	m.mu.Lock()
	m.active = true
	a := m.actor
	m.mu.Unlock()
	a.Activate()
}

func (m *machine[D]) Stop(ctx context.Context) error {
	return m.current().StopCtx(ctx)
}

func (m *machine[D]) Snapshot() (json.RawMessage, error) {
	return json.Marshal(m.current().Snapshot())
}

func (m *machine[D]) Restore(raw json.RawMessage) error {
	var snap gstate.Snapshot[string, D]
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	next := gstate.Hydrate(m.def, snap, m.options()...)

	m.mu.Lock()
	prev := m.actor
	m.actor = next
	m.mu.Unlock()

	prev.Stop()
	select {
	case <-m.synced:
	default:
	}
	return nil
}

func (m *machine[D]) Diagram() string {
	return gstate.ToMermaid(m.def)
}

func (m *machine[D]) States() []string {
	return m.current().States()
}
