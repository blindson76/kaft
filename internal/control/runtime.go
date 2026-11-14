package control

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"sync"
)

// store is the replicated state shared by all controllers.
type store struct {
	// Seq is the last order ID handed out.
	Seq uint64 `json:"seq"`
	// Alive is the leader-observed liveness of each node.
	Alive map[string]bool `json:"alive"`
	// Boots is the boot ID each node's agent last announced.
	Boots map[string]string `json:"boots"`
	// Reports holds the last probe result per controller and node, as
	// canonical JSON.
	Reports map[string]map[string]json.RawMessage `json:"reports"`
	// Orders holds the pending orders by ID.
	Orders map[uint64]Order `json:"orders"`
}

func newStore() store {
	return store{
		Alive:   map[string]bool{},
		Boots:   map[string]string{},
		Reports: map[string]map[string]json.RawMessage{},
		Orders:  map[uint64]Order{},
	}
}

// Runtime is the replicated side of the control layer: the shared store plus
// the registered controllers. Every node runs one and feeds it the same
// committed events in the same order, so all replicas stay identical.
type Runtime struct {
	// applyMu serializes Apply, Snapshot and Restore.
	applyMu sync.Mutex

	// mu guards store and active. It is never held while a controller runs.
	mu     sync.RWMutex
	store  store
	active bool

	controllers []Controller
	byName      map[string]Controller
	outbox      chan Event
}

func NewRuntime() *Runtime {
	return &Runtime{
		store:  newStore(),
		byName: map[string]Controller{},
		outbox: make(chan Event, 1024),
	}
}

// Register adds a controller. All controllers must be registered, on every
// node, before the first event is applied.
func (r *Runtime) Register(c Controller) {
	if _, dup := r.byName[c.Name()]; dup {
		panic(fmt.Sprintf("control: controller %q registered twice", c.Name()))
	}
	r.controllers = append(r.controllers, c)
	r.byName[c.Name()] = c
	c.Start(r)
}

// Apply processes one committed event: it updates the shared store, then
// delivers the event to the controller(s) it concerns. It returns once every
// controller has finished reacting, so a snapshot taken afterwards is
// consistent with the log position.
func (r *Runtime) Apply(ev Event) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	node, _ := ev.Params["node"].(string)

	switch ev.Name {
	case EvNodeUp, EvNodeDown:
		r.mu.Lock()
		r.store.Alive[node] = ev.Name == EvNodeUp
		r.mu.Unlock()
		ev.Target = ""

	case EvNodeReady:
		boot, _ := ev.Params["boot"].(string)
		if node == "" || boot == "" {
			return
		}
		r.mu.Lock()
		r.store.Boots[node] = boot
		r.mu.Unlock()
		ev.Target = ""

	case EvReport:
		raw, err := json.Marshal(ev.Params["report"])
		if err != nil || node == "" || r.byName[ev.Target] == nil {
			return
		}
		r.mu.Lock()
		if r.store.Reports[ev.Target] == nil {
			r.store.Reports[ev.Target] = map[string]json.RawMessage{}
		}
		r.store.Reports[ev.Target][node] = raw
		r.mu.Unlock()

	case EvOrderDone, EvOrderFailed:
		// A result for an order that is no longer pending is a duplicate or
		// arrives after a cancel: drop it so controllers see each outcome once.
		id := toUint(ev.Params["id"])
		r.mu.Lock()
		order, ok := r.store.Orders[id]
		if ok {
			delete(r.store.Orders, id)
		}
		r.mu.Unlock()
		if !ok {
			return
		}
		params := map[string]any{"id": id, "node": order.Node, "kind": order.Kind, "args": order.Args}
		if ev.Name == EvOrderFailed {
			params["error"] = ev.Params["error"]
		}
		ev.Target, ev.Params = order.Controller, params
	}

	if ev.Target != "" {
		if c := r.byName[ev.Target]; c != nil {
			c.Deliver(ev)
		}
		return
	}
	for _, c := range r.controllers {
		c.Deliver(ev)
	}
}

// PlaceOrders implements Host.
func (r *Runtime) PlaceOrders(controller string, specs []OrderSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, spec := range specs {
		r.store.Seq++
		r.store.Orders[r.store.Seq] = Order{ID: r.store.Seq, Controller: controller, OrderSpec: spec}
		log.Printf("[%s] order #%d: %s on %s", controller, r.store.Seq, spec.Kind, spec.Node)
	}
}

// CancelOrders implements Host.
func (r *Runtime) CancelOrders(controller string, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, o := range r.store.Orders {
		if o.Controller == controller && (node == "" || o.Node == node) {
			delete(r.store.Orders, id)
			log.Printf("[%s] order #%d cancelled", controller, id)
		}
	}
}

// Forward implements Host.
func (r *Runtime) Forward(ev Event) {
	select {
	case r.outbox <- ev:
	default:
		log.Printf("control: outbox full, dropping %s/%s", ev.Target, ev.Name)
	}
}

// Outbox yields the events controllers raised outside the committed stream.
// The owner submits them to the raft log when leader and discards them
// otherwise.
func (r *Runtime) Outbox() <-chan Event { return r.outbox }

// Activate starts the leader-only services of every controller.
func (r *Runtime) Activate() {
	r.mu.Lock()
	r.active = true
	r.mu.Unlock()
	for _, c := range r.controllers {
		c.Activate()
	}
}

// Alive reports the replicated liveness of node.
func (r *Runtime) Alive(node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.Alive[node]
}

// Boot returns the boot ID last announced by node's agent, or "".
func (r *Runtime) Boot(node string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.Boots[node]
}

// Report returns the last replicated report of controller for node, or nil.
func (r *Runtime) Report(controller, node string) json.RawMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.Reports[controller][node]
}

// PendingOrders returns the pending orders of controller for node, oldest
// first.
func (r *Runtime) PendingOrders(controller, node string) []Order {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Order
	for _, o := range r.store.Orders {
		if o.Controller == controller && o.Node == node {
			out = append(out, o)
		}
	}
	slices.SortFunc(out, func(a, b Order) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

type snapshot struct {
	Store       store                      `json:"store"`
	Controllers map[string]json.RawMessage `json:"controllers"`
}

// Snapshot serializes the store and every controller.
func (r *Runtime) Snapshot() ([]byte, error) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	snap := snapshot{Controllers: map[string]json.RawMessage{}}
	for _, c := range r.controllers {
		raw, err := c.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", c.Name(), err)
		}
		snap.Controllers[c.Name()] = raw
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap.Store = r.store
	return json.Marshal(snap)
}

// Restore replaces the store and every controller's state.
func (r *Runtime) Restore(data []byte) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	snap := snapshot{Store: newStore()}
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	for _, c := range r.controllers {
		raw, ok := snap.Controllers[c.Name()]
		if !ok {
			continue
		}
		if err := c.Restore(raw); err != nil {
			return fmt.Errorf("restore %s: %w", c.Name(), err)
		}
	}
	r.mu.Lock()
	r.store = snap.Store
	active := r.active
	r.mu.Unlock()
	if active {
		for _, c := range r.controllers {
			c.Activate()
		}
	}
	return nil
}

// Inspect returns a JSON-serializable view of the runtime for debugging.
func (r *Runtime) Inspect() any {
	snap, err := r.Snapshot()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	states := map[string][]string{}
	for _, c := range r.controllers {
		states[c.Name()] = c.States()
	}
	return map[string]any{"states": states, "snapshot": json.RawMessage(snap)}
}
