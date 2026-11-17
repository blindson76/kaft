package control

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
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
	// Deps records, per controller, whether its requirements held when last
	// evaluated. A controller is told only when this flips.
	Deps map[string]bool `json:"deps"`
	// Started lists the controllers that have been started. A controller
	// starts the first time its requirements hold, and stays started.
	Started map[string]bool `json:"started"`
}

func newStore() store {
	return store{
		Alive:   map[string]bool{},
		Boots:   map[string]string{},
		Reports: map[string]map[string]json.RawMessage{},
		Orders:  map[uint64]Order{},
		Deps:    map[string]bool{},
		Started: map[string]bool{},
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
	// scope holds, per controller, the nodes that run its worker. A
	// controller without an entry concerns every node.
	scope  map[string]map[string]bool
	outbox chan Event

	// subs are the listeners told whenever the replicated state changed.
	subMu sync.Mutex
	subs  map[chan struct{}]struct{}
}

func NewRuntime() *Runtime {
	return &Runtime{
		store:  newStore(),
		byName: map[string]Controller{},
		scope:  map[string]map[string]bool{},
		outbox: make(chan Event, 1024),
		subs:   map[chan struct{}]struct{}{},
	}
}

// Register adds a controller. All controllers must be registered, on every
// server node, before the first event is applied.
//
// nodes are the nodes that run the controller's worker; the controller only
// hears about those (node.up / node.down / node.ready), so a node that does
// not run its service never counts as one it has to wait for. No nodes means
// every node. The list must be the same on every server.
func (r *Runtime) Register(c Controller, nodes ...string) {
	if _, dup := r.byName[c.Name()]; dup {
		panic(fmt.Sprintf("control: controller %q registered twice", c.Name()))
	}
	r.controllers = append(r.controllers, c)
	r.byName[c.Name()] = c
	if len(nodes) > 0 {
		set := make(map[string]bool, len(nodes))
		for _, n := range nodes {
			set[n] = true
		}
		r.scope[c.Name()] = set
	}
	c.Start(r)
}

// Controllers returns the names of the registered controllers, in
// registration order.
func (r *Runtime) Controllers() []string {
	out := make([]string, 0, len(r.controllers))
	for _, c := range r.controllers {
		out = append(out, c.Name())
	}
	return out
}

// concerns reports whether node runs the worker of controller.
func (r *Runtime) concerns(controller, node string) bool {
	set, ok := r.scope[controller]
	return !ok || set[node]
}

// Apply processes one committed event: it updates the shared store, then
// delivers the event to the controller(s) it concerns. It returns once every
// controller has finished reacting, so a snapshot taken afterwards is
// consistent with the log position.
func (r *Runtime) Apply(ev Event) {
	r.applyMu.Lock()
	defer r.notify()
	defer r.applyMu.Unlock()

	// Before: start whatever can start, so that controllers without
	// requirements see the very first event. After: react to the states the
	// event led to.
	r.settleDeps()
	r.dispatch(ev)
	r.settleDeps()
}

// deliver hands ev to c unless c has not been started yet. Controllers that
// are not started see nothing; what they missed is replayed when they start.
func (r *Runtime) deliver(c Controller, ev Event) {
	if r.Started(c.Name()) {
		c.Deliver(ev)
	}
}

// dispatch updates the store for ev and delivers it.
func (r *Runtime) dispatch(ev Event) {
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
			r.deliver(c, ev)
		}
		return
	}
	for _, c := range r.controllers {
		// Node events go only to the controllers running on that node.
		if node != "" && !r.concerns(c.Name(), node) {
			continue
		}
		r.deliver(c, ev)
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

// Stop stops every controller, once nothing applies events any more (raft has
// shut down). It waits for their goroutines until ctx ends.
func (r *Runtime) Stop(ctx context.Context) error {
	var errs []error
	for _, c := range r.controllers {
		if err := c.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", c.Name(), err))
		}
	}
	return errors.Join(errs...)
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
	defer r.notify()
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

// Subscribe returns a channel that receives a signal whenever the replicated
// state may have changed, and a function to stop listening. Signals are
// coalesced: one pending signal stands for any number of changes, so the
// listener reads the current state (Inspect) rather than counting events.
func (r *Runtime) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	r.subMu.Lock()
	r.subs[ch] = struct{}{}
	r.subMu.Unlock()
	return ch, func() {
		r.subMu.Lock()
		delete(r.subs, ch)
		r.subMu.Unlock()
	}
}

func (r *Runtime) notify() {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	for ch := range r.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Diagrams returns each controller's state machine as a Mermaid flowchart, in
// registration order.
func (r *Runtime) Diagrams() []Diagram {
	out := make([]Diagram, 0, len(r.controllers))
	for _, c := range r.controllers {
		out = append(out, Diagram{Name: c.Name(), Mermaid: c.Diagram(), Requires: c.Requires()})
	}
	return out
}

// Diagram is one controller's state machine drawing.
type Diagram struct {
	Name    string `json:"name"`
	Mermaid string `json:"mermaid"`
	// Requires lists the controller's dependencies.
	Requires []Requirement `json:"requires,omitempty"`
}

// unmet returns the requirements of c that do not hold right now.
func (r *Runtime) unmet(c Controller) []string {
	var out []string
	for _, req := range c.Requires() {
		target := r.byName[req.Controller]
		ok := false
		if target != nil {
			for _, s := range target.States() {
				if slices.Contains(req.States, s) {
					ok = true
					break
				}
			}
		}
		if !ok {
			out = append(out, req.String())
		}
	}
	return out
}

// Started reports whether the controller has been started. A name that is not
// a registered controller counts as started.
func (r *Runtime) Started(controller string) bool {
	if r.byName[controller] == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.Started[controller]
}

// start brings a controller to life once its requirements hold. Until now it
// received no events and its worker did nothing. It is first told what it
// missed that is still true, from the store: which nodes are alive and any
// reports already filed for it. Then it gets EvDepsReady.
func (r *Runtime) start(c Controller) {
	name := c.Name()
	if len(c.Requires()) > 0 {
		log.Printf("[%s] dependencies ready, starting", name)
	}

	r.mu.Lock()
	r.store.Started[name], r.store.Deps[name] = true, true
	alive := maps.Clone(r.store.Alive)
	reports := maps.Clone(r.store.Reports[name])
	r.mu.Unlock()

	for _, node := range slices.Sorted(maps.Keys(alive)) {
		if !r.concerns(name, node) {
			continue
		}
		ev := EvNodeDown
		if alive[node] {
			ev = EvNodeUp
		}
		c.Deliver(Event{Name: ev, Params: map[string]any{"node": node}})
	}
	for _, node := range slices.Sorted(maps.Keys(reports)) {
		var report any
		if json.Unmarshal(reports[node], &report) == nil {
			c.Deliver(Event{Target: name, Name: EvReport, Params: map[string]any{"node": node, "report": report}})
		}
	}
	c.Deliver(Event{Target: name, Name: EvDepsReady})
}

// settleDeps re-evaluates every controller's requirements against the states
// the controllers are in now. A controller that has not started yet is started
// the first time its requirements hold. A started one is told with
// EvDepsLost / EvDepsReady when they stop or resume holding; it is not stopped.
// Starting or telling one controller can move it to a state another one waits
// for, so this repeats until nothing changes.
//
// This runs on every replica around every committed event and depends only on
// controller states, which are themselves a function of the log. So starting
// controllers and the dependency events need no log entries of their own and
// still happen on every replica at the same point.
func (r *Runtime) settleDeps() {
	for round := 0; round <= 2*len(r.controllers); round++ {
		changed := false
		for _, c := range r.controllers {
			name := c.Name()
			waiting := r.unmet(c)
			ready := len(waiting) == 0

			r.mu.Lock()
			started := r.store.Started[name]
			before, known := r.store.Deps[name]
			if !started || before != ready {
				r.store.Deps[name] = ready
			}
			r.mu.Unlock()

			switch {
			case !started && ready:
				r.start(c)
				changed = true
			case !started:
				if !known {
					log.Printf("[%s] not started, waiting for: %s", name, strings.Join(waiting, ", "))
				}
			case before == ready:
			case ready:
				log.Printf("[%s] dependencies ready again", name)
				c.Deliver(Event{Target: name, Name: EvDepsReady})
				changed = true
			default:
				log.Printf("[%s] dependencies lost: %s", name, strings.Join(waiting, ", "))
				c.Deliver(Event{Target: name, Name: EvDepsLost, Params: map[string]any{"waiting": waiting}})
				changed = true
			}
		}
		if !changed {
			return
		}
	}
	log.Printf("control: controller dependencies did not settle; check for requirements that undo each other")
}

// Validate checks the registered controllers' requirements: each must name a
// registered controller and states that controller has, and requirements must
// not form a cycle, which would leave every controller in it waiting forever.
// Call it once after all controllers are registered.
func (r *Runtime) Validate() error {
	for _, c := range r.controllers {
		for _, req := range c.Requires() {
			target := r.byName[req.Controller]
			if target == nil {
				return fmt.Errorf("controller %q requires unknown controller %q", c.Name(), req.Controller)
			}
			if len(req.States) == 0 {
				return fmt.Errorf("controller %q requires %q in no state", c.Name(), req.Controller)
			}
			for _, s := range req.States {
				if !target.HasState(s) {
					return fmt.Errorf("controller %q requires %q in state %q, which it does not have", c.Name(), req.Controller, s)
				}
			}
		}
	}
	// Depth-first search for a cycle; path is the chain being explored.
	done := map[string]bool{}
	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		if i := slices.Index(path, name); i >= 0 {
			return fmt.Errorf("controller dependency cycle: %s", strings.Join(append(path[i:], name), " -> "))
		}
		if done[name] {
			return nil
		}
		for _, req := range r.byName[name].Requires() {
			if err := visit(req.Controller, append(path, name)); err != nil {
				return err
			}
		}
		done[name] = true
		return nil
	}
	for _, c := range r.controllers {
		if err := visit(c.Name(), nil); err != nil {
			return err
		}
	}
	return nil
}
