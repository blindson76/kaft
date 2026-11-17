// Package control is the orchestration skeleton that sits between raft and the
// service-specific controllers.
//
// The pieces:
//
//   - Event: the only thing that travels through the raft log.
//   - Runtime: the replicated side. It owns the shared store (node liveness,
//     node reports, orders) and feeds committed events to the controllers.
//   - Controller: a decision maker living inside the Runtime. It must be
//     deterministic: its only input is the committed event stream.
//   - Worker / Agent: the node-local side. A Worker probes and manipulates one
//     local service; the Agent reports probe results and executes the orders
//     addressed to its node. All side effects live here.
//
// Adding a new service means registering one Controller and one Worker with the
// same name; nothing else in this package changes.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/blindson76/gstate"
)

// Framework event names. Controllers handle these with On(...).
const (
	// EvNodeUp / EvNodeDown are broadcast to every controller when the leader
	// observes a liveness change. Params: "node". Right after EvNodeDown,
	// the node's pending orders fail with error ErrNodeDown.
	EvNodeUp   = "node.up"
	EvNodeDown = "node.down"
	// EvNodeReady is committed by a node's agent when it starts, and
	// broadcast to every controller: the node (re)started and its local
	// services may have lost their state. Params: "node", "boot" (an ID
	// unique to that start). Applying its own EvNodeReady is also how an
	// agent knows it has caught up with the log; see Agent.
	EvNodeReady = "node.ready"
	// EvReport carries a worker's local probe result to its controller.
	// Params: "node", "report" (the probe value, JSON-decoded).
	EvReport = "report"
	// EvOrderDone / EvOrderFailed carry the outcome of an order to the
	// controller that placed it. Params: "id", "node", "kind", "args", and
	// "error" for failures.
	EvOrderDone   = "order.done"
	EvOrderFailed = "order.failed"
	// EvDepsReady is the last thing a controller receives when it is
	// started, which happens once its dependencies (see Requirement) hold;
	// immediately for a controller without any. After that, EvDepsLost and
	// EvDepsReady report a dependency being lost and regained. Params of
	// EvDepsLost: "waiting" (the unmet requirements, as text).
	EvDepsReady = "deps.ready"
	EvDepsLost  = "deps.lost"
)

// Requirement is a dependency between controllers: the controller declaring
// it wants Controller to be in one of States.
//
// A controller is not started until all its requirements hold: it receives no
// events and its worker stays idle, so nothing of it runs on any node. When it
// starts it is told the current node liveness and then gets EvDepsReady.
//
// Starting is one-way. If a requirement stops holding later the controller
// keeps running and is told with EvDepsLost (and EvDepsReady when it holds
// again); how to react is the controller's decision.
type Requirement struct {
	Controller string   `json:"controller" yaml:"controller"`
	States     []string `json:"states" yaml:"states"`
}

func (r Requirement) String() string {
	return r.Controller + " in " + strings.Join(r.States, "|")
}

// RaiseOrder is the event name a controller raises (as an external event, from
// a transition) to place orders. Build its params with Orders.
const RaiseOrder = "order"

// Event is the unit replicated through the raft log.
type Event struct {
	// Target is the controller the event is for; empty means all of them.
	Target string         `json:"target,omitempty"`
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
	// Internal marks an event a controller raised for itself.
	Internal bool `json:"internal,omitempty"`
	// At is when the event was committed, in Unix milliseconds by the
	// leader's clock. It travels in the log, so every replica sees the same
	// value; 0 when unknown.
	At int64 `json:"at,omitempty"`
}

// RaiseCancel is the event name a controller raises to withdraw its pending
// orders. Build its params with Cancel.
const RaiseCancel = "order.cancel"

// OrderSpec is what a controller asks for: do Kind with Args on Node.
type OrderSpec struct {
	Node string         `json:"node"`
	Kind string         `json:"kind"`
	Args map[string]any `json:"args,omitempty"`
}

// Order is a pending OrderSpec recorded in the replicated store. It is state,
// not a message: it stays in the store until the target node's worker reports
// a result, the controller cancels it or the node goes down (it then fails, see
// EvNodeDown), so it survives leader changes and can be executed more than
// once.
type Order struct {
	ID         uint64 `json:"id"`
	Controller string `json:"controller"`
	OrderSpec
	// Placed is when the order was placed (Unix ms, see Event.At), Cause the
	// committed event the controller was reacting to when it placed it.
	Placed int64  `json:"placed,omitempty"`
	Cause  string `json:"cause,omitempty"`
}

// Order outcomes, as recorded in the order history.
const (
	OrderDone      = "done"
	OrderFailed    = "failed"
	OrderCancelled = "cancelled"
)

// OrderRecord is a finished order in the replicated order history.
type OrderRecord struct {
	Order
	// Status is OrderDone, OrderFailed or OrderCancelled.
	Status string `json:"status"`
	// Error is the failure message of a failed order.
	Error string `json:"error,omitempty"`
	// Result is what the worker returned, for a ResultWorker.
	Result json.RawMessage `json:"result,omitempty"`
	// Finished is when the outcome was committed (Unix ms). Started and
	// DurationMs are when and how long the worker ran it, by the node's
	// clock; 0 for a cancelled order.
	Finished   int64 `json:"finished,omitempty"`
	Started    int64 `json:"started,omitempty"`
	DurationMs int64 `json:"duration_ms,omitempty"`
}

// Orders builds the params for raising RaiseOrder:
//
//	sb.On("x").RaiseWith(control.RaiseOrder, func(d Data) gstate.Params {
//	    return control.Orders(control.OrderSpec{Node: "node-1", Kind: "start"})
//	}).GoTo("starting")
func Orders(specs ...OrderSpec) gstate.Params {
	return gstate.Params{"orders": specs}
}

// Cancel builds the params for raising RaiseCancel. It withdraws the raising
// controller's pending orders for node, or all of them when node is empty.
func Cancel(node string) gstate.Params {
	return gstate.Params{"node": node}
}

// Controller is a deterministic decision maker driven by committed events.
// Use NewMachine to build one from a gstate machine.
type Controller interface {
	Name() string
	// Start is called once, before any event is delivered.
	Start(host Host)
	// Deliver processes one committed event and returns only after the
	// controller has fully reacted to it (including placing orders).
	Deliver(ev Event)
	// Activate starts leader-only services (timers, invokes). Idempotent.
	Activate()
	// Stop ends the controller's own goroutines (timers, invokes) when the
	// node shuts down, waiting for them until ctx ends. No event is delivered
	// afterwards.
	Stop(ctx context.Context) error
	Snapshot() (json.RawMessage, error)
	Restore(json.RawMessage) error
	// States lists the active states, for inspection.
	States() []string
	// Diagram returns the state machine as a Mermaid flowchart, for the web
	// view. It never changes.
	Diagram() string
	// Requires lists what this controller depends on. It never changes.
	Requires() []Requirement
	// HasState reports whether the controller has a state with this name;
	// used to reject requirements that could never be met.
	HasState(state string) bool
}

// Host is what the Runtime offers to a Controller.
type Host interface {
	// PlaceOrders records orders in the replicated store. Only valid while
	// the controller is reacting to a delivered event.
	PlaceOrders(controller string, specs []OrderSpec)
	// CancelOrders drops the controller's pending orders for node (all nodes
	// when empty). Same validity rule as PlaceOrders.
	CancelOrders(controller string, node string)
	// Forward submits an event raised outside the committed stream (a timer,
	// an invoke) to the raft log. It never blocks; non-leaders drop it.
	Forward(ev Event)
}

// ErrNotReady is returned by Worker.Probe while the worker has nothing to
// report yet, for instance during a slow first inspection. The agent skips the
// report quietly and asks again.
var ErrNotReady = errors.New("worker not ready")

// Worker is the node-local counterpart of the controller with the same name.
type Worker interface {
	Name() string
	// Probe returns the local state of the service as a JSON-serializable
	// value. It is called periodically; a report is sent only on change.
	Probe(ctx context.Context) (any, error)
	// Execute carries out an order addressed to this node. It must be
	// idempotent: the same order can be delivered again after a retry or a
	// restart, so check the local state before acting.
	Execute(ctx context.Context, order Order) error
}

// ResultWorker is a Worker whose orders produce a value worth keeping, such
// as what a command printed or the member list it applied. When a worker
// implements it, the agent calls ExecuteResult instead of Execute; the
// result, which must be JSON-serializable and small, is stored in the order
// history and passed to the controller as the "result" parameter.
type ResultWorker interface {
	Worker
	ExecuteResult(ctx context.Context, order Order) (any, error)
}

// Closer is a Worker that holds local resources: a service process,
// connections, goroutines of its own. Close is called once when the node shuts
// down, after the agent has stopped, so no Probe or Execute runs during or
// after it. It should stop the local service cleanly, release everything, and
// cut corners once ctx ends.
//
// The worker's own goroutines must not outlive Close; that is why they run on
// a context Close cancels, never on context.Background().
type Closer interface {
	Close(ctx context.Context) error
}

// MetricsWorker is a Worker that also offers live metrics of its service, for
// the web view. They are read on demand and never go through the raft log.
// Metrics should answer quickly and may return cached values.
type MetricsWorker interface {
	Metrics(ctx context.Context) (any, error)
}

// Node returns the "node" parameter of a framework event.
func Node(e gstate.Event[string]) string {
	s, _ := e.Params["node"].(string)
	return s
}

// OrderID returns the "id" parameter of an order result event.
func OrderID(e gstate.Event[string]) uint64 {
	return toUint(e.Params["id"])
}

// Report decodes the "report" parameter of an EvReport event into T.
func Report[T any](e gstate.Event[string]) (T, error) {
	var out T
	raw, err := json.Marshal(e.Params["report"])
	if err != nil {
		return out, err
	}
	return out, json.Unmarshal(raw, &out)
}

// toUint reads a number that may have gone through JSON (float64).
func toUint(v any) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case float64:
		return uint64(n)
	case int:
		return uint64(n)
	case json.Number:
		i, _ := n.Int64()
		return uint64(i)
	}
	return 0
}
