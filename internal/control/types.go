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

	"github.com/blindson76/gstate"
)

// Framework event names. Controllers handle these with On(...).
const (
	// EvNodeUp / EvNodeDown are broadcast to every controller when the leader
	// observes a liveness change. Params: "node".
	EvNodeUp   = "node.up"
	EvNodeDown = "node.down"
	// EvReport carries a worker's local probe result to its controller.
	// Params: "node", "report" (the probe value, JSON-decoded).
	EvReport = "report"
	// EvOrderDone / EvOrderFailed carry the outcome of an order to the
	// controller that placed it. Params: "id", "node", "kind", "args", and
	// "error" for failures.
	EvOrderDone   = "order.done"
	EvOrderFailed = "order.failed"
)

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
// a result or the controller cancels it, so it survives leader changes and can
// be executed more than once.
type Order struct {
	ID         uint64 `json:"id"`
	Controller string `json:"controller"`
	OrderSpec
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
	Snapshot() (json.RawMessage, error)
	Restore(json.RawMessage) error
	// States lists the active states, for inspection.
	States() []string
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
