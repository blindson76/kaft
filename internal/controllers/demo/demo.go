// Package demo is a minimal controller/worker pair that exercises the control
// skeleton end to end. It is a template for real controllers, not a feature:
// once enough nodes are alive it orders each of them to "greet", waits for the
// results, then idles with a periodic timer.
package demo

import (
	"context"
	"log"
	"slices"
	"sync/atomic"
	"time"

	"example.com/dstate/internal/control"
	"github.com/blindson76/gstate"
)

const Name = "demo"

// Data is the controller's replicated data.
type Data struct {
	Nodes   []string `json:"nodes"`   // alive nodes, sorted
	Waiting int      `json:"waiting"` // order results still expected
	Ticks   int      `json:"ticks"`
}

func (d Data) Clone() Data {
	d.Nodes = slices.Clone(d.Nodes)
	return d
}

type (
	builder = gstate.StateBuilder[string, string, Data]
	event   = gstate.Event[string]
)

// NewController builds the demo controller. minNodes is how many alive nodes
// it waits for before placing orders.
func NewController(minNodes int) control.Controller {
	// trackNodes keeps Data.Nodes in sync with liveness in whatever state the
	// machine is.
	trackNodes := func(sb *builder) {
		sb.On(control.EvNodeUp).AssignEvent(func(d Data, e event) Data {
			if n := control.Node(e); !slices.Contains(d.Nodes, n) {
				d.Nodes = append(d.Nodes, n)
				slices.Sort(d.Nodes)
			}
			return d
		})
		sb.On(control.EvNodeDown).AssignEvent(func(d Data, e event) Data {
			d.Nodes = slices.DeleteFunc(d.Nodes, func(n string) bool { return n == control.Node(e) })
			return d
		})
	}
	oneResult := func(d Data) Data {
		d.Waiting--
		return d
	}

	def := gstate.New[string, string, Data](Name).
		Initial("waiting").
		State("waiting", func(sb *builder) {
			trackNodes(sb)
			sb.Always().
				Guard(func(d Data) bool { return len(d.Nodes) >= minNodes }).
				Assign(func(d Data) Data {
					d.Waiting = len(d.Nodes)
					return d
				}).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params {
					specs := make([]control.OrderSpec, 0, len(d.Nodes))
					for _, n := range d.Nodes {
						specs = append(specs, control.OrderSpec{Node: n, Kind: "greet", Args: map[string]any{"peers": len(d.Nodes)}})
					}
					return control.Orders(specs...)
				}).
				GoTo("greeting")
		}).
		State("greeting", func(sb *builder) {
			trackNodes(sb)
			sb.On(control.EvOrderDone).Assign(oneResult)
			sb.On(control.EvOrderFailed).Assign(oneResult)
			sb.Always().Guard(func(d Data) bool { return d.Waiting <= 0 }).GoTo("ready")
			// A node that died never answers: withdraw what is left and retry
			// with the nodes alive at that point.
			sb.After(10 * time.Second).Raise("timeout")
			sb.On("timeout").Raise(control.RaiseCancel, control.Cancel("")).GoTo("waiting")
		}).
		State("ready", func(sb *builder) {
			trackNodes(sb)
			// The timer runs on the leader only; the event it raises is
			// committed and applied on every node.
			sb.After(5 * time.Second).Raise("tick")
			sb.On("tick").Assign(func(d Data) Data {
				d.Ticks++
				return d
			}).GoTo("ready")
		}).
		Build()

	return control.NewMachine(Name, def, Data{})
}

// Worker is the node-local side of the demo controller.
type Worker struct {
	greeted atomic.Int64
}

func NewWorker() *Worker { return &Worker{} }

func (w *Worker) Name() string { return Name }

func (w *Worker) Probe(ctx context.Context) (any, error) {
	return map[string]any{"greeted": w.greeted.Load()}, nil
}

func (w *Worker) Execute(ctx context.Context, order control.Order) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}
	w.greeted.Add(1)
	log.Printf("[demo] hello! %s with args %v", order.Kind, order.Args)
	return nil
}
