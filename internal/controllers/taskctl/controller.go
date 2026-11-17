package taskctl

import (
	"log"
	"time"

	"example.com/dstate/internal/control"
	"github.com/blindson76/gstate"
)

type (
	builder = gstate.StateBuilder[string, string, Data]
	evt     = gstate.Event[string]
)

// retryDelay is how long an instance whose process failed waits before it is
// placed again. The timer runs on the leader only.
const retryDelay = 5 * time.Second

// settleTimeout is how long the first placement waits for eligible workers
// that have not reported yet.
const settleTimeout = 10 * time.Second

// States, usable in other controllers' depends_on.
const (
	// StateWaiting: the task's dependencies have not held yet, or it waits
	// for the eligible workers to report before the first placement.
	StateWaiting = "waiting"
	// StateDegraded: some instance is not running (being placed, starting,
	// waiting to retry, or no worker can take it).
	StateDegraded = "degraded"
	// StateHealthy: every instance runs.
	StateHealthy = "healthy"
)

// NewController builds the controller of the task called name.
//
//	waiting ─▶ degraded ◀─▶ healthy
//
// The task starts once its dependencies hold (requires): then instances are
// placed. If a dependency is lost later, running instances are left alone and
// failed ones are still moved; how a service copes without its dependency is
// up to the service.
func NewController(name string, t Topology, requires ...control.Requirement) control.Controller {
	logf := func(format string, args ...any) { log.Printf("[%s] "+format, append([]any{name}, args...)...) }

	track := func(sb *builder) {
		sb.On(control.EvNodeUp).AssignEvent(func(d Data, e evt) Data {
			d.Alive[control.Node(e)] = true
			return d
		})
		// A node that goes down takes its pending orders with it: they are
		// withdrawn, so a worker coming back does not start what has moved.
		sb.On(control.EvNodeDown).AssignEvent(func(d Data, e evt) Data {
			node := control.Node(e)
			d.Alive[node], d.Down = false, node
			d.dropOrders(node)
			return d
		}).RaiseWith(control.RaiseCancel, func(d Data) gstate.Params { return control.Cancel(d.Down) })
		// The node (re)started: what it had is gone; wait for its new report.
		sb.On(control.EvNodeReady).AssignEvent(func(d Data, e evt) Data {
			delete(d.Reports, control.Node(e))
			return d
		})
		sb.On(control.EvReport).AssignEvent(func(d Data, e evt) Data {
			r, err := control.Report[Report](e)
			if err != nil {
				logf("bad report from %s: %v", control.Node(e), err)
				return d
			}
			d.Reports[control.Node(e)] = r
			return d
		})
		sb.On(control.EvDepsReady).Assign(func(d Data) Data {
			d.DepsReady = true
			return d
		})
		sb.On(control.EvDepsLost).Assign(func(d Data) Data {
			d.DepsReady = false
			return d
		})
		sb.On(control.EvOrderDone).AssignEvent(func(d Data, e evt) Data {
			kind, node, slot, gen := orderOf(e.Params)
			delete(d.Pending, orderKey(kind, node, slot, gen))
			return d
		})
		// A run order that failed is a failure of the instance: the process
		// could not even start there.
		sb.On(control.EvOrderFailed).AssignEvent(func(d Data, e evt) Data {
			kind, node, slot, gen := orderOf(e.Params)
			delete(d.Pending, orderKey(kind, node, slot, gen))
			errText, _ := e.Params["error"].(string)
			if kind == OrderRun && slot < len(d.Instances) && d.Instances[slot].Node == node && d.Instances[slot].Gen == gen {
				logf("instance %d could not start on %s: %s; moving it", slot, node, errText)
				d.unplace(slot, "could not start on "+node+": "+errText, true)
			}
			return d
		})
	}
	// schedule acts on the current picture whenever it calls for anything.
	schedule := func(sb *builder) {
		sb.Always().
			Guard(func(d Data) bool { _, _, changed := decide(d, t, false); return changed }).
			Assign(func(d Data) Data {
				d, notes, _ := decide(d, t, false)
				for _, n := range notes {
					logf("%s", n)
				}
				return d
			}).
			RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(d.Orders...) })
	}

	def := gstate.New[string, string, Data](name).
		Initial(StateWaiting).
		State(StateWaiting, func(sb *builder) {
			track(sb)
			// Placing waits for every eligible worker to report, so the
			// instances spread instead of landing on whichever came first...
			sb.Always().Guard(func(d Data) bool { return d.DepsReady && d.allReported(t) }).GoTo(StateDegraded)
			// ...or, after a while, goes ahead with the ones that did.
			sb.After(settleTimeout).Raise("settle")
			sb.On("settle").Guard(func(d Data) bool { return d.DepsReady }).GoTo(StateDegraded)
			sb.On("settle").GoTo(StateWaiting)
		}).
		State(StateDegraded, func(sb *builder) {
			track(sb)
			schedule(sb)
			sb.Always().Guard(Data.healthy).
				Assign(func(d Data) Data {
					logf("all %d instance(s) running", len(d.Instances))
					return d
				}).
				GoTo(StateHealthy)
			// Instances that failed are placed again on the tick; re-entering
			// the state starts the next one.
			sb.After(retryDelay).Raise("retry")
			sb.On("retry").
				Assign(func(d Data) Data {
					if !d.needsRetry() {
						d.Orders = nil // nothing to raise again
						return d
					}
					d, notes, _ := decide(d, t, true)
					for _, n := range notes {
						logf("%s", n)
					}
					return d
				}).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(d.Orders...) }).
				GoTo(StateDegraded)
		}).
		State(StateHealthy, func(sb *builder) {
			track(sb)
			schedule(sb)
			sb.Always().Guard(func(d Data) bool { return !d.healthy() }).GoTo(StateDegraded)
		}).
		Build()

	return control.NewMachine(name, def, newData(t), requires...)
}
