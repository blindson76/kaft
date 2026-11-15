package kafkactl

import (
	"log"
	"maps"
	"slices"
	"time"

	"example.com/dstate/internal/control"
	"github.com/blindson76/gstate"
)

type (
	builder    = gstate.StateBuilder[string, string, Data]
	transition = gstate.TransitionBuilder[string, string, Data]
	evt        = gstate.Event[string]
)

// Timeouts run on the leader only and restart on a leader change, so they are
// lower bounds.
const (
	collectTimeout = 20 * time.Second
	stepTimeout    = 5 * time.Minute
	resetTimeout   = 90 * time.Second
)

func orderKey(kind, node string) string { return kind + "@" + node }

// await makes the controller wait for the given orders.
func await(d Data, orders []control.OrderSpec) Data {
	d.Failed = false
	d.Await = map[string]bool{}
	for _, o := range orders {
		d.Await[orderKey(o.Kind, o.Node)] = true
	}
	return d
}

func stepDone(d Data) bool   { return len(d.Await) == 0 && !d.Failed }
func stepFailed(d Data) bool { return len(d.Await) == 0 && d.Failed }

// NewController builds the kafka controller.
//
//		collecting ─▶ executing ─▶ monitoring
//		     ▲            │  ▲          │
//		     └─ resetting ◀──┴──────────┘
//
//	  - collecting: wait for the probe reports, then pick the nodes (formPlan).
//	  - executing: carry out the plan one step at a time.
//	  - monitoring: steady state; any member that stops being healthy leads to a
//	    repair plan.
//	  - resetting: formation failed or the quorum is lost. Stop Kafka
//	    everywhere, which wipes and formats again, and start over.
func NewController(t Topology) control.Controller {
	track := func(sb *builder) {
		sb.On(control.EvNodeUp).AssignEvent(func(d Data, e evt) Data {
			d.Alive[control.Node(e)] = true
			return d
		})
		sb.On(control.EvNodeDown).AssignEvent(func(d Data, e evt) Data {
			d.Alive[control.Node(e)] = false
			return d
		})
		sb.On(control.EvReport).AssignEvent(func(d Data, e evt) Data {
			r, err := control.Report[Report](e)
			if err != nil {
				log.Printf("[kafka] bad report from %s: %v", control.Node(e), err)
				return d
			}
			d.Reports[control.Node(e)] = r
			return d
		})
	}
	results := func(sb *builder) {
		sb.On(control.EvOrderDone).AssignEvent(func(d Data, e evt) Data {
			kind, _ := e.Params["kind"].(string)
			delete(d.Await, orderKey(kind, control.Node(e)))
			return d
		})
		sb.On(control.EvOrderFailed).AssignEvent(func(d Data, e evt) Data {
			kind, _ := e.Params["kind"].(string)
			key := orderKey(kind, control.Node(e))
			if d.Await[key] {
				log.Printf("[kafka] %s failed: %v", key, e.Params["error"])
				delete(d.Await, key)
				d.Failed = true
			}
			return d
		})
	}
	// begin stores a plan; executing then works through its steps.
	begin := func(tr *transition, plan func(Data) (Plan, bool)) {
		tr.Guard(func(d Data) bool { _, ok := plan(d); return ok }).
			Assign(func(d Data) Data {
				d.Plan, _ = plan(d)
				log.Printf("[kafka] plan: members=%v steps=%d", d.Plan.Members, len(d.Plan.Steps))
				return await(d, nil)
			}).
			GoTo("executing")
	}
	reset := func(tr *transition, why string) {
		stops := func(d Data) []control.OrderSpec {
			var out []control.OrderSpec
			for _, n := range slices.Sorted(maps.Keys(d.Alive)) {
				if d.Alive[n] {
					out = append(out, control.OrderSpec{Node: n, Kind: OrderStop})
				}
			}
			return out
		}
		tr.Assign(func(d Data) Data {
			log.Printf("[kafka] resetting: %s", why)
			d.Plan, d.Members, d.Voters = Plan{}, nil, nil
			// Planning waits for the reports the stop orders produce.
			d.Reports = map[string]Report{}
			return await(d, stops(d))
		}).
			Raise(control.RaiseCancel, control.Cancel("")).
			RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(stops(d)...) }).
			GoTo("resetting")
	}
	// fail handles a step that did not work out. During formation everything
	// is reset. An established cluster is left running: the plan is dropped
	// and monitoring decides again from the current state.
	fail := func(on func() *transition, cond func(Data) bool, why string) {
		on().Guard(func(d Data) bool { return cond(d) && len(d.Members) > 0 }).
			Assign(func(d Data) Data {
				log.Printf("[kafka] plan abandoned: %s", why)
				d.Plan, d.Await, d.Failed = Plan{}, nil, false
				return d
			}).
			Raise(control.RaiseCancel, control.Cancel("")).
			GoTo("monitoring")
		reset(on().Guard(cond), why)
	}

	form := func(d Data) (Plan, bool) { return formPlan(d, t) }

	def := gstate.New[string, string, Data](Name).
		Initial("collecting").
		State("collecting", func(sb *builder) {
			track(sb)
			// Decide as soon as every alive node has reported...
			begin(sb.Always(), func(d Data) (Plan, bool) {
				if !d.allReported() {
					return Plan{}, false
				}
				return form(d)
			})
			// ...or, after a while, with the ones that did.
			sb.After(collectTimeout).Raise("collect.timeout")
			begin(sb.On("collect.timeout"), form)
			sb.On("collect.timeout").GoTo("collecting")
		}).
		State("executing", func(sb *builder) {
			track(sb)
			results(sb)
			// Next step: re-entering the state also restarts the step timer.
			sb.Always().Guard(func(d Data) bool { return stepDone(d) && len(d.Plan.Steps) > 0 }).
				Assign(func(d Data) Data {
					d.Plan.Current, d.Plan.Steps = d.Plan.Steps[0], d.Plan.Steps[1:]
					return await(d, d.Plan.Current)
				}).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(d.Plan.Current...) }).
				GoTo("executing")
			// No steps left: the plan's member set is now the cluster.
			sb.Always().Guard(stepDone).
				Assign(func(d Data) Data {
					log.Printf("[kafka] cluster established: %v", d.Plan.Members)
					d.Members, d.Plan = d.Plan.Members, Plan{}
					d.Voters = map[string]string{}
					for _, n := range d.Members {
						d.Voters[n] = d.Reports[n].DirID
					}
					return d
				}).
				GoTo("monitoring")
			fail(sb.Always, stepFailed, "step failed")
			sb.After(stepTimeout).Raise("step.timeout")
			fail(func() *transition { return sb.On("step.timeout") }, func(Data) bool { return true }, "step timed out")
		}).
		State("monitoring", func(sb *builder) {
			track(sb)
			reset(sb.Always().Guard(Data.quorumLost), "controller quorum lost")
			begin(sb.Always(), func(d Data) (Plan, bool) { return repairPlan(d, t) })
		}).
		State("resetting", func(sb *builder) {
			track(sb)
			results(sb)
			done := func(d Data) bool { return len(d.Await) == 0 }
			sb.Always().Guard(done).Raise(control.RaiseCancel, control.Cancel("")).GoTo("collecting")
			sb.After(resetTimeout).Raise("reset.timeout")
			sb.On("reset.timeout").Raise(control.RaiseCancel, control.Cancel("")).GoTo("collecting")
		}).
		Build()

	return control.NewMachine(Name, def, Data{Alive: map[string]bool{}, Reports: map[string]Report{}})
}
