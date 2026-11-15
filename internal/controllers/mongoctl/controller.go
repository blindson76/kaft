package mongoctl

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

// Step timeouts. They run on the leader only and restart on a leader change,
// so they are lower bounds.
const (
	collectTimeout   = 20 * time.Second
	prepareTimeout   = 3 * time.Minute
	startTimeout     = 3 * time.Minute
	electTimeout     = 90 * time.Second
	configureTimeout = 6 * time.Minute
	resetTimeout     = 90 * time.Second
)

func orderKey(kind, node string) string { return kind + "@" + node }

func specs(kind string, nodes []string) []control.OrderSpec {
	out := make([]control.OrderSpec, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, control.OrderSpec{Node: n, Kind: kind})
	}
	return out
}

// await makes the current step wait for the given orders.
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

// NewController builds the mongo controller.
//
//		collecting ─▶ preparing ─▶ starting ─▶ (electing ─▶) configuring ─▶ monitoring
//		     ▲            │            │            │              │            │
//		     └─ resetting ◀────────────┴────────────┴──────────────┴────────────┘
//
//	  - collecting: wait for the probe reports, then decide (formPlan).
//	  - preparing: wipe the nodes the plan says cannot join as they are.
//	  - starting: start mongod on the nodes of the plan.
//	  - electing: wait for the existing replica set to elect a primary.
//	  - configuring: initiate / reconfigure to the planned member set.
//	  - monitoring: steady state; replaces dead members, restarts returning ones.
//	  - resetting: something failed or the set lost its majority. Stop every
//	    mongod, which probes the data again, and decide from scratch.
//
// requires lists dependencies on other controllers. The runtime does not start
// this controller, nor its workers, until they hold. If one is lost after
// that, an established cluster keeps running and is still repaired; only
// forming a new one waits for the dependency to come back.
func NewController(t Topology, requires ...control.Requirement) control.Controller {
	hosts := func(nodes []string) []string {
		out := make([]string, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, t.Addrs[n])
		}
		return out
	}
	configureOrder := func(d Data) control.OrderSpec {
		p := d.Plan
		args := map[string]any{"members": hosts(p.Members)}
		switch p.Mode {
		case ModeInitiate:
			return control.OrderSpec{Node: p.Target, Kind: OrderInitiate, Args: args}
		case ModeForce:
			args["force"] = true
			return control.OrderSpec{Node: p.Target, Kind: OrderReconfig, Args: args}
		default: // adopt, reconfig: through the primary
			return control.OrderSpec{Node: d.primary(p.Members), Kind: OrderReconfig, Args: args}
		}
	}

	// track keeps liveness and reports current in every state.
	track := func(sb *builder) {
		sb.On(control.EvNodeUp).AssignEvent(func(d Data, e evt) Data {
			d.Alive[control.Node(e)] = true
			return d
		})
		sb.On(control.EvNodeDown).AssignEvent(func(d Data, e evt) Data {
			d.Alive[control.Node(e)] = false
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
		sb.On(control.EvReport).AssignEvent(func(d Data, e evt) Data {
			r, err := control.Report[Report](e)
			if err != nil {
				log.Printf("[mongo] bad report from %s: %v", control.Node(e), err)
				return d
			}
			d.Reports[control.Node(e)] = r
			return d
		})
	}
	// results ticks off the awaited orders as their outcomes arrive.
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
				log.Printf("[mongo] %s failed: %v", key, e.Params["error"])
				delete(d.Await, key)
				d.Failed = true
			}
			return d
		})
	}
	// reset abandons everything in flight and stops every mongod.
	reset := func(tr *transition, why string) {
		stops := func(d Data) []control.OrderSpec {
			var alive []string
			for _, n := range slices.Sorted(maps.Keys(d.Alive)) {
				if d.Alive[n] {
					alive = append(alive, n)
				}
			}
			return specs(OrderStop, alive)
		}
		tr.Assign(func(d Data) Data {
			log.Printf("[mongo] resetting: %s", why)
			d.Plan, d.Members = Plan{}, nil
			// Old reports describe data that has moved on since; planning
			// waits for the ones the stop orders produce.
			d.Reports = map[string]Report{}
			return await(d, stops(d))
		}).
			Raise(control.RaiseCancel, control.Cancel("")).
			RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(stops(d)...) }).
			GoTo("resetting")
	}
	// adoptPlan stores a plan and starts its first step, the wipes.
	adoptPlan := func(tr *transition, plan func(Data) (Plan, bool)) {
		tr.Guard(func(d Data) bool { _, ok := plan(d); return ok }).
			Assign(func(d Data) Data {
				d.Plan, _ = plan(d)
				log.Printf("[mongo] plan: %+v", d.Plan)
				return await(d, specs(OrderWipe, d.Plan.Wipe))
			}).
			RaiseWith(control.RaiseOrder, func(d Data) gstate.Params {
				orders := append(specs(OrderWipe, d.Plan.Wipe), specs(OrderStop, d.Plan.Stop)...)
				return control.Orders(orders...)
			}).
			GoTo("preparing")
	}
	// fail handles a step that did not work out. While forming the replica
	// set everything is reset. Once a set is established it is left running:
	// the plan is dropped and monitoring decides again.
	fail := func(on func() *transition, cond func(Data) bool, why string) {
		on().Guard(func(d Data) bool { return cond(d) && len(d.Members) > 0 }).
			Assign(func(d Data) Data {
				log.Printf("[mongo] plan abandoned: %s", why)
				d.Plan, d.Await, d.Failed = Plan{}, nil, false
				return d
			}).
			Raise(control.RaiseCancel, control.Cancel("")).
			GoTo("monitoring")
		reset(on().Guard(cond), why)
	}
	// timeout fails a step that takes longer than d.
	timeout := func(sb *builder, name string, d time.Duration) {
		sb.After(d).Raise(name)
		fail(func() *transition { return sb.On(name) }, func(Data) bool { return true }, name)
	}

	form := func(d Data) (Plan, bool) {
		if !d.DepsReady {
			return Plan{}, false
		}
		return formPlan(d, t)
	}

	def := gstate.New[string, string, Data](Name).
		Initial("collecting").
		State("collecting", func(sb *builder) {
			track(sb)
			// Decide as soon as every alive node has reported...
			adoptPlan(sb.Always(), func(d Data) (Plan, bool) {
				if !d.allReported() {
					return Plan{}, false
				}
				return form(d)
			})
			// ...or, after a while, with the ones that did.
			sb.After(collectTimeout).Raise("collect.timeout")
			adoptPlan(sb.On("collect.timeout"), form)
			sb.On("collect.timeout").GoTo("collecting")
		}).
		State("preparing", func(sb *builder) {
			track(sb)
			results(sb)
			sb.Always().Guard(stepDone).
				Assign(func(d Data) Data { return await(d, specs(OrderStart, d.Plan.Start)) }).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params {
					return control.Orders(specs(OrderStart, d.Plan.Start)...)
				}).
				GoTo("starting")
			fail(sb.Always, stepFailed, "wipe failed")
			timeout(sb, "prepare.timeout", prepareTimeout)
		}).
		State("starting", func(sb *builder) {
			track(sb)
			results(sb)
			direct := func(d Data) bool {
				return stepDone(d) && (d.Plan.Mode == ModeInitiate || d.Plan.Mode == ModeForce)
			}
			sb.Always().Guard(direct).
				Assign(func(d Data) Data { return await(d, []control.OrderSpec{configureOrder(d)}) }).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(configureOrder(d)) }).
				GoTo("configuring")
			sb.Always().Guard(func(d Data) bool { return stepDone(d) && d.Plan.Mode == ModeNone }).
				Assign(func(d Data) Data {
					d.Members, d.Plan = d.Plan.Members, Plan{}
					return d
				}).
				GoTo("monitoring")
			sb.Always().Guard(stepDone).GoTo("electing")
			fail(sb.Always, stepFailed, "start failed")
			timeout(sb, "start.timeout", startTimeout)
		}).
		State("electing", func(sb *builder) {
			track(sb)
			sb.Always().Guard(func(d Data) bool { return d.primary(d.Plan.Members) != "" }).
				Assign(func(d Data) Data { return await(d, []control.OrderSpec{configureOrder(d)}) }).
				RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(configureOrder(d)) }).
				GoTo("configuring")
			timeout(sb, "elect.timeout", electTimeout)
		}).
		State("configuring", func(sb *builder) {
			track(sb)
			results(sb)
			sb.Always().Guard(stepDone).
				Assign(func(d Data) Data {
					log.Printf("[mongo] replica set established: %v", d.Plan.Members)
					d.Members, d.Plan = d.Plan.Members, Plan{}
					return d
				}).
				GoTo("monitoring")
			fail(sb.Always, stepFailed, "configure failed")
			timeout(sb, "configure.timeout", configureTimeout)
		}).
		State("monitoring", func(sb *builder) {
			track(sb)
			reset(sb.Always().Guard(Data.quorumLost), "replica set lost its majority")
			adoptPlan(sb.Always(), func(d Data) (Plan, bool) { return repairPlan(d, t) })
			adoptPlan(sb.Always(), rejoinPlan)
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

	return control.NewMachine(Name, def, Data{Alive: map[string]bool{}, Reports: map[string]Report{}}, requires...)
}
