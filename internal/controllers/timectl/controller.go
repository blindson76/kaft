package timectl

import (
	"log"
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
	// collectTimeout is how long bootstrap waits for missing reports and for
	// an external source before it settles for internal mode.
	collectTimeout = 20 * time.Second
	// configTimeout bounds the wait for all nodes to apply a configuration.
	configTimeout = 60 * time.Second
	// retryInterval is the pause before a failed node is ordered again.
	retryInterval = 15 * time.Second
)

// NewController builds the time controller.
//
//		collecting ─▶ configuring ─▶ auto ─────▶ orphan
//		                   ▲  │        ▲            │
//		                   │  └─▶ internal ─────────┘ (operator)
//		                   └── every change of configuration
//
//	  - collecting: wait for the reports, then pick the mode (decide).
//	  - configuring: a new configuration is being handed to the nodes. It leads
//	    to the state named after the mode once all of them have applied it.
//	  - auto: the nodes follow the external source. Losing it, or seeing it
//	    jump, leads to orphan.
//	  - orphan: the nodes hold each other's time and will bootstrap in internal
//	    mode. Only an operator goes back to auto.
//	  - internal: the nodes follow the master. A lost master is replaced; an
//	    operator can set the time, which takes effect at the next bootstrap.
//
// In the three steady states a node that restarts or joins late is simply
// given the current configuration.
//
// requires lists dependencies on other controllers; the runtime does not start
// this controller, nor its workers, until they hold.
func NewController(t Topology, requires ...control.Requirement) control.Controller {
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
				log.Printf("[time] bad report from %s: %v", control.Node(e), err)
				return d
			}
			d.Reports[control.Node(e)] = r
			return d
		})
	}
	// current tells whether an order result belongs to the epoch in force.
	current := func(d Data, e evt) bool {
		args, _ := e.Params["args"].(map[string]any)
		return uint64(num(args["epoch"])) == d.Epoch && d.Await[control.Node(e)]
	}
	// results handles order outcomes. A failure re-enters self, when given,
	// which restarts that state's retry timer.
	results := func(sb *builder, self string) {
		sb.On(control.EvOrderDone).GuardEvent(current).AssignEvent(func(d Data, e evt) Data {
			delete(d.Await, control.Node(e))
			return d
		})
		failed := sb.On(control.EvOrderFailed).GuardEvent(current).AssignEvent(func(d Data, e evt) Data {
			node := control.Node(e)
			log.Printf("[time] configuring %s failed: %v", node, e.Params["error"])
			delete(d.Await, node)
			if d.Failed == nil {
				d.Failed = map[string]bool{}
			}
			d.Failed[node] = true
			return d
		})
		if self != "" {
			failed.GoTo(self)
		}
	}
	// change puts a new configuration in force: orders of the old one are
	// withdrawn, and configuring hands out the new one.
	change := func(tr *transition, next func(Data, evt) Data) {
		tr.AssignEvent(func(d Data, e evt) Data {
			d = next(d, e)
			log.Printf("[time] epoch %d: mode=%s master=%q delta=%dms shift=%dms (%s)",
				d.Epoch, d.Mode, d.Master, d.Delta, d.Shift, d.Reason)
			return d
		}).
			Raise(control.RaiseCancel, control.Cancel("")).
			GoTo("configuring")
	}
	// catchUp adopts the epoch of a node that is ahead of the controller.
	catchUp := func(sb *builder) {
		change(sb.Always().Guard(Data.newer), func(d Data, _ evt) Data {
			d.Epoch, _ = d.newest()
			d = d.next()
			d.Reason = "a node reported a later epoch"
			return d
		})
	}
	// rollout orders every node that is behind the current epoch.
	rollout := func(sb *builder) {
		sb.Always().Guard(func(d Data) bool { return len(d.todo()) > 0 }).
			Assign(func(d Data) Data {
				d.Batch = d.todo()
				if d.Await == nil {
					d.Await = map[string]bool{}
				}
				for _, node := range d.Batch {
					d.Await[node] = true
				}
				return d
			}).
			RaiseWith(control.RaiseOrder, func(d Data) gstate.Params { return control.Orders(d.orders(t, d.Batch)...) })
	}
	// setTime records a manual correction. It is not applied to the clocks
	// now: every node stores it, and it is applied at the next bootstrap.
	setTime := func(sb *builder) {
		change(sb.On(EvSetTime).Guard(func(d Data) bool { return d.Mode == ModeInternal }), func(d Data, e evt) Data {
			d = d.next()
			d.Delta = num(e.Params["now_ms"]) - num(e.Params["time_ms"])
			d.Reason = "time set by operator"
			return d
		})
	}
	useAuto := func(sb *builder) {
		change(sb.On(EvUseAuto).Guard(func(d Data) bool { return d.Mode != ModeAuto && d.external() }), func(d Data, _ evt) Data {
			d = d.next()
			d.Mode, d.Master, d.Delta, d.Reason = ModeAuto, "", 0, "operator switched to auto"
			return d
		})
	}
	replaceMaster := func(sb *builder) {
		lost := func(d Data) bool {
			_, ok := d.newMaster()
			return d.Mode == ModeInternal && !d.Alive[d.Master] && ok
		}
		change(sb.Always().Guard(lost), func(d Data, _ evt) Data {
			was := d.Master
			d.Master, _ = d.newMaster()
			d = d.next()
			d.Reason = "master " + was + " lost"
			return d
		})
	}
	// steady is what the three steady states share.
	steady := func(sb *builder, self string, specific func()) {
		track(sb)
		results(sb, self)
		catchUp(sb)
		specific()
		rollout(sb)
		sb.After(retryInterval).Raise("retry")
		sb.On("retry").Guard(func(d Data) bool { return len(d.Failed) > 0 }).
			Assign(func(d Data) Data {
				d.Failed = nil
				return d
			}).
			GoTo(self)
	}
	modes := []string{ModeAuto, ModeOrphan, ModeInternal}

	def := gstate.New[string, string, Data](Name).
		Initial("collecting").
		State("collecting", func(sb *builder) {
			track(sb)
			pick := func(force bool) (func(Data) bool, func(Data, evt) Data) {
				return func(d Data) bool {
						_, ok := decide(d, force)
						return ok && (force || d.allReported())
					}, func(d Data, _ evt) Data {
						d, _ = decide(d, force)
						return d
					}
			}
			// Decide as soon as every alive node has reported and the answer
			// is clear...
			guard, next := pick(false)
			change(sb.Always().Guard(guard), next)
			// ...or, after a while, with what there is.
			sb.After(collectTimeout).Raise("collect.timeout")
			guard, next = pick(true)
			change(sb.On("collect.timeout").Guard(guard), next)
			sb.On("collect.timeout").GoTo("collecting")
		}).
		State("configuring", func(sb *builder) {
			track(sb)
			results(sb, "")
			catchUp(sb)
			replaceMaster(sb)
			setTime(sb)
			rollout(sb)
			for _, mode := range modes {
				sb.Always().Guard(func(d Data) bool { return d.Mode == mode && d.settled() }).
					Assign(func(d Data) Data {
						log.Printf("[time] %s mode in effect (epoch %d)", d.Mode, d.Epoch)
						return d
					}).
					GoTo(mode)
			}
			// Nodes that did not make it in time are followed up from the
			// steady state.
			sb.After(configTimeout).Raise("config.timeout")
			for _, mode := range modes {
				sb.On("config.timeout").Guard(func(d Data) bool { return d.Mode == mode }).GoTo(mode)
			}
		}).
		State(ModeAuto, func(sb *builder) {
			steady(sb, ModeAuto, func() {
				change(sb.Always().Guard(func(d Data) bool { return d.jump() != "" || !d.external() }), func(d Data, _ evt) Data {
					reason := "external time source lost"
					if j := d.jump(); j != "" {
						reason = "external time source jumped (" + j + ")"
					}
					d = d.next()
					d.Mode, d.Reason = ModeOrphan, reason
					return d
				})
			})
		}).
		State(ModeOrphan, func(sb *builder) {
			steady(sb, ModeOrphan, func() { useAuto(sb) })
		}).
		State(ModeInternal, func(sb *builder) {
			steady(sb, ModeInternal, func() {
				replaceMaster(sb)
				setTime(sb)
				useAuto(sb)
			})
		}).
		Build()

	return control.NewMachine(Name, def, Data{Alive: map[string]bool{}, Reports: map[string]Report{}}, requires...)
}
