package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"time"
)

// SubmitFunc sends an event towards the raft log, from any node.
type SubmitFunc func(ctx context.Context, ev Event) error

// View is the replicated state an agent works from. On a server node it is
// the local Runtime; on a worker-only node, a RemoteView of a server's.
type View interface {
	// Boot returns the boot ID last announced by node's agent, or "".
	Boot(node string) string
	// Started reports whether the controller has been started.
	Started(controller string) bool
	// Report returns the last replicated report of controller for node.
	Report(controller, node string) json.RawMessage
	// PendingOrders returns the pending orders of controller for node,
	// oldest first.
	PendingOrders(controller, node string) []Order
}

// Agent is the node-local side of the control layer. For each worker it keeps
// two things converging, by comparing against the local replica instead of
// reacting to messages:
//
//   - the replicated report of this node equals the worker's latest probe;
//   - no order addressed to this node stays pending.
//
// Because it works from state, nothing is lost on a leader change, a restart
// or a re-bootstrap: whatever is missing is simply sent again.
//
// Working from state is only safe once the local replica is current. A node
// that just (re)started replays the log, and mid-replay it sees orders as
// pending whose results come later in the same log. So the agent first commits
// an EvNodeReady event carrying a boot ID unique to this start, and stays idle
// until it sees that ID in its own replica. The event is appended after
// everything committed before it, so having applied it proves the replica has
// applied all of that too.
type Agent struct {
	node     string
	boot     string
	rt       View
	submit   SubmitFunc
	workers  []Worker
	interval time.Duration
}

func NewAgent(node string, rt View, submit SubmitFunc, workers ...Worker) *Agent {
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	return &Agent{
		node:     node,
		boot:     hex.EncodeToString(id),
		rt:       rt,
		submit:   submit,
		workers:  workers,
		interval: 500 * time.Millisecond,
	}
}

// Run starts the announce loop and one loop per worker, and returns
// immediately.
func (a *Agent) Run(ctx context.Context) {
	go a.announce(ctx)
	for _, w := range a.workers {
		go a.loop(ctx, w)
	}
}

// caughtUp reports whether this start's EvNodeReady has been applied locally.
func (a *Agent) caughtUp() bool {
	return a.rt.Boot(a.node) == a.boot
}

// announce keeps submitting EvNodeReady until it is applied locally. It keeps
// watching afterwards, so the agent re-announces if the replicated state is
// ever rebuilt without it.
func (a *Agent) announce(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	wasReady := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ready := a.caughtUp()
		if ready && !wasReady {
			log.Printf("Agent caught up with the log (boot %s); executing orders", a.boot)
		}
		wasReady = ready
		if !ready {
			_ = a.submit(ctx, Event{Name: EvNodeReady, Params: map[string]any{"node": a.node, "boot": a.boot}})
		}
	}
}

func (a *Agent) loop(ctx context.Context, w Worker) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	// outcomes remembers the result of executed orders until the replica shows
	// them as no longer pending, so a lost result is re-sent, not re-executed.
	outcomes := map[uint64]string{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// A worker does nothing, not even probe, until its controller has
		// been started: probing can be invasive (it may wipe local data).
		if !a.caughtUp() || !a.rt.Started(w.Name()) {
			continue
		}
		a.report(ctx, w)
		a.work(ctx, w, outcomes)
	}
}

func (a *Agent) report(ctx context.Context, w Worker) {
	value, err := w.Probe(ctx)
	if errors.Is(err, ErrNotReady) {
		return
	}
	if err != nil {
		log.Printf("[%s] probe: %v", w.Name(), err)
		return
	}
	// Normalize through JSON so the comparison matches what the runtime
	// stores after the event has travelled through the log.
	raw, err := json.Marshal(value)
	if err != nil {
		log.Printf("[%s] probe result not serializable: %v", w.Name(), err)
		return
	}
	var report any
	if err := json.Unmarshal(raw, &report); err != nil {
		return
	}
	canonical, _ := json.Marshal(report)
	if bytes.Equal(canonical, a.rt.Report(w.Name(), a.node)) {
		return
	}
	// Errors are not logged: without a leader this fails every tick, and the
	// next tick retries anyway.
	_ = a.submit(ctx, Event{
		Target: w.Name(),
		Name:   EvReport,
		Params: map[string]any{"node": a.node, "report": report},
	})
}

func (a *Agent) work(ctx context.Context, w Worker, outcomes map[uint64]string) {
	pending := a.rt.PendingOrders(w.Name(), a.node)

	still := make(map[uint64]bool, len(pending))
	for _, o := range pending {
		still[o.ID] = true
	}
	for id := range outcomes {
		if !still[id] {
			delete(outcomes, id)
		}
	}

	for _, o := range pending {
		outcome, executed := outcomes[o.ID]
		if !executed {
			log.Printf("[%s] executing order #%d: %s", w.Name(), o.ID, o.Kind)
			if err := w.Execute(ctx, o); err != nil {
				log.Printf("[%s] order #%d failed: %v", w.Name(), o.ID, err)
				outcome = err.Error()
				if outcome == "" {
					outcome = "failed"
				}
			}
			outcomes[o.ID] = outcome
			// Report what the order changed before reporting its outcome, so
			// the controller never sees "done" with the state from before.
			a.report(ctx, w)
		}

		ev := Event{Name: EvOrderDone, Params: map[string]any{"id": o.ID, "node": a.node}}
		if outcome != "" {
			ev.Name = EvOrderFailed
			ev.Params["error"] = outcome
		}
		_ = a.submit(ctx, ev)
	}
}
