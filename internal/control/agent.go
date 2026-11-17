package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
)

// SubmitFunc sends an event towards the raft log, from any node.
type SubmitFunc func(ctx context.Context, ev Event) error

// View is the replicated state an agent works from: the node's local Runtime,
// which every node has, voter or not. Tests substitute their own.
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
	wg       sync.WaitGroup
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
// immediately. They run until ctx ends; Wait waits for them.
func (a *Agent) Run(ctx context.Context) {
	a.wg.Go(func() { a.announce(ctx) })
	for _, w := range a.workers {
		a.wg.Go(func() { a.loop(ctx, w) })
	}
}

// Wait returns once every loop has returned after ctx ended, including any
// order or probe that was in progress.
func (a *Agent) Wait() { a.wg.Wait() }

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
	outcomes := map[uint64]outcome{}
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

// outcome is how one execution of an order ended.
type outcome struct {
	err     string // "" when it succeeded
	result  any
	started time.Time
	took    time.Duration
}

// event is the order result event for o, re-sent unchanged until committed.
func (oc outcome) event(node string, id uint64) Event {
	ev := Event{Name: EvOrderDone, Params: map[string]any{
		"id":          id,
		"node":        node,
		"started_ms":  oc.started.UnixMilli(),
		"duration_ms": oc.took.Milliseconds(),
	}}
	if oc.result != nil {
		ev.Params["result"] = oc.result
	}
	if oc.err != "" {
		ev.Name = EvOrderFailed
		ev.Params["error"] = oc.err
	}
	return ev
}

// maxResult bounds an order result as stored in the replicated history.
const maxResult = 4 << 10

// normalize makes an order result fit for the log: JSON-decoded, so the
// controller sees what it would after replay, and replaced by a note when it
// cannot be serialized or is too big.
func normalize(worker string, id uint64, result any) any {
	if result == nil {
		return nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		log.Printf("[%s] order #%d result not serializable: %v", worker, id, err)
		return map[string]any{"error": "result not serializable: " + err.Error()}
	}
	if len(raw) > maxResult {
		return map[string]any{"truncated": true, "bytes": len(raw), "head": string(raw[:maxResult/4])}
	}
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

// execute runs o on w, through ExecuteResult when w offers it.
func execute(ctx context.Context, w Worker, o Order) (any, error) {
	if rw, ok := w.(ResultWorker); ok {
		return rw.ExecuteResult(ctx, o)
	}
	return nil, w.Execute(ctx, o)
}

func (a *Agent) work(ctx context.Context, w Worker, outcomes map[uint64]outcome) {
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
		oc, executed := outcomes[o.ID]
		if !executed {
			log.Printf("[%s] executing order #%d: %s", w.Name(), o.ID, o.Kind)
			oc.started = time.Now()
			result, err := execute(ctx, w, o)
			oc.took = time.Since(oc.started)
			if ctx.Err() != nil {
				// Interrupted by the node shutting down, which says nothing
				// about the order: it stays pending and runs again after the
				// restart (orders are idempotent).
				log.Printf("[%s] order #%d interrupted by shutdown", w.Name(), o.ID)
				return
			}
			if err != nil {
				log.Printf("[%s] order #%d failed: %v", w.Name(), o.ID, err)
				oc.err = err.Error()
				if oc.err == "" {
					oc.err = "failed"
				}
			}
			oc.result = normalize(w.Name(), o.ID, result)
			outcomes[o.ID] = oc
			// Report what the order changed before reporting its outcome, so
			// the controller never sees "done" with the state from before.
			a.report(ctx, w)
		}
		_ = a.submit(ctx, oc.event(a.node, o.ID))
	}
}
