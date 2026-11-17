package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWorker struct {
	executed atomic.Int64
}

func (w *fakeWorker) Name() string                       { return "svc" }
func (w *fakeWorker) Probe(context.Context) (any, error) { return map[string]any{}, nil }
func (w *fakeWorker) Execute(context.Context, Order) error {
	w.executed.Add(1)
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A restarted node replays the log. Mid-replay it sees an order as pending
// although its result is further down the same log. The agent must not execute
// it: it has to wait until its own EvNodeReady is applied.
func TestAgentDoesNotExecuteDuringCatchUp(t *testing.T) {
	rt := NewRuntime()
	// Replay has reached the order but not yet its result.
	rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "start"}})

	// While "replaying", submitted events are accepted but not applied yet,
	// like an entry sitting in the log behind the ones still being replayed.
	var live atomic.Bool
	submit := func(_ context.Context, ev Event) error {
		if live.Load() {
			rt.Apply(ev)
		}
		return nil
	}

	w := &fakeWorker{}
	a := NewAgent("n1", rt, submit, w)
	a.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	if n := w.executed.Load(); n != 0 {
		t.Fatalf("order executed %d time(s) during catch-up", n)
	}

	// Replay continues: the result from before the restart is applied, and
	// only then the agent's own ready event.
	rt.Apply(Event{Name: EvOrderDone, Params: map[string]any{"id": uint64(1), "node": "n1"}})
	live.Store(true)
	waitFor(t, "agent to catch up", a.caughtUp)

	time.Sleep(50 * time.Millisecond)
	if n := w.executed.Load(); n != 0 {
		t.Fatalf("already completed order executed %d time(s) after catch-up", n)
	}

	// Orders placed from here on are executed, exactly once.
	rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "start"}})
	waitFor(t, "new order to execute", func() bool { return w.executed.Load() == 1 })
	waitFor(t, "order to leave the store", func() bool { return len(rt.PendingOrders("svc", "n1")) == 0 })
	if n := w.executed.Load(); n != 1 {
		t.Fatalf("new order executed %d times, want 1", n)
	}
}

type probeCounter struct {
	fakeWorker
	name   string
	probes atomic.Int64
}

func (w *probeCounter) Name() string { return w.name }
func (w *probeCounter) Probe(context.Context) (any, error) {
	w.probes.Add(1)
	return map[string]any{}, nil
}

// The worker of a controller that has not been started must stay completely
// idle: a probe can be invasive (Kafka's wipes the log directory).
func TestAgentKeepsWorkerIdleUntilControllerStarted(t *testing.T) {
	rt := NewRuntime()
	a := fake("a")
	rt.Register(a)
	rt.Register(fake("b", Requirement{Controller: "a", States: []string{"up"}}))

	wa, wb := &probeCounter{name: "a"}, &probeCounter{name: "b"}
	submit := func(_ context.Context, ev Event) error { rt.Apply(ev); return nil }
	agent := NewAgent("n1", rt, submit, wa, wb)
	agent.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent.Run(ctx)

	waitFor(t, "worker a to probe", func() bool { return wa.probes.Load() > 0 })
	time.Sleep(50 * time.Millisecond)
	if n := wb.probes.Load(); n != 0 {
		t.Fatalf("worker b probed %d time(s) before its controller started", n)
	}

	goTo(rt, "a", "up")
	waitFor(t, "worker b to probe", func() bool { return wb.probes.Load() > 0 })
}

// blockingWorker's orders run until their context ends.
type blockingWorker struct {
	fakeWorker
	started chan struct{}
}

func (w *blockingWorker) Execute(ctx context.Context, _ Order) error {
	close(w.started)
	<-ctx.Done()
	return ctx.Err()
}

// An order cut short by the node shutting down is not a failure: no result is
// sent, so it stays pending and runs again after the restart. Wait returns
// once the interrupted order has returned.
func TestAgentShutdownLeavesInterruptedOrderPending(t *testing.T) {
	rt := NewRuntime()
	var failed atomic.Int64
	submit := func(_ context.Context, ev Event) error {
		if ev.Name == EvOrderFailed {
			failed.Add(1)
		}
		rt.Apply(ev)
		return nil
	}
	w := &blockingWorker{started: make(chan struct{})}
	a := NewAgent("n1", rt, submit, w)
	a.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	a.Run(ctx)
	waitFor(t, "agent to catch up", a.caughtUp)

	rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "start"}})
	<-w.started
	cancel()

	done := make(chan struct{})
	go func() {
		a.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after the context ended")
	}
	if n := failed.Load(); n != 0 {
		t.Fatalf("interrupted order reported as failed %d time(s)", n)
	}
	if n := len(rt.PendingOrders("svc", "n1")); n != 1 {
		t.Fatalf("%d pending order(s) after shutdown, want the interrupted one", n)
	}
}
