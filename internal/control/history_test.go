package control

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Finished, failed and cancelled orders land in the replicated history, with
// the commit times and the cause the log carries, and survive a snapshot.
func TestOrderHistory(t *testing.T) {
	rt := NewRuntime()
	rt.Register(fake("svc"))

	// Orders placed while an event is applied record its time and name.
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "n1"}, At: 1000})
	rt.applyMu.Lock()
	rt.now, rt.cause = 2000, "svc/go (n1)"
	rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "a"}, {Node: "n1", Kind: "b"}, {Node: "n2", Kind: "c"}})
	rt.applyMu.Unlock()

	rt.Apply(Event{Name: EvOrderDone, At: 3000, Params: map[string]any{
		"id": 1, "node": "n1", "started_ms": 2500, "duration_ms": 120, "result": map[string]any{"ok": true}}})
	rt.Apply(Event{Name: EvOrderFailed, At: 4000, Params: map[string]any{"id": 2, "node": "n1", "error": "boom"}})
	rt.Apply(Event{Name: EvOrderDone, At: 4500, Params: map[string]any{"id": 2, "node": "n1"}}) // duplicate: ignored
	rt.applyMu.Lock()
	rt.now = 5000
	rt.CancelOrders("svc", "")
	rt.applyMu.Unlock()

	pending, history := rt.OrderLog()
	if len(pending) != 0 {
		t.Fatalf("pending %+v", pending)
	}
	if len(history) != 3 {
		t.Fatalf("history has %d records: %+v", len(history), history)
	}
	done, failed, cancelled := history[0], history[1], history[2]
	if done.ID != 1 || done.Status != OrderDone || done.Placed != 2000 || done.Cause != "svc/go (n1)" ||
		done.Finished != 3000 || done.Started != 2500 || done.DurationMs != 120 || string(done.Result) != `{"ok":true}` {
		t.Errorf("done record %+v", done)
	}
	if failed.ID != 2 || failed.Status != OrderFailed || failed.Error != "boom" || failed.Finished != 4000 {
		t.Errorf("failed record %+v", failed)
	}
	if cancelled.ID != 3 || cancelled.Status != OrderCancelled || cancelled.Finished != 5000 {
		t.Errorf("cancelled record %+v", cancelled)
	}

	snap, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	other := NewRuntime()
	other.Register(fake("svc"))
	if err := other.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, h := other.OrderLog(); len(h) != 3 || h[0].Cause != "svc/go (n1)" {
		t.Errorf("restored history %+v", h)
	}
}

func TestOrderHistoryIsBounded(t *testing.T) {
	rt := NewRuntime()
	rt.Register(fake("svc"))
	for range historyLimit + 20 {
		rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "k"}})
	}
	rt.CancelOrders("svc", "")
	_, h := rt.OrderLog()
	if len(h) != historyLimit || h[0].ID != 21 || h[len(h)-1].ID != historyLimit+20 {
		t.Fatalf("history: %d records, first #%d, last #%d", len(h), h[0].ID, h[len(h)-1].ID)
	}
}

type resultWorker struct{ fakeWorker }

func (w *resultWorker) ExecuteResult(ctx context.Context, o Order) (any, error) {
	time.Sleep(20 * time.Millisecond)
	return map[string]any{"members": []string{"a", "b"}}, w.Execute(ctx, o)
}

// The agent sends how long an order ran and, for a ResultWorker, its result.
func TestAgentSendsResultAndTiming(t *testing.T) {
	rt := NewRuntime()
	rt.Register(fake("svc"))
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "n1"}})
	rt.PlaceOrders("svc", []OrderSpec{{Node: "n1", Kind: "start"}})

	submit := func(_ context.Context, ev Event) error {
		ev.At = time.Now().UnixMilli()
		// Through JSON, as on the way through the log.
		raw, _ := json.Marshal(ev)
		var back Event
		_ = json.Unmarshal(raw, &back)
		rt.Apply(back)
		return nil
	}
	a := NewAgent("n1", rt, submit, &resultWorker{})
	a.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Run(ctx)

	waitFor(t, "the order to finish", func() bool { _, h := rt.OrderLog(); return len(h) == 1 })
	_, h := rt.OrderLog()
	rec := h[0]
	if rec.Status != OrderDone || rec.DurationMs < 20 || rec.Started == 0 || rec.Finished < rec.Started {
		t.Errorf("record %+v", rec)
	}
	if string(rec.Result) != `{"members":["a","b"]}` {
		t.Errorf("result %s", rec.Result)
	}
}
