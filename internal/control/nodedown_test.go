package control

import (
	"slices"
	"testing"
)

// When a node goes down its pending orders fail with ErrNodeDown: they are
// recorded as failed and each controller is told, after EvNodeDown. Orders of
// other nodes are left alone.
func TestNodeDownFailsItsPendingOrders(t *testing.T) {
	rt := NewRuntime()
	a := &recorder{fakeController: fake("a")}
	b := &recorder{fakeController: fake("b")}
	rt.Register(a)
	rt.Register(b)
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "n1"}})
	rt.Apply(Event{Name: EvNodeUp, Params: map[string]any{"node": "n2"}})
	rt.PlaceOrders("a", []OrderSpec{{Node: "n1", Kind: "a.stop"}, {Node: "n2", Kind: "a.stop"}})
	rt.PlaceOrders("b", []OrderSpec{{Node: "n1", Kind: "b.start"}})
	a.seen, b.seen = nil, nil

	rt.Apply(Event{Name: EvNodeDown, At: 7000, Params: map[string]any{"node": "n1"}})

	if want := []string{"node.down:n1", "order.failed:n1"}; !slices.Equal(a.seen, want) {
		t.Errorf("a saw %v, want %v", a.seen, want)
	}
	if want := []string{"node.down:n1", "order.failed:n1"}; !slices.Equal(b.seen, want) {
		t.Errorf("b saw %v, want %v", b.seen, want)
	}
	pending, history := rt.OrderLog()
	if len(pending) != 1 || pending[0].Node != "n2" {
		t.Errorf("pending %+v, want only the order of n2", pending)
	}
	if len(history) != 2 {
		t.Fatalf("history %+v", history)
	}
	for _, rec := range history {
		if rec.Node != "n1" || rec.Status != OrderFailed || rec.Error != ErrNodeDown || rec.Finished != 7000 {
			t.Errorf("record %+v", rec)
		}
	}
}
