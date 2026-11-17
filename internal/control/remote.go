package control

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
)

// RemoteView is the View of a worker-only node: it has no replica of its own,
// so it keeps fetching its AgentState from a server and answers from the last
// copy it got.
//
// fetch should ask the raft leader. The leader's replica only moves forward,
// so the agent never sees an order it already settled come back as pending;
// a follower could be behind the leader the view was read from before.
//
// The agent's catch-up check works unchanged: until the fetched state carries
// this start's boot ID, the agent stays idle.
type RemoteView struct {
	node     string
	fetch    func(ctx context.Context, node string) (AgentState, error)
	interval time.Duration

	mu    sync.RWMutex
	state AgentState
	wg    sync.WaitGroup
}

func NewRemoteView(node string, fetch func(ctx context.Context, node string) (AgentState, error)) *RemoteView {
	return &RemoteView{node: node, fetch: fetch, interval: 250 * time.Millisecond}
}

// Run keeps the view current until ctx ends. It returns immediately; Wait
// waits for it to finish.
func (v *RemoteView) Run(ctx context.Context) {
	v.wg.Go(func() {
		ticker := time.NewTicker(v.interval)
		defer ticker.Stop()
		failing := false
		for {
			s, err := v.fetch(ctx, v.node)
			switch {
			case ctx.Err() != nil:
				return
			case err == nil:
				if failing {
					log.Printf("Remote state reachable again")
				}
				failing = false
				v.mu.Lock()
				v.state = s
				v.mu.Unlock()
			case !failing:
				// Kept: the agent only acts on what it saw last, and an
				// order executed twice is safe (workers are idempotent).
				log.Printf("Remote state unavailable: %v", err)
				failing = true
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
}

// Wait returns once the loop Run started has ended.
func (v *RemoteView) Wait() { v.wg.Wait() }

// Boot implements View.
func (v *RemoteView) Boot(node string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if node != v.state.Node {
		return ""
	}
	return v.state.Boot
}

// Started implements View. A controller the servers do not know is reported
// as not started, so its worker stays idle.
func (v *RemoteView) Started(controller string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.state.Started[controller]
}

// Report implements View.
func (v *RemoteView) Report(controller, node string) json.RawMessage {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if node != v.state.Node {
		return nil
	}
	return v.state.Reports[controller]
}

// PendingOrders implements View.
func (v *RemoteView) PendingOrders(controller, node string) []Order {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if node != v.state.Node {
		return nil
	}
	var out []Order
	for _, o := range v.state.Orders {
		if o.Controller == controller {
			out = append(out, o)
		}
	}
	return out
}
