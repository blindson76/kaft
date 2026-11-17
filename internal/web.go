package internal

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"example.com/dstate/internal/control"
	"github.com/coder/websocket"
	"github.com/hashicorp/raft"
)

// uiHTML is the live view: a single self-contained page that draws each
// controller's state machine and follows it over /ws.
//
//go:embed ui.html
var uiHTML []byte

// wsHello is the first message on a connection: everything that never changes.
type wsHello struct {
	Type        string            `json:"type"` // "hello"
	Node        string            `json:"node"`
	Nodes       []wsNode          `json:"nodes"`
	Controllers []control.Diagram `json:"controllers"`
}

type wsNode struct {
	ID   string `json:"id"`
	HTTP string `json:"http"`
}

// wsState is sent after the hello and again whenever it differs from the last
// one sent. It is always the complete current state, never a delta, so a
// client that missed messages or just connected needs no history.
type wsState struct {
	Type    string   `json:"type"` // "state"
	Raft    string   `json:"raft"`
	Leader  string   `json:"leader"`
	Members []string `json:"members"`
	// Voters are the members with a vote; the others are worker-only nodes.
	Voters  []string `json:"voters"`
	Control any      `json:"control"`
}

// members returns the raft members, voters or not, as this node knows them.
func (c *Cluster) members() []string { return c.membersWith(nil) }

// voters returns the raft members with a vote.
func (c *Cluster) voters() []string {
	v := raft.Voter
	return c.membersWith(&v)
}

func (c *Cluster) membersWith(suffrage *raft.ServerSuffrage) []string {
	out := []string{}
	if cf := c.r.GetConfiguration(); cf.Error() == nil {
		for _, s := range cf.Configuration().Servers {
			if suffrage == nil || s.Suffrage == *suffrage {
				out = append(out, string(s.ID))
			}
		}
	}
	return out
}

func (c *Cluster) currentState() wsState {
	_, leader := c.r.LeaderWithID()
	return wsState{
		Type:    "state",
		Raft:    c.r.State().String(),
		Leader:  string(leader),
		Members: c.members(),
		Voters:  c.voters(),
		Control: c.rt.Inspect(),
	}
}

// serveWS streams this node's view of the replicated state. Every node serves
// its own replica; they converge, so any node shows the cluster, and comparing
// two nodes shows replication lag.
func (c *Cluster) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	// The client sends nothing; reading is only how a close is noticed. The
	// request context ends with the node too (see startHTTP).
	ctx, cancel := context.WithCancel(conn.CloseRead(r.Context()))
	var wg sync.WaitGroup
	// Runs first of the deferred calls: the metrics poller ends before the
	// connection is closed and the handler returns.
	defer wg.Wait()
	defer cancel()

	send := func(v any) ([]byte, error) {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return raw, conn.Write(wctx, websocket.MessageText, raw)
	}

	hello := wsHello{Type: "hello", Node: c.nodeID, Controllers: c.rt.Diagrams()}
	// Every node has a replica and serves this view.
	for _, id := range c.nodes {
		hello.Nodes = append(hello.Nodes, wsNode{ID: id, HTTP: c.nodesMap[id].HTTPAddr})
	}
	if _, err := send(hello); err != nil {
		return
	}

	// Metrics are polled from every node while this view is open, on their
	// own goroutine so a slow node never delays the state.
	wg.Go(func() {
		t := time.NewTicker(metricsInterval)
		defer t.Stop()
		for {
			if _, err := send(c.clusterMetrics(ctx)); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	})

	changed, unsubscribe := c.rt.Subscribe()
	defer unsubscribe()
	// Raft role, leader and membership change without a control event, so
	// they are picked up on a slow tick.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	var last []byte
	for {
		raw, err := json.Marshal(c.currentState())
		if err != nil {
			return
		}
		if !bytes.Equal(raw, last) {
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(wctx, websocket.MessageText, raw)
			cancel()
			if err != nil {
				return
			}
			last = raw
		}

		select {
		case <-ctx.Done():
			return
		case <-changed:
			// A burst of events is one repaint.
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		case <-tick.C:
		}
	}
}
