package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"example.com/dstate/internal/control"
	"github.com/hashicorp/raft"
)

var httpClient = &http.Client{Timeout: 3 * time.Second}

// postEvent hands ev to the node at addr, which must be the leader.
func postEvent(ctx context.Context, addr string, ev control.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/event", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("leader rejected event: %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

// serveHTTP exposes the node's HTTP endpoint. Every node, voter or not, has a
// replica and serves:
//
//	POST /event    commit an event (leader only; the others answer 503)
//	GET  /state    raft and control state of this node, for inspection
//	GET  /         live view of the state machines (see web.go)
//	GET  /ws       WebSocket feed behind that view
//	GET  /metrics  live metrics of its workers (metrics.go)
//
// plus whatever the controllers added with Handle.
//
// It fails if the address cannot be listened on. Requests run on contexts
// derived from ctx, so open views and long requests end with the node;
// Shutdown stops the server.
func (c *Cluster) startHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	c.routesTo(mux)
	mux.HandleFunc("GET /metrics", c.serveMetrics)
	for pattern, handler := range c.routes {
		mux.HandleFunc(pattern, handler)
	}

	addr := c.nodesMap[c.nodeID].HTTPAddr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	c.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	c.wg.Go(func() {
		if err := c.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error:%v", err)
		}
	})
	return nil
}

func (c *Cluster) routesTo(mux *http.ServeMux) {
	mux.HandleFunc("POST /event", func(w http.ResponseWriter, r *http.Request) {
		var ev control.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Not forwarded again: a stale leader hint must not bounce an event
		// around the cluster. The sender retries.
		if c.r.State() != raft.Leader {
			http.Error(w, ErrNoLeader.Error(), http.StatusServiceUnavailable)
			return
		}
		if err := c.apply(ev); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	})

	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		_, leader := c.r.LeaderWithID()
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"node":    c.nodeID,
			"roles":   c.self.Roles,
			"workers": c.workerNames(),
			"raft":    c.r.State().String(),
			"leader":  leader,
			"members": c.members(),
			"voters":  c.voters(),
			"control": c.rt.Inspect(),
		})
	})

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})
	mux.HandleFunc("GET /ws", c.serveWS)
}
