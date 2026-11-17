package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
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

// leaderTimeout bounds one attempt at a server from a worker-only node, so an
// unreachable server does not hold up trying the next one.
const leaderTimeout = time.Second

// leaderClient reaches the raft leader from a worker-only node. Servers answer
// 503 unless they lead, so it tries them in turn, starting with the one that
// answered last.
type leaderClient struct {
	addrs []string

	mu   sync.Mutex
	last int
}

// call runs fn against the servers until one succeeds.
func (l *leaderClient) call(ctx context.Context, fn func(ctx context.Context, addr string) error) error {
	if len(l.addrs) == 0 {
		return ErrNoLeader
	}
	l.mu.Lock()
	start := l.last
	l.mu.Unlock()

	var err error
	for i := range l.addrs {
		idx := (start + i) % len(l.addrs)
		actx, cancel := context.WithTimeout(ctx, leaderTimeout)
		err = fn(actx, l.addrs[idx])
		cancel()
		if err == nil {
			l.mu.Lock()
			l.last = idx
			l.mu.Unlock()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

// fetchAgentState reads the state node's agent works from, from the leader.
func (c *Cluster) fetchAgentState(ctx context.Context, node string) (control.AgentState, error) {
	var out control.AgentState
	err := c.leader.call(ctx, func(ctx context.Context, addr string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/agent/"+url.PathEscape(node), nil)
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
			return fmt.Errorf("%s: %s: %s", addr, resp.Status, bytes.TrimSpace(msg))
		}
		var s control.AgentState
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			return err
		}
		out = s
		return nil
	})
	return out, err
}

// serveHTTP exposes the node's HTTP endpoint. On a server:
//
//	POST /event         commit an event (leader only; followers answer 503)
//	GET  /agent/{node}  what node's agent works from (leader only), for
//	                    worker-only nodes
//	GET  /state         raft and control state of this node, for inspection
//	GET  /              live view of the state machines (see web.go)
//	GET  /ws            WebSocket feed behind that view
//
// A worker-only node only serves GET /state with its roles and workers; the
// leader also checks its liveness against this endpoint.
//
// Every node serves GET /metrics: live metrics of its workers (metrics.go).
//
// Both serve whatever the controllers added with Handle.
func (c *Cluster) serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()
	if c.IsServer() {
		c.serverRoutes(mux)
	} else {
		mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(map[string]any{
				"node":    c.nodeID,
				"roles":   c.self.Roles,
				"workers": c.workerNames(),
				"servers": c.servers,
			})
		})
	}
	mux.HandleFunc("GET /metrics", c.serveMetrics)
	for pattern, handler := range c.routes {
		mux.HandleFunc(pattern, handler)
	}

	srv := &http.Server{Addr: c.nodesMap[c.nodeID].HTTPAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP server error:%v", err)
	}
}

func (c *Cluster) serverRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /agent/{node}", func(w http.ResponseWriter, r *http.Request) {
		// Leader only: its replica never goes back, so a worker never sees
		// a settled order as pending again.
		if c.r.State() != raft.Leader {
			http.Error(w, ErrNoLeader.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.rt.AgentState(r.PathValue("node")))
	})

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
		members := []string{}
		if cf := c.r.GetConfiguration(); cf.Error() == nil {
			for _, s := range cf.Configuration().Servers {
				members = append(members, string(s.ID))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"node":    c.nodeID,
			"roles":   c.self.Roles,
			"workers": c.workerNames(),
			"raft":    c.r.State().String(),
			"leader":  leader,
			"members": members,
			"control": c.rt.Inspect(),
		})
	})

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})
	mux.HandleFunc("GET /ws", c.serveWS)
}
