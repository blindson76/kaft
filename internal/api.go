package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"example.com/dstate/internal/control"
	"github.com/hashicorp/raft"
)

var httpClient = &http.Client{Timeout: 3 * time.Second}

// forwardedHeader marks an event one node hands to the leader. A node that is
// not the leader refuses such an event instead of passing it on again, so a
// stale leader hint cannot bounce it around the cluster.
const forwardedHeader = "X-Kaft-Forwarded"

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
	req.Header.Set(forwardedHeader, "1")
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

// serveHTTP exposes the node's HTTP endpoint:
//
//	POST /event  commit an event; any node takes it (see below)
//	GET  /state  raft and control state of this node, for inspection
//	GET  /       live view of the state machines (see web.go)
//	GET  /ws     WebSocket feed behind that view
//
// plus whatever the controllers added with Handle.
func (c *Cluster) serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()

	// An event from outside the cluster (an operator, another system) can
	// be sent to any node: one that is not the leader passes it on. Such an
	// event is stamped with "now_ms", this node's clock in Unix milliseconds,
	// unless it carries one: a controller that needs the time of the request
	// (a manual time setting) must not read a clock itself, as every replica
	// would read a different one. An event passed on by another node is
	// committed as it is, by the leader only.
	mux.HandleFunc("POST /event", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		var ev control.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Header.Get(forwardedHeader) != "" {
			// The sender retries.
			if c.r.State() != raft.Leader {
				http.Error(w, ErrNoLeader.Error(), http.StatusServiceUnavailable)
				return
			}
			if err := c.apply(ev); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			}
			return
		}

		if ev.Name == "" || ev.Internal {
			http.Error(w, `expected {"target": "<controller>", "name": "<event>", "params": {...}}`, http.StatusBadRequest)
			return
		}
		if ev.Params == nil {
			ev.Params = map[string]any{}
		}
		if _, ok := ev.Params["now_ms"]; !ok {
			ev.Params["now_ms"] = now.UnixMilli()
		}
		if err := c.Submit(r.Context(), ev); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ev)
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
