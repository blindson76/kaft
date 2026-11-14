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

// serveHTTP exposes the node's HTTP endpoint:
//
//	POST /event  commit an event (leader only; followers answer 503)
//	GET  /state  raft and control state of this node, for inspection
func (c *Cluster) serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()

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
			"raft":    c.r.State().String(),
			"leader":  leader,
			"members": members,
			"control": c.rt.Inspect(),
		})
	})

	srv := &http.Server{Addr: c.nodesMap[c.nodeID].HTTPAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP server error:%v", err)
	}
}
