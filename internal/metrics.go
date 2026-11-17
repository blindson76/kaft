package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"example.com/dstate/internal/control"
)

// Live metrics never go through the raft log: they change all the time and
// only matter while somebody looks. Each node serves its workers' metrics on
// GET /metrics; the node serving the web view polls every node while a view
// is open and pushes the result over the WebSocket.

const (
	metricsInterval = 2 * time.Second
	metricsTimeout  = 3 * time.Second
)

// nodeMetrics is what GET /metrics returns: per worker, its metrics or why
// they could not be read.
type nodeMetrics struct {
	Node    string                   `json:"node"`
	Workers map[string]workerMetrics `json:"workers"`
	Error   string                   `json:"error,omitempty"`
}

type workerMetrics struct {
	Metrics any    `json:"metrics,omitempty"`
	Error   string `json:"error,omitempty"`
}

// localMetrics collects the metrics of this node's workers, concurrently.
func (c *Cluster) localMetrics(ctx context.Context) nodeMetrics {
	out := nodeMetrics{Node: c.nodeID, Workers: map[string]workerMetrics{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, w := range c.workers {
		mw, ok := w.(control.MetricsWorker)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, metricsTimeout)
			defer cancel()
			var r workerMetrics
			if m, err := mw.Metrics(ctx); err != nil {
				r.Error = err.Error()
			} else {
				r.Metrics = m
			}
			mu.Lock()
			out.Workers[w.Name()] = r
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (c *Cluster) serveMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(c.localMetrics(r.Context()))
}

// fetchMetrics reads another node's metrics.
func (c *Cluster) fetchMetrics(ctx context.Context, node NodeConfig) nodeMetrics {
	out := nodeMetrics{Node: node.ID}
	ctx, cancel := context.WithTimeout(ctx, metricsTimeout+time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+node.HTTPAddr+"/metrics", nil)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		out.Error = "unreachable"
		return out
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		out.Error = fmt.Sprintf("HTTP %s", resp.Status)
		return out
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		out.Error = "bad response: " + err.Error()
	}
	out.Node = node.ID
	return out
}

// wsMetrics is pushed over the WebSocket every metricsInterval.
type wsMetrics struct {
	Type  string                 `json:"type"` // "metrics"
	At    time.Time              `json:"at"`
	Nodes map[string]nodeMetrics `json:"nodes"`
}

// clusterMetrics reads the metrics of every node that runs workers.
func (c *Cluster) clusterMetrics(ctx context.Context) wsMetrics {
	out := wsMetrics{Type: "metrics", Nodes: map[string]nodeMetrics{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range slices.Sorted(maps.Keys(c.nodesMap)) {
		node := c.nodesMap[id]
		if !node.IsWorker() || len(node.Controllers) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m nodeMetrics
			if id == c.nodeID {
				m = c.localMetrics(ctx)
			} else {
				m = c.fetchMetrics(ctx, node)
			}
			mu.Lock()
			out.Nodes[id] = m
			mu.Unlock()
		}()
	}
	wg.Wait()
	out.At = time.Now()
	return out
}
