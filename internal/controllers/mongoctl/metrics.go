package mongoctl

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Thresholds for alerts.
const (
	alertLag        = 30 * time.Second // a secondary this far behind the primary
	alertConnPct    = 80               // connections in use, % of the limit
	alertOplogFull  = 90               // oplog fill, %, before its window counts
	alertOplogShort = time.Hour        // a full oplog covering less than this
	metricsMaxAge   = time.Second      // cached metrics are reused this long

	codeUnauthorized = 13
)

// Metrics is the live view of the local mongod, for the web UI.
type Metrics struct {
	Running bool   `json:"running"`
	Role    string `json:"role"`
	// UptimeSec is how long mongod has been running.
	UptimeSec float64 `json:"uptime_sec,omitempty"`
	// Connections in use and the limit.
	Connections    int     `json:"connections"`
	ConnectionsMax int     `json:"connections_max"`
	ResidentMB     float64 `json:"resident_mb"`
	// Ops is operations per second since the previous sample, by type.
	Ops map[string]float64 `json:"ops"`
	// LagSec is how far this node is behind the primary, from its own view.
	LagSec *float64 `json:"lag_sec,omitempty"`
	// OplogWindowSec is the time span the oplog covers; a member that is
	// further behind than this cannot catch up and needs a resync.
	OplogWindowSec float64 `json:"oplog_window_sec"`
	OplogUsedPct   float64 `json:"oplog_used_pct"`
	// Members is the replica set as this node sees it; reported by the
	// primary only, which is the reference for lag.
	Members []MemberMetrics `json:"members,omitempty"`
	// Alerts are critical conditions this node can see.
	Alerts []string `json:"alerts,omitempty"`
}

type MemberMetrics struct {
	Host   string   `json:"host"`
	State  string   `json:"state"`
	Health bool     `json:"health"`
	LagSec *float64 `json:"lag_sec,omitempty"`
	PingMs *float64 `json:"ping_ms,omitempty"`
}

type serverStatus struct {
	Uptime      float64 `bson:"uptime"`
	Connections struct {
		Current   float64 `bson:"current"`
		Available float64 `bson:"available"`
	} `bson:"connections"`
	Mem struct {
		Resident float64 `bson:"resident"`
	} `bson:"mem"`
	Opcounters map[string]float64 `bson:"opcounters"`
}

type rsStatus struct {
	Members []struct {
		Name       string    `bson:"name"`
		StateStr   string    `bson:"stateStr"`
		Health     float64   `bson:"health"`
		OptimeDate time.Time `bson:"optimeDate"`
		PingMs     *float64  `bson:"pingMs"`
		Self       bool      `bson:"self"`
	} `bson:"members"`
}

// metricsState keeps what rates and caching need between calls.
type metricsState struct {
	mu sync.Mutex
	// client is the metrics' own authenticated connection, opened for the
	// running mongod whose monitored client is owner (see Worker.session).
	client *driver.Client
	owner  *driver.Client
	last   *Metrics
	at     time.Time
	ops    map[string]float64
	opsAt  time.Time
	uptime float64
}

// Metrics reads live metrics from the local mongod. Results are cached for a
// second, so several viewers cost one set of queries.
func (w *Worker) Metrics(ctx context.Context) (any, error) {
	s := &w.metrics
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last != nil && time.Since(s.at) < metricsMaxAge {
		return *s.last, nil
	}
	m, err := w.readMetrics(ctx, s)
	if err != nil {
		return nil, err
	}
	s.last, s.at = &m, time.Now()
	return m, nil
}

// drop closes the metrics' connection; the next read opens a new one.
func (s *metricsState) drop() {
	if s.client != nil {
		_ = s.client.Disconnect(context.Background())
	}
	s.client, s.owner = nil, nil
}

func (w *Worker) readMetrics(ctx context.Context, s *metricsState) (Metrics, error) {
	w.mu.Lock()
	owner, role := w.client, w.status.Role
	w.mu.Unlock()
	m := Metrics{Role: role, Ops: map[string]float64{}}
	if owner == nil {
		// Not running as a member: nothing to read. Whether that matters
		// depends on membership, which the view knows.
		s.drop()
		return m, nil
	}
	m.Running = true
	if s.owner != owner {
		s.drop()
		c, _, err := w.session(ctx)
		if err != nil {
			return m, err
		}
		s.client, s.owner = c, owner
	}
	c := s.client
	admin := c.Database("admin")

	var ss serverStatus
	if err := admin.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&ss); err != nil {
		if commandCode(err) == codeUnauthorized {
			s.drop() // users were replaced (initial sync): log in again
		}
		return m, fmt.Errorf("serverStatus: %w", err)
	}
	m.UptimeSec = ss.Uptime
	m.Connections = int(ss.Connections.Current)
	m.ConnectionsMax = int(ss.Connections.Current + ss.Connections.Available)
	m.ResidentMB = ss.Mem.Resident
	now := time.Now()
	// Counters restart with mongod: no rate across a restart.
	if s.ops != nil && ss.Uptime >= s.uptime {
		dt := now.Sub(s.opsAt).Seconds()
		for k, v := range ss.Opcounters {
			if d := v - s.ops[k]; dt > 0 && d >= 0 {
				m.Ops[k] = d / dt
			}
		}
	}
	s.ops, s.opsAt, s.uptime = ss.Opcounters, now, ss.Uptime
	if m.ConnectionsMax > 0 && m.Connections*100 >= m.ConnectionsMax*alertConnPct {
		m.Alerts = append(m.Alerts, fmt.Sprintf("mongo %s: %d of %d connections in use", w.cfg.Addr, m.Connections, m.ConnectionsMax))
	}

	var rs rsStatus
	if err := admin.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&rs); err == nil {
		w.memberMetrics(&m, rs)
	}
	w.oplogMetrics(ctx, c, &m)
	return m, nil
}

func (w *Worker) memberMetrics(m *Metrics, rs rsStatus) {
	var primary time.Time
	for _, mem := range rs.Members {
		if mem.StateStr == "PRIMARY" {
			primary = mem.OptimeDate
		}
	}
	if primary.IsZero() {
		m.Alerts = append(m.Alerts, "mongo: replica set has no primary")
		return
	}
	lagOf := func(t time.Time) *float64 {
		l := max(primary.Sub(t).Seconds(), 0)
		return &l
	}
	for _, mem := range rs.Members {
		if mem.Self && m.Role != RolePrimary {
			m.LagSec = lagOf(mem.OptimeDate)
		}
	}
	// Member-level alerts come from the primary only, so the view does not
	// get one per node for the same problem.
	if m.Role != RolePrimary {
		return
	}
	for _, mem := range rs.Members {
		mm := MemberMetrics{Host: mem.Name, State: mem.StateStr, Health: mem.Health == 1, PingMs: mem.PingMs}
		switch {
		case !mm.Health:
			m.Alerts = append(m.Alerts, fmt.Sprintf("mongo member %s is down", mem.Name))
		case mem.StateStr == "SECONDARY":
			mm.LagSec = lagOf(mem.OptimeDate)
			if *mm.LagSec >= alertLag.Seconds() {
				m.Alerts = append(m.Alerts, fmt.Sprintf("mongo member %s is %.0fs behind the primary", mem.Name, *mm.LagSec))
			}
		case mem.StateStr != "PRIMARY":
			m.Alerts = append(m.Alerts, fmt.Sprintf("mongo member %s is %s", mem.Name, mem.StateStr))
		}
		m.Members = append(m.Members, mm)
	}
	slices.SortFunc(m.Members, func(a, b MemberMetrics) int {
		if a.Host < b.Host {
			return -1
		}
		return 1
	})
}

// oplogMetrics reads the time span and fill of the oplog.
func (w *Worker) oplogMetrics(ctx context.Context, c *driver.Client, m *Metrics) {
	oplog := c.Database("local").Collection("oplog.rs")
	edge := func(dir int) (bson.Timestamp, bool) {
		var e struct {
			TS bson.Timestamp `bson:"ts"`
		}
		opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: dir}}).SetProjection(bson.D{{Key: "ts", Value: 1}})
		if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&e); err != nil {
			return bson.Timestamp{}, false
		}
		return e.TS, true
	}
	first, ok1 := edge(1)
	last, ok2 := edge(-1)
	if !ok1 || !ok2 {
		return
	}
	m.OplogWindowSec = float64(last.T) - float64(first.T)

	cur, err := oplog.Aggregate(ctx, bson.A{bson.D{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}}})
	if err != nil {
		return
	}
	defer cur.Close(ctx)
	var st struct {
		StorageStats struct {
			Size    float64 `bson:"size"`
			MaxSize float64 `bson:"maxSize"`
		} `bson:"storageStats"`
	}
	if cur.Next(ctx) && cur.Decode(&st) == nil && st.StorageStats.MaxSize > 0 {
		m.OplogUsedPct = 100 * st.StorageStats.Size / st.StorageStats.MaxSize
	}
	// The window only limits recovery once the oplog is full and drops its
	// oldest entries; before that it is just the age of the set.
	if m.OplogUsedPct >= alertOplogFull && m.OplogWindowSec < alertOplogShort.Seconds() {
		m.Alerts = append(m.Alerts, fmt.Sprintf("mongo %s: oplog covers only %.0f min; a member down longer needs a full resync", w.cfg.Addr, m.OplogWindowSec/60))
	}
}
