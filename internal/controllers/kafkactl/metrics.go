package kafkactl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Thresholds for alerts.
const (
	alertVoterLag   = 1000             // metadata records a voter may trail the leader
	alertVoterStale = 10 * time.Second // since a voter last caught up
	metricsMaxAge   = time.Second      // cached metrics are reused this long
)

// Metrics is the live view of the local broker and, through it, the cluster.
type Metrics struct {
	Running bool `json:"running"`
	// BrokerID is this node's Kafka node.id.
	BrokerID int32 `json:"broker_id"`

	// The controller quorum (KRaft metadata log).
	QuorumLeader  int32         `json:"quorum_leader"`
	QuorumEpoch   int32         `json:"quorum_epoch"`
	HighWatermark int64         `json:"high_watermark"`
	Voters        []VoterMetric `json:"voters,omitempty"`
	Observers     int           `json:"observers"`

	// Cluster metadata as this broker sees it.
	Brokers          []int32  `json:"brokers"`
	Topics           int      `json:"topics"`
	Partitions       int      `json:"partitions"`
	LeaderPartitions int      `json:"leader_partitions"` // led by this broker
	UnderReplicated  []string `json:"under_replicated,omitempty"`
	Offline          []string `json:"offline,omitempty"`

	// MessagesPerSec is the rate the end offsets of all user topics grow.
	MessagesPerSec *float64 `json:"messages_per_sec,omitempty"`
	// Groups and their total lag.
	Groups   int   `json:"groups"`
	GroupLag int64 `json:"group_lag"`

	Alerts []string `json:"alerts,omitempty"`
}

type VoterMetric struct {
	ID  int32 `json:"id"`
	Lag int64 `json:"lag"`
	// CaughtUpAgoSec is how long ago the voter was last fully caught up.
	CaughtUpAgoSec *float64 `json:"caught_up_ago_sec,omitempty"`
}

type metricsState struct {
	mu     sync.Mutex
	client *kgo.Client
	last   *Metrics
	at     time.Time
	ends   int64
	endsAt time.Time
}

func (s *metricsState) close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

// Metrics reads live metrics from the local broker. Results are cached for a
// second, so several viewers cost one set of requests.
func (w *Worker) Metrics(ctx context.Context) (any, error) {
	w.mu.Lock()
	running := w.running
	w.mu.Unlock()

	s := &w.metrics
	s.mu.Lock()
	defer s.mu.Unlock()
	if !running {
		s.close()
		s.last, s.ends = nil, 0
		return Metrics{BrokerID: int32(w.cfg.ID)}, nil
	}
	if s.last != nil && time.Since(s.at) < metricsMaxAge {
		return *s.last, nil
	}
	if s.client == nil {
		cl, err := kgo.NewClient(kgo.SeedBrokers(w.cfg.Addr), kgo.RequestTimeoutOverhead(2*time.Second))
		if err != nil {
			return nil, err
		}
		s.client = cl
	}
	m, err := w.readMetrics(ctx, s)
	if err != nil {
		return nil, err
	}
	s.last, s.at = &m, time.Now()
	return m, nil
}

func (w *Worker) readMetrics(ctx context.Context, s *metricsState) (Metrics, error) {
	m := Metrics{Running: true, BrokerID: int32(w.cfg.ID), QuorumLeader: -1}
	adm := kadm.NewClient(s.client)

	md, err := adm.Metadata(ctx)
	if err != nil {
		return m, fmt.Errorf("metadata: %w", err)
	}
	for _, b := range md.Brokers {
		m.Brokers = append(m.Brokers, b.NodeID)
	}
	slices.Sort(m.Brokers)
	var userTopics []string
	for _, t := range md.Topics.Sorted() {
		if !t.IsInternal {
			m.Topics++
			userTopics = append(userTopics, t.Topic)
		}
		for _, p := range t.Partitions.Sorted() {
			m.Partitions++
			name := t.Topic + "-" + strconv.Itoa(int(p.Partition))
			switch {
			case p.Leader < 0:
				m.Offline = append(m.Offline, name)
			case len(p.ISR) < len(p.Replicas):
				m.UnderReplicated = append(m.UnderReplicated, name)
			}
			if p.Leader == m.BrokerID {
				m.LeaderPartitions++
			}
		}
	}
	if n := len(m.Offline); n > 0 {
		m.Alerts = append(m.Alerts, fmt.Sprintf("kafka: %d partition(s) offline (no leader): %s", n, brief(m.Offline)))
	}
	if n := len(m.UnderReplicated); n > 0 {
		m.Alerts = append(m.Alerts, fmt.Sprintf("kafka: %d partition(s) under-replicated: %s", n, brief(m.UnderReplicated)))
	}

	if err := w.quorumMetrics(ctx, s.client, &m); err != nil {
		m.Alerts = append(m.Alerts, "kafka: controller quorum not answering: "+err.Error())
	}

	if len(userTopics) > 0 {
		if ends, err := adm.ListEndOffsets(ctx, userTopics...); err == nil {
			var total int64
			ends.Each(func(o kadm.ListedOffset) {
				if o.Err == nil && o.Offset > 0 {
					total += o.Offset
				}
			})
			now := time.Now()
			if s.ends > 0 && total >= s.ends {
				rate := float64(total-s.ends) / now.Sub(s.endsAt).Seconds()
				m.MessagesPerSec = &rate
			}
			s.ends, s.endsAt = total, now
		}
	}

	if groups, err := adm.ListGroups(ctx); err == nil {
		m.Groups = len(groups)
		if len(groups) > 0 {
			if lags, err := adm.Lag(ctx, groups.Groups()...); err == nil {
				for _, g := range lags {
					if g.Error() == nil {
						m.GroupLag += g.Lag.Total()
					}
				}
			}
		}
	}
	return m, nil
}

// quorumMetrics describes the KRaft metadata quorum.
func (w *Worker) quorumMetrics(ctx context.Context, cl *kgo.Client, m *Metrics) error {
	req := kmsg.NewPtrDescribeQuorumRequest()
	t := kmsg.NewDescribeQuorumRequestTopic()
	t.Topic = "__cluster_metadata"
	p := kmsg.NewDescribeQuorumRequestTopicPartition()
	t.Partitions = append(t.Partitions, p)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return errors.New("empty answer")
	}
	q := resp.Topics[0].Partitions[0]
	if q.ErrorCode != 0 {
		return fmt.Errorf("error code %d", q.ErrorCode)
	}
	m.QuorumLeader, m.QuorumEpoch, m.HighWatermark = q.LeaderID, q.LeaderEpoch, q.HighWatermark
	m.Observers = len(q.Observers)
	if q.LeaderID < 0 {
		m.Alerts = append(m.Alerts, "kafka: controller quorum has no leader")
	}
	now := time.Now().UnixMilli()
	for _, v := range q.CurrentVoters {
		vm := VoterMetric{ID: v.ReplicaID, Lag: max(q.HighWatermark-v.LogEndOffset, 0)}
		if v.ReplicaID != q.LeaderID && v.LastCaughtUpTimestamp > 0 {
			ago := float64(now-v.LastCaughtUpTimestamp) / 1000
			vm.CaughtUpAgoSec = &ago
			if time.Duration(ago*float64(time.Second)) > alertVoterStale {
				m.Alerts = append(m.Alerts, fmt.Sprintf("kafka: quorum voter %d not caught up for %.0fs", v.ReplicaID, ago))
			}
		}
		if v.ReplicaID != q.LeaderID && vm.Lag > alertVoterLag {
			m.Alerts = append(m.Alerts, fmt.Sprintf("kafka: quorum voter %d is %d records behind", v.ReplicaID, vm.Lag))
		}
		m.Voters = append(m.Voters, vm)
	}
	slices.SortFunc(m.Voters, func(a, b VoterMetric) int { return int(a.ID - b.ID) })
	return nil
}

// brief lists the first few names.
func brief(names []string) string {
	const n = 5
	if len(names) <= n {
		return fmt.Sprint(names)
	}
	return fmt.Sprintf("%v and %d more", names[:n], len(names)-n)
}
