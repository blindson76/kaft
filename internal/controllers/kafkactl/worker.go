package kafkactl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.com/dstate/internal/control"
)

// Kafka entry points. The Windows .bat wrappers build a classpath that
// overflows cmd's line limit unless Kafka sits in a very short path, so the
// same classes are run directly with a wildcard classpath instead.
const (
	classServer   = "kafka.Kafka"
	classStorage  = "kafka.tools.StorageTool"
	classQuorum   = "org.apache.kafka.tools.MetadataQuorumCommand"
	classTopics   = "org.apache.kafka.tools.TopicCommand"
	classReassign = "org.apache.kafka.tools.reassign.ReassignPartitionsCommand"
)

// WorkerConfig configures the node-local Kafka.
type WorkerConfig struct {
	Node string
	// ID is the Kafka node.id.
	ID int
	// Addr / ControllerAddr are the broker and controller listeners.
	Addr           string
	ControllerAddr string
	// Bootstrap lists the controller listeners of every node.
	Bootstrap []string
	// Home is the Kafka installation directory.
	Home string
	// DataDir is this node's directory.
	DataDir string
	// ClusterID is the fixed cluster ID every node is formatted with.
	ClusterID string
	// Replication is the replication factor for internal topics.
	Replication int
	// Heap is the broker's maximum heap, e.g. "512M".
	Heap string
}

// Worker runs the node-local Kafka process.
type Worker struct {
	cfg WorkerConfig

	// ops serializes the probe and the orders.
	ops  sync.Mutex
	once sync.Once

	// mu guards the fields below.
	mu      sync.Mutex
	report  *Report
	proc    *exec.Cmd
	exited  chan struct{}
	running bool

	metrics metricsState
}

func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.Heap == "" {
		cfg.Heap = "512M"
	}
	return &Worker{cfg: cfg}
}

func (w *Worker) Name() string { return Name }

// Probe returns the last probe result with the live running flag. It does no
// I/O; the running flag changes when the process starts serving or exits.
func (w *Worker) Probe(ctx context.Context) (any, error) {
	w.once.Do(func() {
		go func() {
			w.ops.Lock()
			defer w.ops.Unlock()
			w.probe(context.Background())
		}()
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.report == nil {
		return nil, control.ErrNotReady
	}
	r := *w.report
	r.Running = w.running
	return r, nil
}

// Execute carries out one order. Every order tolerates being run again.
func (w *Worker) Execute(ctx context.Context, order control.Order) error {
	w.ops.Lock()
	defer w.ops.Unlock()

	switch order.Kind {
	case OrderStart:
		bootstrap, _ := order.Args["bootstrap"].(bool)
		return w.start(ctx, bootstrap)
	case OrderStop:
		if r := w.probe(ctx); r.State != StateReady {
			return fmt.Errorf("node not ready after stop: %s", r.Reason)
		}
		return nil
	case OrderAddVoter:
		return w.addVoter(ctx, decode[[]string](order.Args["controllers"]))
	case OrderRemoveVoter:
		type voter struct {
			ID  int    `json:"id"`
			Dir string `json:"dir"`
		}
		for _, v := range decode[[]voter](order.Args["voters"]) {
			if err := w.removeVoter(ctx, decode[[]string](order.Args["controllers"]), v.ID, v.Dir); err != nil {
				return err
			}
		}
		return nil
	case OrderReassign:
		brokers, _ := order.Args["brokers"].(string)
		return w.reassign(ctx, brokers)
	}
	return fmt.Errorf("unknown order kind %q", order.Kind)
}

// decode reads an order argument, whose concrete type differs between a
// freshly placed order and one restored from a snapshot.
func decode[T any](v any) T {
	var out T
	raw, _ := json.Marshal(v)
	_ = json.Unmarshal(raw, &out)
	return out
}

// --- paths and tools -------------------------------------------------------

func (w *Worker) logDir() string    { return filepath.Join(w.cfg.DataDir, "logs") }
func (w *Worker) propsPath() string { return filepath.Join(w.cfg.DataDir, "server.properties") }
func (w *Worker) pidPath() string   { return filepath.Join(w.cfg.DataDir, "kafka.pid") }

func (w *Worker) javaArgs(heap, log4j, class string, args ...string) []string {
	out := []string{
		"-Xmx" + heap,
		"-Dlog4j2.configurationFile=" + filepath.Join(w.cfg.Home, "config", log4j),
		"-Dkafka.logs.dir=" + filepath.Join(w.cfg.DataDir, "applogs"),
		"-cp", filepath.Join(w.cfg.Home, "libs", "*"),
		class,
	}
	return append(out, args...)
}

// tool runs one of the Kafka command line tools and returns its output.
func (w *Worker) tool(ctx context.Context, class string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "java", w.javaArgs("256M", "tools-log4j2.yaml", class, args...)...).CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err != nil {
		return text, fmt.Errorf("%s: %w: %s", class[strings.LastIndex(class, ".")+1:], err, lastLine(text))
	}
	return text, nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func portOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func removeAll(dir string) error {
	var err error
	// Windows releases file handles a moment after the process exits.
	for i := 0; i < 20; i++ {
		if err = os.RemoveAll(dir); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

// --- probe -----------------------------------------------------------------

// probe stops Kafka, deletes its log directory and formats a new one, so the
// node is an empty server ready to join a cluster.
func (w *Worker) probe(ctx context.Context) Report {
	w.stop()

	id := make([]byte, 6)
	_, _ = rand.Read(id)
	r := Report{ProbeID: hex.EncodeToString(id), State: StateReady}

	dir, err := w.format(ctx, false)
	if err != nil {
		r.State, r.Reason = StateBroken, err.Error()
	}
	r.DirID = dir

	log.Printf("[kafka] probe: state=%s dir=%s %s", r.State, r.DirID, r.Reason)
	w.mu.Lock()
	w.report = &r
	w.mu.Unlock()
	return r
}

// format wipes the log directory and formats it. standalone makes the node a
// single-node controller quorum, which is how a cluster is created; otherwise
// it is formatted to join an existing quorum. It returns the directory ID
// Kafka generated.
func (w *Worker) format(ctx context.Context, standalone bool) (string, error) {
	if err := removeAll(w.logDir()); err != nil {
		return "", fmt.Errorf("wipe log dir: %w", err)
	}
	if err := os.MkdirAll(w.cfg.DataDir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(w.propsPath(), []byte(w.properties()), 0o644); err != nil {
		return "", err
	}
	mode := "--no-initial-controllers"
	if standalone {
		mode = "--standalone"
	}
	if _, err := w.tool(ctx, classStorage, "format", "--config", w.propsPath(), "--cluster-id", w.cfg.ClusterID, mode); err != nil {
		return "", err
	}

	meta, err := os.ReadFile(filepath.Join(w.logDir(), "meta.properties"))
	if err != nil {
		return "", fmt.Errorf("read meta.properties: %w", err)
	}
	for _, line := range strings.Split(string(meta), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "directory.id="); ok {
			return v, nil
		}
	}
	return "", errors.New("no directory.id in meta.properties")
}

func (w *Worker) properties() string {
	rf := strconv.Itoa(max(1, w.cfg.Replication))
	minISR := strconv.Itoa(max(1, w.cfg.Replication-1))
	lines := []string{
		"process.roles=broker,controller",
		"node.id=" + strconv.Itoa(w.cfg.ID),
		"controller.quorum.bootstrap.servers=" + strings.Join(w.cfg.Bootstrap, ","),
		"listeners=PLAINTEXT://" + w.cfg.Addr + ",CONTROLLER://" + w.cfg.ControllerAddr,
		"advertised.listeners=PLAINTEXT://" + w.cfg.Addr + ",CONTROLLER://" + w.cfg.ControllerAddr,
		"controller.listener.names=CONTROLLER",
		"inter.broker.listener.name=PLAINTEXT",
		"listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"log.dirs=" + filepath.ToSlash(w.logDir()),
		"num.partitions=3",
		"default.replication.factor=" + rf,
		"min.insync.replicas=" + minISR,
		"offsets.topic.replication.factor=" + rf,
		"transaction.state.log.replication.factor=" + rf,
		"transaction.state.log.min.isr=" + minISR,
		"share.coordinator.state.topic.replication.factor=" + rf,
		"share.coordinator.state.topic.min.isr=" + minISR,
	}
	return strings.Join(lines, "\n") + "\n"
}

// --- process ---------------------------------------------------------------

// start launches Kafka and waits until the broker listener accepts
// connections.
func (w *Worker) start(ctx context.Context, bootstrap bool) error {
	w.mu.Lock()
	running, report := w.running, w.report
	w.mu.Unlock()
	if running {
		return nil
	}
	if report == nil || report.State != StateReady {
		return errors.New("node is not ready to start: probe failed or pending")
	}
	w.stop()

	if bootstrap {
		dir, err := w.format(ctx, true)
		if err != nil {
			return fmt.Errorf("format standalone: %w", err)
		}
		w.mu.Lock()
		w.report.DirID = dir
		w.mu.Unlock()
		log.Printf("[kafka] formatted as single-node quorum, dir=%s", dir)
	}

	cmd := exec.Command("java", w.javaArgs(w.cfg.Heap, "log4j2.yaml", classServer, w.propsPath())...)
	out, err := os.Create(filepath.Join(w.cfg.DataDir, "kafka.out"))
	if err != nil {
		return err
	}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		out.Close()
		return fmt.Errorf("start kafka: %w", err)
	}
	_ = os.WriteFile(w.pidPath(), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)

	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		out.Close()
		close(exited)
		// If this is still the current process, it died on its own.
		w.mu.Lock()
		if w.proc == cmd {
			w.proc, w.running = nil, false
			log.Printf("[kafka] process exited")
		}
		w.mu.Unlock()
	}()
	w.mu.Lock()
	w.proc, w.exited = cmd, exited
	w.mu.Unlock()

	deadline := time.After(2 * time.Minute)
	for !portOpen(w.cfg.Addr) {
		select {
		case <-exited:
			return errors.New("kafka exited during startup (see kafka.out)")
		case <-deadline:
			w.stop()
			return errors.New("kafka did not accept connections in time")
		case <-time.After(500 * time.Millisecond):
		}
	}
	w.mu.Lock()
	w.running = w.proc == cmd
	w.mu.Unlock()
	log.Printf("[kafka] started")
	return nil
}

// stop kills Kafka: the process this worker started, or one left behind by a
// previous run of this program. There is nothing to shut down cleanly for:
// the data is wiped before the next start.
func (w *Worker) stop() {
	w.mu.Lock()
	cmd, exited := w.proc, w.exited
	w.proc, w.running = nil, false
	w.mu.Unlock()

	if cmd != nil {
		_ = cmd.Process.Kill()
		<-exited
	} else if raw, err := os.ReadFile(w.pidPath()); err == nil && portOpen(w.cfg.Addr) {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Kill()
			}
		}
	}
	_ = os.Remove(w.pidPath())
	for i := 0; i < 100 && portOpen(w.cfg.Addr); i++ {
		time.Sleep(200 * time.Millisecond)
	}
}

// --- quorum and partitions -------------------------------------------------

// retry runs fn until it succeeds, for conditions that clear by themselves: a
// node still catching up, another quorum change in flight.
func retry(ctx context.Context, d time.Duration, fn func() error) error {
	deadline := time.Now().Add(d)
	for {
		err := fn()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// voters returns the describe --status line listing the current voters.
func (w *Worker) voters(ctx context.Context, controllers []string) (string, error) {
	out, err := w.tool(ctx, classQuorum, "--bootstrap-controller", strings.Join(controllers, ","), "describe", "--status")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "CurrentVoters") {
			return line, nil
		}
	}
	return "", errors.New("no CurrentVoters in quorum status")
}

func voterEntry(id int, dir string) string {
	return fmt.Sprintf(`"id": %d, "directoryId": "%s"`, id, dir)
}

// addVoter adds this node, which must be running, to the controller quorum.
func (w *Worker) addVoter(ctx context.Context, controllers []string) error {
	w.mu.Lock()
	dir := ""
	if w.report != nil {
		dir = w.report.DirID
	}
	w.mu.Unlock()

	return retry(ctx, 3*time.Minute, func() error {
		if line, err := w.voters(ctx, controllers); err == nil && strings.Contains(line, voterEntry(w.cfg.ID, dir)) {
			return nil // already a voter
		}
		_, err := w.tool(ctx, classQuorum,
			"--bootstrap-controller", strings.Join(controllers, ","),
			"--command-config", w.propsPath(), "add-controller")
		if err == nil {
			log.Printf("[kafka] added to the controller quorum")
		}
		return err
	})
}

// removeVoter removes the voter (id, dir) from the controller quorum.
func (w *Worker) removeVoter(ctx context.Context, controllers []string, id int, dir string) error {
	return retry(ctx, 3*time.Minute, func() error {
		line, err := w.voters(ctx, controllers)
		if err != nil {
			return err
		}
		if !strings.Contains(line, voterEntry(id, dir)) {
			return nil // not a voter (any more)
		}
		_, err = w.tool(ctx, classQuorum,
			"--bootstrap-controller", strings.Join(controllers, ","),
			"remove-controller", "--controller-id", strconv.Itoa(id), "--controller-directory-id", dir)
		if err == nil {
			log.Printf("[kafka] removed voter %d (%s) from the controller quorum", id, dir)
		}
		return err
	})
}

// reassignWait bounds how long one reassign order waits for the data to move.
// It stays under the controller's step timeout so that a slow move is
// reported by the worker ("still in progress") instead of timing out silently;
// the next attempt picks up the same reassignment and keeps waiting.
const reassignWait = 4 * time.Minute

type assignment struct {
	Partitions []struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Replicas  []int  `json:"replicas"`
	} `json:"partitions"`
}

// misplaced lists the partitions that have a replica outside brokers.
func (a assignment) misplaced(brokers []int) []string {
	var out []string
	for _, p := range a.Partitions {
		for _, r := range p.Replicas {
			if !slices.Contains(brokers, r) {
				out = append(out, fmt.Sprintf("%s-%d%v", p.Topic, p.Partition, p.Replicas))
				break
			}
		}
	}
	return out
}

// jsonAfter returns the JSON line that follows marker in a tool's output.
func jsonAfter(out, marker string) (string, bool) {
	_, rest, ok := strings.Cut(out, marker)
	if !ok {
		return "", false
	}
	for _, line := range strings.Split(rest, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "{") {
			return line, true
		}
	}
	return "", false
}

// reassign moves every partition of every topic onto the given brokers, waits
// for the move to finish and checks the result.
//
//  1. Wait for a reassignment that is already running (an earlier attempt).
//  2. Read the current assignment; if nothing is off the target brokers,
//     there is nothing to do.
//  3. Execute the assignment Kafka itself proposes for those brokers.
//  4. Poll until Kafka reports every partition's reassignment complete.
//  5. Read the assignment again and fail unless every replica is on a target
//     broker.
func (w *Worker) reassign(ctx context.Context, brokerList string) error {
	var brokers []int
	for _, s := range strings.Split(brokerList, ",") {
		id, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("bad broker list %q", brokerList)
		}
		brokers = append(brokers, id)
	}
	server := []string{"--bootstrap-server", w.cfg.Addr}
	deadline := time.Now().Add(reassignWait)
	wait := func(what string) error {
		if time.Now().After(deadline) {
			return fmt.Errorf("reassignment still in progress after %v (%s)", reassignWait, what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	}

	// 1. An earlier attempt may still be moving data.
	for {
		out, err := w.tool(ctx, classReassign, append(server, "--list")...)
		if err != nil {
			return err
		}
		if strings.Contains(out, "No partition reassignments found") {
			break
		}
		if err := wait("earlier reassignment"); err != nil {
			return err
		}
	}

	out, err := w.tool(ctx, classTopics, append(server, "--list")...)
	if err != nil {
		return err
	}
	type topic struct {
		Topic string `json:"topic"`
	}
	var topics []topic
	for _, line := range strings.Split(out, "\n") {
		if name := strings.TrimSpace(line); name != "" && !strings.ContainsAny(name, " \t") {
			topics = append(topics, topic{name})
		}
	}
	if len(topics) == 0 {
		log.Printf("[kafka] reassign: no topics")
		return nil
	}
	topicsFile := filepath.Join(w.cfg.DataDir, "reassign-topics.json")
	planFile := filepath.Join(w.cfg.DataDir, "reassign-plan.json")
	raw, _ := json.Marshal(map[string]any{"version": 1, "topics": topics})
	if err := os.WriteFile(topicsFile, raw, 0o644); err != nil {
		return err
	}

	// generate asks Kafka for the current assignment and a proposal for the
	// target brokers. It fails until the new broker has registered.
	generate := func() (current assignment, proposed string, err error) {
		err = retry(ctx, time.Until(deadline), func() error {
			out, err := w.tool(ctx, classReassign, append(server,
				"--topics-to-move-json-file", topicsFile, "--broker-list", brokerList, "--generate")...)
			if err != nil {
				return err
			}
			cur, ok1 := jsonAfter(out, "Current partition replica assignment")
			prop, ok2 := jsonAfter(out, "Proposed partition reassignment configuration")
			if !ok1 || !ok2 {
				return fmt.Errorf("unexpected --generate output: %s", lastLine(out))
			}
			proposed = prop
			return json.Unmarshal([]byte(cur), &current)
		})
		return current, proposed, err
	}

	// 2. Already where it should be?
	current, proposed, err := generate()
	if err != nil {
		return err
	}
	if len(current.misplaced(brokers)) == 0 {
		log.Printf("[kafka] reassign: all %d partition(s) already on brokers %s", len(current.Partitions), brokerList)
		return nil
	}

	// 3. Start the move.
	if err := os.WriteFile(planFile, []byte(proposed), 0o644); err != nil {
		return err
	}
	if _, err := w.tool(ctx, classReassign, append(server, "--reassignment-json-file", planFile, "--execute")...); err != nil {
		return err
	}
	log.Printf("[kafka] reassign: moving %d partition(s) of %d topic(s) to brokers %s",
		len(current.misplaced(brokers)), len(topics), brokerList)

	// 4. Wait for Kafka to report every partition complete.
	reported := -1
	for {
		out, err := w.tool(ctx, classReassign, append(server, "--reassignment-json-file", planFile, "--verify")...)
		if err != nil {
			return err
		}
		pending := strings.Count(out, "is still in progress")
		if pending == 0 {
			break
		}
		if pending != reported {
			log.Printf("[kafka] reassign: %d partition(s) still moving", pending)
			reported = pending
		}
		if err := wait(fmt.Sprintf("%d partition(s) moving", pending)); err != nil {
			return err
		}
	}

	// 5. Check the outcome rather than trusting the tool's exit code.
	current, _, err = generate()
	if err != nil {
		return err
	}
	if left := current.misplaced(brokers); len(left) > 0 {
		return fmt.Errorf("reassignment finished but %d partition(s) are still off brokers %s: %s",
			len(left), brokerList, strings.Join(left, " "))
	}
	log.Printf("[kafka] reassign: done, all %d partition(s) on brokers %s", len(current.Partitions), brokerList)
	return nil
}
