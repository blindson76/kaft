package taskctl

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.com/dstate/internal/control"
	"example.com/dstate/internal/util"
)

// WorkerConfig configures a task's worker on one node.
//
// Command, Args, Dir and the Env values may use {task}, {instance} (the slot,
// from 0), {node}, {role}, {replica} (the number within the role) and, for a
// role the instance refers to, {node:R} and {host:R}. The process also gets
// KAFT_TASK, KAFT_INSTANCE, KAFT_NODE, KAFT_GEN, KAFT_ROLE and KAFT_REPLICA
// in its environment.
type WorkerConfig struct {
	Task string
	Node string

	Command string
	Args    []string
	Env     map[string]string
	Dir     string
	// Roles holds what each role adds: its Args come after the task's, its
	// Env entries override the task's.
	Roles map[string]RoleConfig
	// LogDir holds each instance's output (<task>-<instance>.log) and pid
	// file.
	LogDir string
	// StopTimeout is how long a process gets to exit after being asked to,
	// where the OS can ask (not on Windows), before it is killed. Default 10s.
	StopTimeout time.Duration
}

// RoleConfig is what a role adds to the task's command line and environment.
type RoleConfig struct {
	Args []string
	Env  map[string]string
}

// Worker runs a task's processes on one node.
type Worker struct {
	cfg  WorkerConfig
	boot string

	// life ends with Close; the process watchers run on bg.
	life   context.Context
	cancel context.CancelFunc
	bg     sync.WaitGroup

	// ops serializes orders and Close.
	ops  sync.Mutex
	once sync.Once

	// mu guards procs.
	mu    sync.Mutex
	procs map[int]*proc
}

type proc struct {
	slot   int
	gen    uint64
	pid    int
	group  *util.Group
	exited chan struct{}
	// Set when the process exits, under Worker.mu.
	done bool
	exit string
}

func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = 10 * time.Second
	}
	id := make([]byte, 6)
	_, _ = rand.Read(id)
	life, cancel := context.WithCancel(context.Background())
	return &Worker{cfg: cfg, boot: hex.EncodeToString(id), life: life, cancel: cancel, procs: map[int]*proc{}}
}

func (w *Worker) Name() string { return w.cfg.Task }

// Probe reports the processes. The first probe ends what an earlier run of
// this program on this node left running, so the report starts from nothing.
func (w *Worker) Probe(ctx context.Context) (any, error) {
	w.once.Do(w.cleanup)
	w.mu.Lock()
	defer w.mu.Unlock()
	r := Report{Boot: w.boot}
	for _, slot := range slices.Sorted(maps.Keys(w.procs)) {
		p := w.procs[slot]
		r.Instances = append(r.Instances, Proc{Slot: slot, Gen: p.gen, PID: p.pid, Running: !p.done, Exit: p.exit})
	}
	return r, nil
}

// Execute carries out one order. Both kinds are idempotent.
func (w *Worker) Execute(ctx context.Context, order control.Order) error {
	w.ops.Lock()
	defer w.ops.Unlock()
	if err := cmp.Or(w.life.Err(), ctx.Err()); err != nil {
		return err
	}
	slot, gen := int(number(order.Args["slot"])), uint64(number(order.Args["gen"]))

	w.mu.Lock()
	p := w.procs[slot]
	w.mu.Unlock()

	switch order.Kind {
	case OrderRun:
		if p != nil && p.gen == gen {
			return nil // already started; if it has exited, the report says so
		}
		if p != nil {
			w.stop(ctx, p)
		}
		role, _ := order.Args["role"].(string)
		vars := map[string]string{}
		switch raw := order.Args["vars"].(type) {
		case map[string]string: // placed by this process
			vars = raw
		case map[string]any: // through JSON
			for k, v := range raw {
				vars[k], _ = v.(string)
			}
		}
		return w.start(slot, gen, role, int(number(order.Args["replica"])), vars)
	case OrderStop:
		if p != nil && p.gen == gen {
			w.stop(ctx, p)
		}
		return nil
	}
	return fmt.Errorf("unknown order kind %q", order.Kind)
}

// Close implements control.Closer: it stops every process of the task on this
// node, killing what has not exited when ctx ends.
func (w *Worker) Close(ctx context.Context) error {
	w.cancel()
	w.ops.Lock()
	defer w.ops.Unlock()

	w.mu.Lock()
	procs := slices.Collect(maps.Values(w.procs))
	w.mu.Unlock()
	if len(procs) > 0 {
		log.Printf("[%s] stopping %d instance(s)", w.cfg.Task, len(procs))
	}
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Go(func() { w.stop(ctx, p) })
	}
	wg.Wait()
	w.bg.Wait()
	return nil
}

// --- processes -------------------------------------------------------------

func (w *Worker) base(slot int) string {
	return filepath.Join(w.cfg.LogDir, w.cfg.Task+"-"+strconv.Itoa(slot))
}

func (w *Worker) start(slot int, gen uint64, role string, replica int, vars map[string]string) error {
	pairs := []string{"{task}", w.cfg.Task, "{instance}", strconv.Itoa(slot), "{node}", w.cfg.Node,
		"{role}", role, "{replica}", strconv.Itoa(replica)}
	for _, k := range slices.Sorted(maps.Keys(vars)) {
		pairs = append(pairs, "{"+k+"}", vars[k])
	}
	r := strings.NewReplacer(pairs...)
	rc := w.cfg.Roles[role]
	var args []string
	for _, a := range slices.Concat(w.cfg.Args, rc.Args) {
		args = append(args, r.Replace(a))
	}
	cmd := exec.Command(r.Replace(w.cfg.Command), args...)
	cmd.Dir = r.Replace(w.cfg.Dir)
	cmd.Env = append(os.Environ(),
		"KAFT_TASK="+w.cfg.Task,
		"KAFT_INSTANCE="+strconv.Itoa(slot),
		"KAFT_NODE="+w.cfg.Node,
		"KAFT_GEN="+strconv.FormatUint(gen, 10),
		"KAFT_ROLE="+role,
		"KAFT_REPLICA="+strconv.Itoa(replica),
	)
	env := maps.Clone(w.cfg.Env)
	if env == nil {
		env = map[string]string{}
	}
	maps.Copy(env, rc.Env)
	for _, k := range slices.Sorted(maps.Keys(env)) {
		cmd.Env = append(cmd.Env, k+"="+r.Replace(env[k]))
	}

	if err := os.MkdirAll(w.cfg.LogDir, 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(w.base(slot)+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "\n=== %s: kaft starts instance %d%s (placement %d) on %s: %s %s\n", time.Now().Format(time.RFC3339),
		slot, roleLabel(role, replica), gen, w.cfg.Node, cmd.Path, strings.Join(args, " "))
	cmd.Stdout, cmd.Stderr = out, out

	group, err := util.StartGroup(cmd)
	if err != nil {
		out.Close()
		return fmt.Errorf("start %s: %w", cmd.Path, err)
	}
	p := &proc{slot: slot, gen: gen, pid: cmd.Process.Pid, group: group, exited: make(chan struct{})}
	// Lets a later run of this program end the process if this one dies
	// without stopping it; it checks the executable before it kills.
	image, _ := filepath.Abs(cmd.Path)
	_ = os.WriteFile(w.base(slot)+".pid", []byte(fmt.Sprintf("%d\n%s\n", p.pid, image)), 0o644)

	w.mu.Lock()
	w.procs[slot] = p
	w.mu.Unlock()
	log.Printf("[%s] instance %d%s started (pid %d)", w.cfg.Task, slot, roleLabel(role, replica), p.pid)

	w.bg.Go(func() {
		err := cmd.Wait()
		out.Close()
		group.Release()
		why := "exited with code 0"
		var ee *exec.ExitError
		switch {
		case errors.As(err, &ee):
			why = fmt.Sprintf("exited with code %d", ee.ExitCode())
		case err != nil:
			why = err.Error()
		}
		w.mu.Lock()
		p.done, p.exit = true, why
		current := w.procs[slot] == p
		w.mu.Unlock()
		if current {
			_ = os.Remove(w.base(slot) + ".pid")
		}
		close(p.exited)
		if w.life.Err() == nil {
			log.Printf("[%s] instance %d (pid %d) %s", w.cfg.Task, slot, p.pid, why)
		}
	})
	return nil
}

// stop ends a process and forgets it: asked to exit first where the OS can
// ask, killed after StopTimeout or when ctx ends.
func (w *Worker) stop(ctx context.Context, p *proc) {
	select {
	case <-p.exited:
	default:
		_ = p.group.Terminate()
		t := time.NewTimer(w.cfg.StopTimeout)
		select {
		case <-p.exited:
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
		_ = p.group.Kill()
		<-p.exited
		log.Printf("[%s] instance %d (pid %d) stopped", w.cfg.Task, p.slot, p.pid)
	}
	w.mu.Lock()
	if w.procs[p.slot] == p {
		delete(w.procs, p.slot)
	}
	w.mu.Unlock()
	_ = os.Remove(w.base(p.slot) + ".pid")
}

// cleanup ends the processes an earlier run of this program on this node left
// behind, found by their pid files. A pid is only acted on while it still runs
// the executable recorded with it: pids are reused.
func (w *Worker) cleanup() {
	files, _ := filepath.Glob(filepath.Join(w.cfg.LogDir, w.cfg.Task+"-*.pid"))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		lines := strings.SplitN(strings.TrimSpace(string(raw)), "\n", 2)
		if pid, err := strconv.Atoi(strings.TrimSpace(lines[0])); err == nil && len(lines) == 2 {
			switch killed, err := util.KillGroup(pid, strings.TrimSpace(lines[1])); {
			case killed && err != nil:
				log.Printf("[%s] could not end pid %d left by an earlier run: %v", w.cfg.Task, pid, err)
			case killed:
				log.Printf("[%s] ended pid %d left by an earlier run", w.cfg.Task, pid)
			}
		}
		_ = os.Remove(f)
	}
}

// roleLabel names an instance's role for the log, e.g. " (slave 1)".
func roleLabel(role string, replica int) string {
	if role == "" {
		return ""
	}
	return fmt.Sprintf(" (%s %d)", role, replica)
}
