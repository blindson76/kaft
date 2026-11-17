package taskctl

import (
	"context"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"example.com/dstate/internal/control"
)

// The test binary is also the service the worker runs: with TASK_HELPER set it
// behaves as told instead of running the tests.
func TestMain(m *testing.M) {
	switch os.Getenv("TASK_HELPER") {
	case "":
		os.Exit(m.Run())
	case "run":
		fmt.Printf("instance=%s node=%s role=%s/%s mode=%s args=%v\n", os.Getenv("KAFT_INSTANCE"), os.Getenv("KAFT_NODE"),
			os.Getenv("KAFT_ROLE"), os.Getenv("KAFT_REPLICA"), os.Getenv("MODE"), os.Args[1:])
		time.Sleep(time.Minute)
		os.Exit(0)
	case "fail":
		os.Exit(3)
	case "spawn":
		// Like a launcher script: the real service is a child process.
		child := osexec.Command(os.Args[0])
		child.Env = append(os.Environ(), "TASK_HELPER=run")
		if err := child.Start(); err != nil {
			os.Exit(4)
		}
		fmt.Printf("child=%d\n", child.Process.Pid)
		_ = child.Wait()
		os.Exit(0)
	}
}

func helperWorker(t *testing.T, mode string, logDir string) *Worker {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(WorkerConfig{
		Task:        "svc",
		Node:        "n1",
		Command:     exe,
		Args:        []string{"--name={task}-{instance}@{node}"},
		Env:         map[string]string{"TASK_HELPER": mode},
		LogDir:      logDir,
		StopTimeout: time.Second,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})
	return w
}

func order(kind string, slot int, gen uint64) control.Order {
	return control.Order{OrderSpec: control.OrderSpec{Node: "n1", Kind: kind, Args: map[string]any{"slot": slot, "gen": gen}}}
}

func report(t *testing.T, w *Worker) Report {
	t.Helper()
	r, err := w.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r.(Report)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func do(t *testing.T, w *Worker, o control.Order) {
	t.Helper()
	if err := w.Execute(context.Background(), o); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRunsAndStopsInstances(t *testing.T) {
	dir := t.TempDir()
	w := helperWorker(t, "run", dir)
	if r := report(t, w); len(r.Instances) != 0 {
		t.Fatalf("fresh worker reports %v", r.Instances)
	}

	do(t, w, order(OrderRun, 0, 1))
	do(t, w, order(OrderRun, 1, 2))
	r := report(t, w)
	if len(r.Instances) != 2 || !r.Instances[0].Running || r.Instances[0].PID == 0 {
		t.Fatalf("after two runs: %+v", r.Instances)
	}
	pid := r.Instances[0].PID

	// The same order again changes nothing.
	do(t, w, order(OrderRun, 0, 1))
	if got := report(t, w).Instances[0].PID; got != pid {
		t.Fatalf("repeated run order restarted the instance: pid %d -> %d", pid, got)
	}
	// A newer placement of the slot replaces the process.
	do(t, w, order(OrderRun, 0, 3))
	if p := report(t, w).Instances[0]; p.PID == pid || p.Gen != 3 || !p.Running {
		t.Fatalf("new placement of slot 0: %+v (old pid %d)", p, pid)
	}
	// A stop for an old placement is ignored, the current one stops it.
	do(t, w, order(OrderStop, 1, 1))
	if len(report(t, w).Instances) != 2 {
		t.Fatal("stop for an old placement stopped the instance")
	}
	do(t, w, order(OrderStop, 1, 2))
	if r := report(t, w); len(r.Instances) != 1 || r.Instances[0].Slot != 0 {
		t.Fatalf("after stopping slot 1: %+v", r.Instances)
	}

	// The placeholders reach the process.
	eventually(t, "instance output", func() bool {
		out, _ := os.ReadFile(filepath.Join(dir, "svc-0.log"))
		return strings.Contains(string(out), "instance=0 node=n1 role=/0 mode= args=[--name=svc-0@n1]")
	})
}

func TestWorkerReportsAProcessThatExits(t *testing.T) {
	w := helperWorker(t, "fail", t.TempDir())
	do(t, w, order(OrderRun, 0, 1))
	eventually(t, "the exit to be reported", func() bool {
		r := report(t, w)
		return len(r.Instances) == 1 && !r.Instances[0].Running
	})
	if p := report(t, w).Instances[0]; p.Exit != "exited with code 3" {
		t.Fatalf("exit reported as %q", p.Exit)
	}
	// Forgotten once the controller has seen it.
	do(t, w, order(OrderStop, 0, 1))
	if r := report(t, w); len(r.Instances) != 0 {
		t.Fatalf("exited instance not forgotten: %+v", r.Instances)
	}
}

func TestWorkerRunOrderFailsForAMissingCommand(t *testing.T) {
	w := NewWorker(WorkerConfig{Task: "svc", Node: "n1", Command: filepath.Join(t.TempDir(), "nope.exe"), LogDir: t.TempDir()})
	if err := w.Execute(context.Background(), order(OrderRun, 0, 1)); err == nil {
		t.Fatal("run of a missing command succeeded")
	}
}

func TestWorkerCloseStopsEverything(t *testing.T) {
	w := helperWorker(t, "run", t.TempDir())
	do(t, w, order(OrderRun, 0, 1))
	do(t, w, order(OrderRun, 1, 2))
	procs := []*proc{w.procs[0], w.procs[1]}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, p := range procs {
		select {
		case <-p.exited:
		default:
			t.Fatalf("instance %d still running after Close", p.slot)
		}
	}
	if err := w.Execute(context.Background(), order(OrderRun, 0, 3)); err == nil {
		t.Fatal("closed worker took an order")
	}
}

// A process left behind by an earlier run of the program (one that died
// without stopping it) is ended by the next worker's first probe.
func TestWorkerEndsWhatAnEarlierRunLeftBehind(t *testing.T) {
	dir := t.TempDir()
	old := helperWorker(t, "run", dir)
	do(t, old, order(OrderRun, 0, 1))
	left := old.procs[0]

	w := helperWorker(t, "run", dir)
	if r := report(t, w); len(r.Instances) != 0 {
		t.Fatalf("new worker reports %+v", r.Instances)
	}
	select {
	case <-left.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("left-behind process still running")
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*.pid")); len(files) != 0 {
		t.Fatalf("pid files left: %v", files)
	}
}

// A pid file whose pid now belongs to another program is not acted on.
func TestWorkerLeavesAReusedPidAlone(t *testing.T) {
	dir := t.TempDir()
	// This test process: its image is not the recorded one.
	pidFile := filepath.Join(dir, "svc-0.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n%s\n", os.Getpid(), filepath.Join(dir, "other.exe"))), 0o644); err != nil {
		t.Fatal(err)
	}
	w := helperWorker(t, "run", dir)
	report(t, w) // would have killed this test
	if _, err := os.Stat(pidFile); err == nil {
		t.Fatal("stale pid file kept")
	}
}

// Stopping an instance also stops the processes it started, as a launcher
// script's service.
func TestWorkerStopsTheProcessesAnInstanceStarted(t *testing.T) {
	dir := t.TempDir()
	w := helperWorker(t, "spawn", dir)
	do(t, w, order(OrderRun, 0, 1))
	var child int
	eventually(t, "the child to start", func() bool {
		out, _ := os.ReadFile(filepath.Join(dir, "svc-0.log"))
		_, after, ok := strings.Cut(string(out), "child=")
		if !ok {
			return false
		}
		child, _ = strconv.Atoi(strings.TrimSpace(strings.SplitN(after, "\n", 2)[0]))
		return child > 0
	})
	if !running(child) {
		t.Fatal("child not running")
	}
	do(t, w, order(OrderStop, 0, 1))
	eventually(t, "the child to be gone", func() bool { return !running(child) })
}

// A role's args come after the task's, its env overrides the task's, and the
// placeholders of other roles' placements are filled in from the order.
func TestWorkerRunsARoleWithItsArguments(t *testing.T) {
	dir := t.TempDir()
	w := helperWorker(t, "run", dir)
	w.cfg.Env["MODE"] = "task"
	w.cfg.Roles = map[string]RoleConfig{
		"slave": {Args: []string{"--replicaof", "{host:master}", "{role}-{replica}"}, Env: map[string]string{"MODE": "slave"}},
	}
	o := order(OrderRun, 1, 1)
	o.Args["role"], o.Args["replica"] = "slave", 0
	o.Args["vars"] = map[string]any{"host:master": "10.0.0.1", "node:master": "n1"}
	do(t, w, o)
	eventually(t, "the slave's output", func() bool {
		out, _ := os.ReadFile(filepath.Join(dir, "svc-1.log"))
		return strings.Contains(string(out), "instance=1 node=n1 role=slave/0 mode=slave args=[--name=svc-1@n1 --replicaof 10.0.0.1 slave-0]")
	})
}
