package timectl

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/dstate/internal/control"
)

// fakeSystem records what the worker asked of ntpd and the clock.
type fakeSystem struct {
	confs  []string
	shifts []time.Duration
	err    error
}

func (s *fakeSystem) Apply(conf string, shift time.Duration) error {
	s.confs = append(s.confs, conf)
	s.shifts = append(s.shifts, shift)
	return s.err
}

func testWorker(t *testing.T) (*Worker, *fakeSystem) {
	sys := &fakeSystem{}
	w := NewWorker(WorkerConfig{
		Node:      "n1",
		StateFile: filepath.Join(t.TempDir(), "state", "time.json"),
		External:  []string{"ext"},
		System:    sys,
	})
	return w, sys
}

func configure(c Config) control.Order {
	return control.Order{OrderSpec: control.OrderSpec{Node: "n1", Kind: OrderConfigure, Args: c.args()}}
}

func TestExecuteStoresStateAndShiftsOnce(t *testing.T) {
	w, sys := testWorker(t)
	ctx := context.Background()

	// Bootstrap in internal mode, an operator sets the time: stored only.
	if err := w.Execute(ctx, configure(Config{Epoch: 1, Mode: ModeInternal, Persist: ModeInternal, Local: true})); err != nil {
		t.Fatal(err)
	}
	if err := w.Execute(ctx, configure(Config{Epoch: 2, Mode: ModeInternal, Persist: ModeInternal, Local: true, DeltaMS: 5000})); err != nil {
		t.Fatal(err)
	}
	state, err := w.load()
	if err != nil || state.Epoch != 2 || state.Mode != ModeInternal || state.DeltaMS != 5000 {
		t.Fatalf("stored %+v (%v), want epoch 2, internal, 5000ms", state, err)
	}
	if sys.shifts[0] != 0 || sys.shifts[1] != 0 {
		t.Fatalf("clock shifted when the time was set: %v", sys.shifts)
	}

	// Next bootstrap: the correction is applied, and only once even if the
	// order is delivered again.
	next := configure(Config{Epoch: 3, Mode: ModeInternal, Persist: ModeInternal, Local: true, ShiftMS: 5000, ShiftFrom: 2})
	for range 2 {
		if err := w.Execute(ctx, next); err != nil {
			t.Fatal(err)
		}
	}
	if len(sys.shifts) != 3 || sys.shifts[2] != 5*time.Second {
		t.Fatalf("shifts %v, want exactly one of 5s at the end", sys.shifts)
	}
	if state, _ := w.load(); state.Epoch != 3 || state.DeltaMS != 0 {
		t.Fatalf("stored %+v, want epoch 3 with the delta cleared", state)
	}
}

func TestExecuteDoesNotShiftANodeThatMissedTheCorrection(t *testing.T) {
	w, sys := testWorker(t)
	if err := w.Execute(context.Background(), configure(Config{Epoch: 3, Mode: ModeInternal, Persist: ModeInternal, ShiftMS: 5000, ShiftFrom: 2})); err != nil {
		t.Fatal(err)
	}
	if sys.shifts[0] != 0 {
		t.Fatalf("a node at epoch 0 shifted by %s for a correction made at epoch 2", sys.shifts[0])
	}
}

func TestExecuteFailureLeavesStateForARetry(t *testing.T) {
	w, sys := testWorker(t)
	sys.err = errors.New("access denied")
	if err := w.Execute(context.Background(), configure(Config{Epoch: 1, Mode: ModeAuto, Persist: ModeAuto})); err == nil {
		t.Fatal("no error")
	}
	if state, _ := w.load(); state.Epoch != 0 {
		t.Fatalf("epoch %d stored although ntpd was not configured", state.Epoch)
	}
}

func TestRender(t *testing.T) {
	for name, tc := range map[string]struct {
		c    Config
		want []string
		not  []string
	}{
		"auto":   {Config{Mode: ModeAuto, Servers: []string{"ext"}, Peers: []string{"h2"}}, []string{"server ext iburst", "peer h2", "tos orphan"}, []string{"127.127.1.0"}},
		"orphan": {Config{Mode: ModeOrphan, Peers: []string{"h2"}}, []string{"peer h2", "tos orphan"}, []string{"server "}},
		"master": {Config{Mode: ModeInternal, Local: true}, []string{"server 127.127.1.0", "fudge 127.127.1.0"}, []string{"peer", "orphan"}},
		"client": {Config{Mode: ModeInternal, Servers: []string{"h1"}}, []string{"server h1 iburst"}, []string{"127.127.1.0", "peer"}},
	} {
		conf := render(tc.c)
		for _, s := range tc.want {
			if !strings.Contains(conf, s) {
				t.Errorf("%s: %q missing in:\n%s", name, s, conf)
			}
		}
		for _, s := range tc.not {
			if strings.Contains(conf, s) {
				t.Errorf("%s: unexpected %q in:\n%s", name, s, conf)
			}
		}
	}
}

func probe(t *testing.T, w *Worker) Report {
	t.Helper()
	w.once.Do(func() {}) // no background watch: the test feeds the readings
	w.mu.Lock()
	w.ready = true
	w.mu.Unlock()
	r, err := w.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r.(Report)
}

func TestAvailabilityToleratesALostPacket(t *testing.T) {
	w, _ := testWorker(t)
	fail := errors.New("timeout")

	w.record("ext", time.Time{}, fail)
	if r := probe(t, w); len(r.External) != 0 {
		t.Fatalf("available without ever answering: %v", r.External)
	}
	w.record("ext", time.Now(), nil)
	w.record("ext", time.Time{}, fail)
	w.record("ext", time.Time{}, fail)
	if r := probe(t, w); len(r.External) != 1 {
		t.Fatal("unavailable after two missed answers")
	}
	w.record("ext", time.Time{}, fail)
	if r := probe(t, w); len(r.External) != 0 {
		t.Fatal("still available after three missed answers")
	}
}

func TestJumpDetection(t *testing.T) {
	w, _ := testWorker(t)

	// A steady source, even one far from the local clock, is not a jump.
	for range 3 {
		w.record("ext", time.Now().Add(time.Hour), nil)
	}
	if r := probe(t, w); r.Jump != "" {
		t.Fatalf("steady source reported as jumping: %s", r.Jump)
	}
	// It moves by 3 seconds.
	w.record("ext", time.Now().Add(time.Hour+3*time.Second), nil)
	if r := probe(t, w); r.Jump == "" {
		t.Fatal("3s move within the window not reported")
	}
	// The report stays until the node is configured again.
	w.record("ext", time.Now().Add(time.Hour+3*time.Second), nil)
	if r := probe(t, w); r.Jump == "" {
		t.Fatal("jump forgotten before it was acted on")
	}
	if err := w.Execute(context.Background(), configure(Config{Epoch: 1, Mode: ModeOrphan, Persist: ModeInternal})); err != nil {
		t.Fatal(err)
	}
	if r := probe(t, w); r.Jump != "" || r.State.Mode != ModeInternal {
		t.Fatalf("after configuring: %+v, want no jump and internal stored", r)
	}
}

func TestSlowDriftIsNotAJump(t *testing.T) {
	w, _ := testWorker(t)
	w.cfg.JumpWindow = 50 * time.Millisecond
	w.record("ext", time.Now(), nil)
	time.Sleep(80 * time.Millisecond)
	// 3s away from the first reading, but that one is outside the window.
	w.record("ext", time.Now().Add(3*time.Second), nil)
	if r := probe(t, w); r.Jump != "" {
		t.Fatalf("move outside the window reported: %s", r.Jump)
	}
}

// serveNTP answers SNTP requests with the local time plus offset.
func serveNTP(t *testing.T, stratum byte, offset time.Duration) string {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	go func() {
		buf := make([]byte, 48)
		for {
			_, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			resp := make([]byte, 48)
			resp[0], resp[1] = 0x24, stratum
			since := time.Now().Add(offset).Sub(ntpEpoch)
			binary.BigEndian.PutUint32(resp[40:], uint32(since/time.Second))
			binary.BigEndian.PutUint32(resp[44:], uint32(uint64(since%time.Second)<<32/1e9))
			_, _ = conn.WriteTo(resp, addr)
		}
	}()
	return conn.LocalAddr().String()
}

func TestQueryTime(t *testing.T) {
	at, err := queryTime(serveNTP(t, 2, time.Hour), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if off := time.Until(at) - time.Hour; off < -100*time.Millisecond || off > 100*time.Millisecond {
		t.Fatalf("server time off by %s", off)
	}
	// Stratum 0: the server itself has no time.
	if _, err := queryTime(serveNTP(t, 0, 0), time.Second); err == nil {
		t.Fatal("unsynchronized server accepted")
	}
	// Nobody there.
	if _, err := queryTime("127.0.0.1:9", 200*time.Millisecond); err == nil {
		t.Fatal("no error from a dead address")
	}
}
