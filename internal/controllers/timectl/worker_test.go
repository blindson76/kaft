package timectl

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"example.com/dstate/internal/control"
)

// fakeSystem records what the worker asked of ntpd and the clock.
type fakeSystem struct {
	confs    []string
	shifts   []time.Duration
	err      error
	shiftErr error
	status   NTPDStatus
}

func (s *fakeSystem) Apply(conf string) error {
	s.confs = append(s.confs, conf)
	return s.err
}

func (s *fakeSystem) Shift(d time.Duration) error {
	s.shifts = append(s.shifts, d)
	return s.shiftErr
}

func (s *fakeSystem) Status(context.Context) NTPDStatus { return s.status }

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

func TestPendingCorrectionIsAppliedOnProbe(t *testing.T) {
	w, sys := testWorker(t)
	ctx := context.Background()

	if err := w.Execute(ctx, configure(Config{Epoch: 1, Mode: ModeInternal, Persist: ModeInternal, Local: true})); err != nil {
		t.Fatal(err)
	}
	probe(t, w)
	if len(sys.shifts) != 0 {
		t.Fatalf("clock shifted without a correction: %v", sys.shifts)
	}

	// An operator sets the time: the node stores the correction...
	if err := w.Execute(ctx, configure(Config{Epoch: 2, Mode: ModeInternal, Persist: ModeInternal, Local: true, DeltaMS: 5000})); err != nil {
		t.Fatal(err)
	}
	if state, err := w.load(); err != nil || state.Epoch != 2 || state.DeltaMS != 5000 {
		t.Fatalf("stored %+v (%v), want epoch 2 with 5000ms pending", state, err)
	}
	// ...and applies it at the next probe, once.
	for range 3 {
		r := probe(t, w)
		if r.State.DeltaMS != 0 || r.State.AppliedDeltaMS != 5000 || r.ShiftError != "" {
			t.Fatalf("reported %+v, want the correction applied and cleared", r)
		}
	}
	if len(sys.shifts) != 1 || sys.shifts[0] != 5*time.Second {
		t.Fatalf("shifts %v, want exactly one of 5s", sys.shifts)
	}
	// A configuration redelivered, or a later one, does not bring it back.
	if err := w.Execute(ctx, configure(Config{Epoch: 3, Mode: ModeInternal, Persist: ModeInternal})); err != nil {
		t.Fatal(err)
	}
	probe(t, w)
	if len(sys.shifts) != 1 {
		t.Fatalf("shifts %v, want still one", sys.shifts)
	}
}

func TestPendingCorrectionSurvivesANewConfiguration(t *testing.T) {
	w, sys := testWorker(t)
	ctx := context.Background()
	sys.shiftErr = errors.New("access denied")

	if err := w.Execute(ctx, configure(Config{Epoch: 1, Mode: ModeInternal, Persist: ModeInternal, DeltaMS: 2000})); err != nil {
		t.Fatal(err)
	}
	if r := probe(t, w); r.ShiftError == "" || r.State.DeltaMS != 2000 {
		t.Fatalf("reported %+v, want the failure and the correction still pending", r)
	}
	// Not tried again at once...
	probe(t, w)
	if len(sys.shifts) != 1 {
		t.Fatalf("shifts %v, want one attempt before the retry interval", sys.shifts)
	}
	// ...a new master changes nothing about it...
	if err := w.Execute(ctx, configure(Config{Epoch: 2, Mode: ModeInternal, Persist: ModeInternal, Servers: []string{"h2"}})); err != nil {
		t.Fatal(err)
	}
	if state, _ := w.load(); state.DeltaMS != 2000 {
		t.Fatalf("stored %+v, want the correction kept", state)
	}
	// ...and after the retry interval it goes through.
	sys.shiftErr = nil
	w.shiftTried = time.Now().Add(-shiftRetry)
	if r := probe(t, w); r.ShiftError != "" || r.State.DeltaMS != 0 {
		t.Fatalf("reported %+v, want the correction applied", r)
	}

	// Going to auto drops a correction not applied yet.
	sys.shiftErr = errors.New("access denied")
	if err := w.Execute(ctx, configure(Config{Epoch: 3, Mode: ModeInternal, Persist: ModeInternal, DeltaMS: 1000})); err != nil {
		t.Fatal(err)
	}
	if err := w.Execute(ctx, configure(Config{Epoch: 4, Mode: ModeAuto, Persist: ModeAuto})); err != nil {
		t.Fatal(err)
	}
	if state, _ := w.load(); state.DeltaMS != 0 {
		t.Fatalf("stored %+v, want no correction in auto mode", state)
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

	// Back, but it has to answer for a while first.
	w.cfg.Recover = 50 * time.Millisecond
	w.record("ext", time.Now(), nil)
	if r := probe(t, w); len(r.External) != 0 {
		t.Fatal("a lost server counted as available at its first answer")
	}
	time.Sleep(60 * time.Millisecond)
	w.record("ext", time.Now(), nil)
	if r := probe(t, w); len(r.External) != 1 {
		t.Fatal("not available after answering for the recovery time")
	}
}

func TestJumpDetection(t *testing.T) {
	w, _ := testWorker(t)
	w.cfg.HoldDown = 100 * time.Millisecond

	// A steady source, even one far from the local clock, is not a jump.
	for range 3 {
		w.record("ext", time.Now().Add(time.Hour), nil)
	}
	if r := probe(t, w); len(r.Faults) != 0 || len(r.External) != 1 {
		t.Fatalf("steady source reported as faulty: %+v", r)
	}
	// It moves by 3 seconds: held back.
	w.record("ext", time.Now().Add(time.Hour+3*time.Second), nil)
	if r := probe(t, w); r.Faults["ext"] == "" || len(r.External) != 0 {
		t.Fatalf("3s move within the window not reported: %+v", r)
	}
	// Configuring the node does not end the hold-down...
	if err := w.Execute(context.Background(), configure(Config{Epoch: 1, Mode: ModeOrphan, Persist: ModeInternal})); err != nil {
		t.Fatal(err)
	}
	w.record("ext", time.Now().Add(time.Hour+3*time.Second), nil)
	if r := probe(t, w); r.Faults["ext"] == "" {
		t.Fatal("fault forgotten before the hold-down ended")
	}
	// ...time does.
	time.Sleep(120 * time.Millisecond)
	if r := probe(t, w); len(r.Faults) != 0 || len(r.External) != 1 {
		t.Fatalf("after the hold-down: %+v, want ext usable again", r)
	}
}

func TestNtpdVerdictsHoldSourcesBack(t *testing.T) {
	sys := &fakeSystem{}
	w := NewWorker(WorkerConfig{
		Node:      "n1",
		StateFile: filepath.Join(t.TempDir(), "time.json"),
		External:  []string{"ext.example:123", "10.0.0.9"},
		System:    sys,
	})
	w.lookup = func(host string) ([]string, error) {
		if host == "ext.example" {
			return []string{"10.0.0.8"}, nil
		}
		return nil, errors.New("no such host")
	}
	for _, s := range w.cfg.External {
		w.record(s, time.Now(), nil)
	}

	// ntpd lost 10.0.0.9: this node stops offering it, without blaming it.
	w.monitor(NTPDStatus{Running: true, Peers: []Peer{{Addr: "10.0.0.8", State: PeerSync}, {Addr: "10.0.0.9", State: PeerUnreachable}}})
	r := probe(t, w)
	if !slices.Equal(r.External, []string{"ext.example:123"}) || len(r.Faults) != 0 || !r.NTPD.Running {
		t.Fatalf("reported %+v, want only ext.example and no fault", r)
	}
	// ntpd calls ext.example a falseticker: that is the server's fault.
	w.monitor(NTPDStatus{Running: true, Peers: []Peer{{Addr: "10.0.0.8", State: PeerFalseticker}}})
	r = probe(t, w)
	if len(r.External) != 0 || r.Faults["ext.example:123"] == "" {
		t.Fatalf("reported %+v, want ext.example held back as a fault", r)
	}
}

func TestParsePeers(t *testing.T) {
	out := `     remote           refid      st t when poll reach   delay   offset  jitter
==============================================================================
*192.168.1.1     .GPS.            1 u   33   64  377    0.512   -0.027   0.011
+192.168.1.2     .GPS.            1 u   12   64  377    0.330    0.110   0.002
x10.0.0.5        .GPS.            1 u   12   64  377    0.330  912.110   0.002
 10.0.0.6        .INIT.          16 u    -   64    0    0.000    0.000   0.000
 10.0.0.7        .GPS.            1 u  200   64  370    0.330    0.110   0.002
 127.127.1.0     .LOCL.           8 l   12   64  377    0.000    0.000   0.000
`
	want := []Peer{
		{"192.168.1.1", PeerSync}, {"192.168.1.2", PeerCandidate}, {"10.0.0.5", PeerFalseticker},
		{"10.0.0.6", PeerReject}, {"10.0.0.7", PeerUnreachable}, {"127.127.1.0", PeerReject},
	}
	if got := parsePeers(out); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestSlowDriftIsNotAJump(t *testing.T) {
	w, _ := testWorker(t)
	w.cfg.JumpWindow = 50 * time.Millisecond
	w.record("ext", time.Now(), nil)
	time.Sleep(80 * time.Millisecond)
	// 3s away from the first reading, but that one is outside the window.
	w.record("ext", time.Now().Add(3*time.Second), nil)
	if r := probe(t, w); len(r.Faults) != 0 {
		t.Fatalf("move outside the window reported: %v", r.Faults)
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
