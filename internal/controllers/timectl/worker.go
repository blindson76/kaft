package timectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"example.com/dstate/internal/control"
)

// WorkerConfig configures the node-local side.
type WorkerConfig struct {
	Node string
	// StateFile is where the node keeps its State between starts.
	StateFile string
	// External lists the external time servers to watch.
	External []string
	// Interval is how often each server is asked. Defaults to 2s.
	Interval time.Duration
	// A server whose time moves by JumpThreshold or more within JumpWindow,
	// measured against this node's steady clock, is reported as jumping.
	// Defaults: 2s within 10s.
	JumpThreshold time.Duration
	JumpWindow    time.Duration
	// HoldDown is how long a server that jumped, or that ntpd found
	// unreachable or a falseticker, is withheld from the controller before it
	// may be used again. Default 10m.
	HoldDown time.Duration
	// Recover is how long a server that was lost has to answer without a
	// miss before it counts as available again. Default 30s.
	Recover time.Duration
	// System is ntpd and the clock.
	System System
}

// shiftRetry is the pause before a failed clock correction is tried again.
const shiftRetry = 10 * time.Second

// lostAfter is how many requests in a row a server has to miss before it
// counts as unavailable: one lost packet is not an outage.
const lostAfter = 3

// sample is one reading of a server: its time, expressed as an offset from a
// clock nothing can step.
type sample struct {
	at     time.Duration
	offset time.Duration
}

type source struct {
	available bool
	missed    int
	samples   []sample
	// lost is when the server was last lost; back is when it started
	// answering again since.
	lost, back time.Time
	// fault, while before until, keeps the server out of the report and is
	// reported: the fault lies with the server (it jumped, or ntpd calls it a
	// falseticker), so no node is to follow it.
	fault string
	until time.Time
	// local, while before localUntil, keeps the server out of the report: the
	// local ntpd lost it, which may well be this node's problem only.
	local      string
	localUntil time.Time
}

// Worker watches the external time servers and applies time configurations.
type Worker struct {
	cfg   WorkerConfig
	start time.Time
	query func(server string, timeout time.Duration) (time.Time, error)
	// lookup resolves a server name to the addresses ntpq shows for it.
	lookup func(host string) ([]string, error)

	// ops serializes the changes to ntpd, the clock and the state file.
	ops  sync.Mutex
	once sync.Once
	// shiftTried and shiftErr are the last attempt at a pending correction;
	// ops guards them.
	shiftTried time.Time
	shiftErr   string

	// mu guards the fields below.
	mu      sync.Mutex
	ready   bool
	sources map[string]*source
	ntpd    NTPDStatus
}

func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.JumpThreshold <= 0 {
		cfg.JumpThreshold = 2 * time.Second
	}
	if cfg.JumpWindow <= 0 {
		cfg.JumpWindow = 10 * time.Second
	}
	if cfg.HoldDown <= 0 {
		cfg.HoldDown = 10 * time.Minute
	}
	if cfg.Recover <= 0 {
		cfg.Recover = 30 * time.Second
	}
	w := &Worker{cfg: cfg, start: time.Now(), query: queryTime, lookup: net.LookupHost, sources: map[string]*source{}}
	for _, server := range cfg.External {
		w.sources[server] = &source{}
	}
	return w
}

func (w *Worker) Name() string { return Name }

// Probe reports the stored state, the external servers that can be used and
// what the local ntpd says. A manual time correction the node holds is applied
// to the clock first. The watch starts with the first probe.
func (w *Worker) Probe(ctx context.Context) (any, error) {
	w.once.Do(func() { go w.watch() })

	w.mu.Lock()
	ready := w.ready
	w.mu.Unlock()
	if !ready {
		return nil, control.ErrNotReady
	}

	r := Report{}
	w.ops.Lock()
	w.correct()
	state, err := w.load()
	r.ShiftError = w.shiftErr
	w.ops.Unlock()
	if err != nil {
		r.Error = err.Error()
	}
	r.State = state

	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	for _, server := range w.cfg.External {
		s := w.sources[server]
		if s.fault != "" && !now.Before(s.until) {
			log.Printf("[time] external server %s no longer held back (%s)", server, s.fault)
			s.fault = ""
		}
		if s.local != "" && !now.Before(s.localUntil) {
			log.Printf("[time] external server %s no longer held back locally (%s)", server, s.local)
			s.local = ""
		}
		switch {
		case s.fault != "":
			if r.Faults == nil {
				r.Faults = map[string]string{}
			}
			r.Faults[server] = s.fault
		case s.available && s.local == "":
			r.External = append(r.External, server)
		}
	}
	slices.Sort(r.External)
	r.NTPD = w.ntpd
	return r, nil
}

// correct applies a pending manual time correction: the clock is set back by
// the delta stored in the state file, and the delta is cleared. A failure is
// tried again after shiftRetry. w.ops must be held.
func (w *Worker) correct() {
	state, err := w.load()
	if err != nil || state.DeltaMS == 0 || time.Since(w.shiftTried) < shiftRetry {
		return
	}
	w.shiftTried = time.Now()
	shift := time.Duration(state.DeltaMS) * time.Millisecond
	log.Printf("[time] applying the manual time correction: clock back by %s", shift)
	if err := w.cfg.System.Shift(shift); err != nil {
		log.Printf("[time] time correction failed: %v", err)
		w.shiftErr = err.Error()
		return
	}
	// The delta is cleared right after the clock is set. Should this fail
	// the correction would be applied again: that window is as small as it
	// can be.
	state.DeltaMS = 0
	state.AppliedDeltaMS = shift.Milliseconds()
	state.Applied = time.Now().UTC().Format(time.RFC3339)
	if err := w.save(state); err != nil {
		w.shiftErr = "time corrected, but the state file was not updated: " + err.Error()
		log.Printf("[time] %s", w.shiftErr)
		return
	}
	w.shiftErr = ""
}

// watch asks every external server for the time and the local ntpd about its
// sources, forever.
func (w *Worker) watch() {
	for {
		for _, server := range w.cfg.External {
			at, err := w.query(server, time.Second)
			w.record(server, at, err)
		}
		w.monitor(w.cfg.System.Status(context.Background()))
		w.mu.Lock()
		w.ready = true
		w.mu.Unlock()
		time.Sleep(w.cfg.Interval)
	}
}

// monitor takes in what ntpd says. An external server that ntpd calls a
// falseticker is held back like one that jumped; one that ntpd has lost is
// held back by this node only.
func (w *Worker) monitor(st NTPDStatus) {
	addrs := map[string][]string{}
	for _, server := range w.cfg.External {
		host := ntpHost(server)
		addrs[server] = []string{host}
		if net.ParseIP(host) == nil {
			if ips, err := w.lookup(host); err == nil {
				addrs[server] = append(addrs[server], ips...)
			}
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.ntpd = st
	for _, server := range w.cfg.External {
		for _, addr := range addrs[server] {
			p, ok := st.peer(addr)
			if !ok {
				continue
			}
			switch p.State {
			case PeerFalseticker:
				w.hold(server, "ntpd: falseticker")
			case PeerUnreachable:
				s := w.sources[server]
				if s.local == "" {
					log.Printf("[time] external server %s held back here for %s: ntpd lost it", server, w.cfg.HoldDown)
					s.local = "ntpd: unreachable"
				}
				s.localUntil = time.Now().Add(w.cfg.HoldDown)
			}
			break
		}
	}
}

// hold withholds a server for the hold-down time. w.mu must be held.
func (w *Worker) hold(server, why string) {
	s := w.sources[server]
	if s.fault == "" {
		log.Printf("[time] external server %s held back for %s: %s", server, w.cfg.HoldDown, why)
		s.fault = why
	}
	s.until = time.Now().Add(w.cfg.HoldDown)
}

// record takes one answer, or failure, of a server into account.
func (w *Worker) record(server string, at time.Time, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.sources[server]
	if s == nil {
		s = &source{}
		w.sources[server] = s
	}

	if err != nil {
		s.missed++
		s.back = time.Time{}
		if s.available && s.missed >= lostAfter {
			log.Printf("[time] external server %s lost: %v", server, err)
			s.available, s.samples, s.lost = false, nil, time.Now()
		}
		return
	}
	s.missed = 0
	if !s.available {
		// A server that was lost before has to prove itself for a while,
		// or a flapping one would reconfigure the nodes every time.
		if s.back.IsZero() {
			s.back = time.Now()
		}
		if !s.lost.IsZero() && time.Since(s.back) < w.cfg.Recover {
			return
		}
		log.Printf("[time] external server %s available", server)
		s.available = true
	}

	// The server's time is compared with time elapsed on the monotonic
	// clock, not with the system clock: ntpd steps the system clock, and that
	// must not look like the server moving.
	now := time.Since(w.start)
	s.samples = append(s.samples, sample{at: now, offset: at.Sub(w.start.Round(0)) - now})
	for now-s.samples[0].at > w.cfg.JumpWindow {
		s.samples = s.samples[1:]
	}
	lo, hi := s.samples[0].offset, s.samples[0].offset
	for _, m := range s.samples {
		lo, hi = min(lo, m.offset), max(hi, m.offset)
	}
	if moved := hi - lo; moved >= w.cfg.JumpThreshold {
		s.samples = s.samples[len(s.samples)-1:]
		w.hold(server, fmt.Sprintf("jumped: moved %s within %s", moved.Round(time.Millisecond), w.cfg.JumpWindow))
	}
}

// Execute applies a time configuration. Running it again changes nothing: a
// node already at the order's epoch, or beyond, is left as it is.
func (w *Worker) Execute(ctx context.Context, order control.Order) error {
	if order.Kind != OrderConfigure {
		return fmt.Errorf("unknown order kind %q", order.Kind)
	}
	var c Config
	raw, _ := json.Marshal(order.Args)
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("bad configuration: %w", err)
	}

	w.ops.Lock()
	defer w.ops.Unlock()

	// An unreadable state file counts as no state; it is replaced below.
	state, _ := w.load()
	if state.Epoch >= c.Epoch {
		return nil
	}
	if err := w.cfg.System.Apply(render(c)); err != nil {
		return err
	}
	// A correction ordered now replaces one still pending; one still pending
	// is kept by an order that brings none, unless the node now follows an
	// external source, which would undo it anyway.
	delta := c.DeltaMS
	if delta == 0 && c.Mode != ModeAuto {
		delta = state.DeltaMS
	}
	err := w.save(State{
		Epoch:          c.Epoch,
		Mode:           c.Persist,
		DeltaMS:        delta,
		AppliedDeltaMS: state.AppliedDeltaMS,
		Applied:        state.Applied,
		Master:         c.Master,
		Updated:        time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("store state: %w", err)
	}
	if c.DeltaMS != 0 {
		// Applied at the next probe, without waiting out an earlier failure.
		w.shiftTried = time.Time{}
	}
	log.Printf("[time] configured: epoch=%d mode=%s master=%q servers=%v next-bootstrap=%s pending-delta=%dms",
		c.Epoch, c.Mode, c.Master, c.Servers, c.Persist, delta)
	return nil
}

func (w *Worker) load() (State, error) {
	var s State
	raw, err := os.ReadFile(w.cfg.StateFile)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return State{}, fmt.Errorf("state file %s: %w", w.cfg.StateFile, err)
	}
	return s, nil
}

// save replaces the state file in one step, so a crash never leaves half of it.
func (w *Worker) save(s State) error {
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(w.cfg.StateFile), 0o755); err != nil {
		return err
	}
	tmp := w.cfg.StateFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.cfg.StateFile)
}
