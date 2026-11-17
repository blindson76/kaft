package timectl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
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
	// System is ntpd and the clock.
	System System
}

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
}

// Worker watches the external time servers and applies time configurations.
type Worker struct {
	cfg   WorkerConfig
	start time.Time
	query func(server string, timeout time.Duration) (time.Time, error)

	// life ends with Close; the watch runs on it and bg tracks it.
	life   context.Context
	cancel context.CancelFunc
	bg     sync.WaitGroup

	// ops serializes orders.
	ops  sync.Mutex
	once sync.Once

	// mu guards the fields below.
	mu      sync.Mutex
	ready   bool
	sources map[string]*source
	jump    string
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
	life, cancel := context.WithCancel(context.Background())
	return &Worker{cfg: cfg, start: time.Now(), query: queryTime, sources: map[string]*source{}, life: life, cancel: cancel}
}

func (w *Worker) Name() string { return Name }

// Close implements control.Closer: it ends the watch. ntpd and the clock are
// left as they are; the node's time does not depend on this program running.
func (w *Worker) Close(ctx context.Context) error {
	w.cancel()
	done := make(chan struct{})
	go func() {
		w.bg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("time watch: %w", ctx.Err())
	}
}

// Probe reports the stored state and what the watch on the external servers
// currently says. The watch starts with the first probe and runs until Close.
func (w *Worker) Probe(ctx context.Context) (any, error) {
	w.once.Do(func() { w.bg.Go(func() { w.watch(w.life) }) })

	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.ready {
		return nil, control.ErrNotReady
	}
	r := Report{Jump: w.jump}
	for _, server := range w.cfg.External {
		if w.sources[server].available {
			r.External = append(r.External, server)
		}
	}
	slices.Sort(r.External)

	state, err := w.load()
	if err != nil {
		r.Error = err.Error()
	}
	r.State = state
	return r, nil
}

// watch asks every external server for the time until ctx ends.
func (w *Worker) watch(ctx context.Context) {
	t := time.NewTicker(w.cfg.Interval)
	defer t.Stop()
	for {
		for _, server := range w.cfg.External {
			if ctx.Err() != nil {
				return
			}
			at, err := w.query(server, time.Second)
			w.record(server, at, err)
		}
		w.mu.Lock()
		w.ready = true
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
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
		if s.available && s.missed >= lostAfter {
			log.Printf("[time] external server %s lost: %v", server, err)
			s.available, s.samples = false, nil
		}
		return
	}
	if !s.available {
		log.Printf("[time] external server %s available", server)
	}
	s.available, s.missed = true, 0

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
		if w.jump == "" {
			w.jump = fmt.Sprintf("%s moved %s within %s", server, moved.Round(time.Millisecond), w.cfg.JumpWindow)
			log.Printf("[time] external time jumped: %s", w.jump)
		}
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
	if err := ctx.Err(); err != nil {
		return err
	}

	// From here on ctx is not looked at: applying stops ntpd, may set the
	// clock, and starts ntpd again. Cut short in between, the node would be
	// left without a time service.

	// An unreadable state file counts as no state; it is replaced below.
	state, _ := w.load()
	if state.Epoch >= c.Epoch {
		return nil
	}
	var shift time.Duration
	if c.ShiftMS != 0 && state.Epoch == c.ShiftFrom {
		shift = time.Duration(c.ShiftMS) * time.Millisecond
		log.Printf("[time] applying the stored time correction: clock back by %s", shift)
	}
	if err := w.cfg.System.Apply(render(c), shift); err != nil {
		return err
	}
	// The state is stored last. If this fails the order is tried again, and
	// the correction with it; that window is as small as it can be.
	err := w.save(State{
		Epoch:   c.Epoch,
		Mode:    c.Persist,
		DeltaMS: c.DeltaMS,
		Master:  c.Master,
		Updated: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("store state: %w", err)
	}
	log.Printf("[time] configured: epoch=%d mode=%s master=%q next-bootstrap=%s delta=%dms",
		c.Epoch, c.Mode, c.Master, c.Persist, c.DeltaMS)

	// A reported jump has been acted on; watch afresh.
	w.mu.Lock()
	w.jump = ""
	for _, s := range w.sources {
		s.samples = nil
	}
	w.mu.Unlock()
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
