package timectl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// System is the node's time machinery: ntpd and the clock.
type System interface {
	// Apply makes ntpd run with conf (the lines render produces). ntpd is
	// (re)started when conf changed or it is not running; otherwise nothing
	// is done.
	Apply(conf string) error
	// Shift sets the clock back by d. ntpd is stopped around the step, so it
	// does not fight it, and started again afterwards.
	Shift(d time.Duration) error
	// Status tells whether ntpd runs and what it makes of its sources. An
	// ntpd that died since the last Apply is started again first.
	Status(ctx context.Context) NTPDStatus
}

// Peer states, from the tally code ntpd gives each association.
const (
	PeerSync        = "sync"        // the source ntpd follows
	PeerCandidate   = "candidate"   // usable, combined with the sync source
	PeerOutlier     = "outlier"     // discarded by the clustering
	PeerFalseticker = "falseticker" // disagrees with the majority: not to be trusted
	PeerReject      = "reject"      // not usable (yet)
	// PeerUnreachable: ntpd reached the source before but its last three
	// polls went unanswered.
	PeerUnreachable = "unreachable"
)

// Peer is one association of ntpd, as ntpq lists it. It only holds what
// changes rarely: it is part of the report, and every change of the report
// goes through the log.
type Peer struct {
	Addr  string `json:"addr"`
	State string `json:"state"`
}

// NTPDStatus is the monitoring view of the local ntpd.
type NTPDStatus struct {
	Running bool   `json:"running"`
	Error   string `json:"error,omitempty"`
	Peers   []Peer `json:"peers,omitempty"`
}

// peer returns the association for addr, if ntpd has one.
func (s NTPDStatus) peer(addr string) (Peer, bool) {
	for _, p := range s.Peers {
		if p.Addr == addr {
			return p, true
		}
	}
	return Peer{}, false
}

// render writes the ntpd configuration lines for c.
func render(c Config) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Time mode: %s. Written by kaft; changes are overwritten.\n", c.Mode)
	// Never give up on a large offset: after a manual correction the clocks
	// are far apart on purpose.
	b.WriteString("tinker panic 0\n")
	for _, s := range c.Servers {
		fmt.Fprintf(&b, "server %s iburst\n", ntpHost(s))
	}
	if c.Local {
		// The node's own clock as the reference.
		b.WriteString("server 127.127.1.0\nfudge 127.127.1.0 stratum 8\n")
	}
	for _, p := range c.Peers {
		fmt.Fprintf(&b, "peer %s\n", p)
	}
	if len(c.Peers) > 0 {
		// Without a server the peers elect one of themselves to follow.
		b.WriteString("tos orphan 10\n")
	}
	return b.String()
}

// ntpHost drops a port from a server address: ntpd always uses port 123, the
// port is only for the SNTP watch.
func ntpHost(server string) string {
	if host, _, err := net.SplitHostPort(server); err == nil {
		return host
	}
	return server
}

// NTPD runs ntpd as a child process of this program, not as a service. It
// writes the configuration file itself and restarts the process whenever the
// configuration changes or the process dies. The process needs the right to
// set the clock (administrator on Windows, root elsewhere), and nothing else
// may hold UDP port 123: an ntpd service must be stopped and disabled.
type NTPD struct {
	// Bin is the ntpd executable, Ntpq the ntpq executable used to ask it
	// about its sources.
	Bin  string
	Ntpq string
	// Conf is the configuration file this program writes; the drift, pid and
	// log files go next to it.
	Conf string
	// Args are appended to the command line.
	Args []string

	mu    sync.Mutex
	conf  string // the configuration ntpd is to run with; empty before Apply
	proc  *process
	err   error // why ntpd last failed to start
	swept bool  // a process left behind by a previous run has been dealt with
}

// process is one run of ntpd. err is set before exited is closed.
type process struct {
	cmd    *exec.Cmd
	exited chan struct{}
	err    error
}

func (n *NTPD) dir() string     { return filepath.Dir(n.Conf) }
func (n *NTPD) pidFile() string { return filepath.Join(n.dir(), "ntpd.pid") }

func (n *NTPD) full(conf string) string {
	return "restrict default noquery nomodify notrap\n" +
		"restrict -6 default noquery nomodify notrap\n" +
		"restrict 127.0.0.1\n" +
		"restrict -6 ::1\n" +
		fmt.Sprintf("driftfile %q\n", filepath.Join(n.dir(), "ntp.drift")) +
		conf
}

func (n *NTPD) Apply(conf string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if conf == n.conf && n.running() {
		return nil
	}
	n.stop()
	if err := os.MkdirAll(n.dir(), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(n.Conf, []byte(n.full(conf)), 0o644); err != nil {
		return err
	}
	n.conf = conf
	return n.start()
}

func (n *NTPD) Shift(d time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stop()
	err := setClock(time.Now().Add(-d))
	if err != nil {
		err = fmt.Errorf("set clock: %w", err)
	}
	// ntpd is started again even if the clock could not be set: a node with
	// the wrong time is still better off following the others.
	if n.conf != "" {
		if serr := n.start(); serr != nil && err == nil {
			err = serr
		}
	}
	return err
}

func (n *NTPD) Status(ctx context.Context) NTPDStatus {
	n.mu.Lock()
	if n.conf != "" && !n.running() {
		log.Printf("[time] ntpd is not running (%v); starting it again", n.why())
		if err := n.start(); err != nil {
			log.Printf("[time] %v", err)
		}
	}
	st := NTPDStatus{Running: n.running()}
	if !st.Running {
		if err := n.why(); err != nil {
			st.Error = err.Error()
		}
	}
	n.mu.Unlock()
	if !st.Running {
		return st
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, n.Ntpq, "-n", "-c", "peers", "127.0.0.1").Output()
	if err != nil {
		st.Error = fmt.Sprintf("ntpq: %v", err)
		return st
	}
	st.Peers = parsePeers(string(out))
	return st
}

// running reports whether the process is alive. n.mu must be held.
func (n *NTPD) running() bool {
	if n.proc == nil {
		return false
	}
	select {
	case <-n.proc.exited:
		return false
	default:
		return true
	}
}

// why tells why ntpd is not running. n.mu must be held.
func (n *NTPD) why() error {
	if n.proc != nil && !n.running() {
		return n.proc.err
	}
	return n.err
}

// start launches ntpd in the foreground. n.mu must be held.
func (n *NTPD) start() error {
	n.sweep()
	args := append([]string{
		"-n", // stay in the foreground: this program is the supervisor
		"-g", // accept any first offset
		"-c", n.Conf,
		"-p", n.pidFile(),
		"-l", filepath.Join(n.dir(), "ntpd.log"),
	}, n.Args...)
	cmd := exec.Command(n.Bin, args...)
	if err := cmd.Start(); err != nil {
		n.proc, n.err = nil, fmt.Errorf("start ntpd: %w", err)
		return n.err
	}
	p := &process{cmd: cmd, exited: make(chan struct{})}
	n.proc, n.err = p, nil
	go func() {
		err := cmd.Wait()
		if err == nil {
			err = errors.New("exited")
		}
		p.err = fmt.Errorf("ntpd %w", err)
		close(p.exited)
	}()
	log.Printf("[time] ntpd started (pid %d) with %s", cmd.Process.Pid, n.Conf)
	return nil
}

// stop ends the process this program started. n.mu must be held.
func (n *NTPD) stop() {
	p := n.proc
	n.proc = nil
	if p == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		log.Printf("[time] ntpd (pid %d) did not exit after being killed", p.cmd.Process.Pid)
	}
	_ = os.Remove(n.pidFile())
}

// sweep stops, once, an ntpd left running by a previous run of this program:
// it would hold port 123. It is found through the pid file this program
// makes ntpd write. n.mu must be held.
func (n *NTPD) sweep() {
	if n.swept {
		return
	}
	n.swept = true
	raw, err := os.ReadFile(n.pidFile())
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 || pid == os.Getpid() {
		return
	}
	if p, err := os.FindProcess(pid); err == nil {
		if p.Kill() == nil {
			log.Printf("[time] stopped ntpd left over from a previous run (pid %d)", pid)
			time.Sleep(500 * time.Millisecond)
		}
	}
	_ = os.Remove(n.pidFile())
}

// parsePeers reads the output of "ntpq -n -c peers":
//
//	     remote           refid      st t when poll reach   delay   offset  jitter
//	==============================================================================
//	*192.168.1.1     .GPS.            1 u   33   64  377    0.512   -0.027   0.011
//	x10.0.0.5        .GPS.            1 u   12   64    1    0.330  912.110   0.002
func parsePeers(out string) []Peer {
	var peers []Peer
	body := false
	for _, line := range strings.Split(strings.ReplaceAll(out, "\r", ""), "\n") {
		if strings.HasPrefix(line, "===") {
			body = true
			continue
		}
		if !body || len(line) < 2 {
			continue
		}
		f := strings.Fields(line[1:])
		if len(f) < 7 {
			continue
		}
		reach, err := strconv.ParseUint(f[6], 8, 16)
		if err != nil {
			continue
		}
		p := Peer{Addr: f[0], State: PeerReject}
		switch line[0] {
		case '*', 'o':
			p.State = PeerSync
		case '+', '#':
			p.State = PeerCandidate
		case '-', '.':
			p.State = PeerOutlier
		case 'x':
			p.State = PeerFalseticker
		}
		if reach != 0 && reach&7 == 0 {
			p.State = PeerUnreachable
		}
		peers = append(peers, p)
	}
	return peers
}

// DryRun stands in for the real thing: it only logs what would be done. For
// trying the controller out on a machine whose clock must be left alone.
type DryRun struct {
	mu   sync.Mutex
	conf string
}

func (s *DryRun) Apply(conf string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conf == s.conf {
		return nil
	}
	s.conf = conf
	log.Printf("[time] dry run: would restart ntpd with:\n%s", conf)
	return nil
}

func (s *DryRun) Shift(d time.Duration) error {
	log.Printf("[time] dry run: would set the clock back by %s", d)
	return nil
}

func (s *DryRun) Status(context.Context) NTPDStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return NTPDStatus{Running: s.conf != ""}
}
