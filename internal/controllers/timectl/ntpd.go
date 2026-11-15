package timectl

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// System is the node's time machinery: ntpd and the clock.
type System interface {
	// Apply sets the clock back by shift, if not zero, and makes ntpd run
	// with conf (the lines render produces). It does nothing if ntpd already
	// runs with conf and there is no shift.
	Apply(conf string, shift time.Duration) error
}

// render writes the ntpd configuration lines for c.
func render(c Config) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Time mode: %s. Written by kaft; changes are overwritten.\n", c.Mode)
	// Never give up on a large offset: after a manual correction the clocks
	// are far apart on purpose.
	b.WriteString("tinker panic 0\n")
	for _, s := range c.Servers {
		fmt.Fprintf(&b, "server %s iburst\n", s)
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

// Meinberg drives the Meinberg NTP port for Windows: it rewrites ntp.conf and
// restarts the service. The process needs administrator rights.
type Meinberg struct {
	// Conf is the path of ntp.conf.
	Conf string
	// Service is the Windows service name.
	Service string
}

func (m Meinberg) Apply(conf string, shift time.Duration) error {
	full := "restrict default noquery nomodify notrap\n" +
		"restrict -6 default noquery nomodify notrap\n" +
		"restrict 127.0.0.1\n" +
		"restrict -6 ::1\n" +
		fmt.Sprintf("driftfile %q\n", filepath.Join(filepath.Dir(m.Conf), "ntp.drift")) +
		conf
	if old, err := os.ReadFile(m.Conf); err == nil && string(old) == full && shift == 0 {
		return nil
	}

	// Stopping fails when the service is not running, which is fine.
	if out, err := exec.Command("net", "stop", m.Service).CombinedOutput(); err != nil {
		log.Printf("[time] stopping %s: %v: %s", m.Service, err, bytes.TrimSpace(out))
	}
	var shiftErr error
	if shift != 0 {
		if shiftErr = setClock(time.Now().Add(-shift)); shiftErr != nil {
			shiftErr = fmt.Errorf("set clock: %w", shiftErr)
		}
	}
	// ntpd is started again even if the clock could not be set: a node with
	// the wrong time is still better off following the others.
	if err := os.WriteFile(m.Conf, []byte(full), 0o644); err != nil {
		return err
	}
	if out, err := exec.Command("net", "start", m.Service).CombinedOutput(); err != nil {
		return fmt.Errorf("start %s: %w: %s", m.Service, err, bytes.TrimSpace(out))
	}
	return shiftErr
}

// DryRun stands in for the real thing: it only logs what would be done. For
// trying the controller out on a machine whose clock must be left alone.
type DryRun struct {
	mu   sync.Mutex
	conf string
}

func (s *DryRun) Apply(conf string, shift time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conf == s.conf && shift == 0 {
		return nil
	}
	s.conf = conf
	log.Printf("[time] dry run: would set the clock back by %s and restart ntpd with:\n%s", shift, conf)
	return nil
}
