//go:build !windows

package timectl

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeNTPD makes stand-ins for ntpd (logs its start, then sleeps) and ntpq
// (prints a fixed peer table) in a temporary directory.
func fakeNTPD(t *testing.T) (*NTPD, string) {
	dir := t.TempDir()
	starts := filepath.Join(dir, "starts")
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ntpd := write("ntpd", "printf '%s\\n' \"$*\" >> "+starts+"\nexec sleep 60\n")
	ntpq := write("ntpq", `cat <<'X'
     remote           refid      st t when poll reach   delay   offset  jitter
==============================================================================
*10.0.0.1        .GPS.            1 u   33   64  377    0.512   -0.027   0.011
X
`)
	n := &NTPD{Bin: ntpd, Ntpq: ntpq, Conf: filepath.Join(dir, "etc", "ntp.conf")}
	t.Cleanup(func() {
		n.mu.Lock()
		n.stop()
		n.mu.Unlock()
	})
	return n, starts
}

func countStarts(t *testing.T, path string) int {
	t.Helper()
	// The script logs right after it starts; give it a moment.
	deadline := time.Now().Add(2 * time.Second)
	for {
		raw, _ := os.ReadFile(path)
		n := strings.Count(string(raw), "\n")
		if time.Now().After(deadline) || n > 0 {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNTPDRunsAsAChildProcess(t *testing.T) {
	n, starts := fakeNTPD(t)
	ctx := context.Background()

	if st := n.Status(ctx); st.Running {
		t.Fatal("running before any configuration")
	}
	if err := n.Apply("server a iburst\n"); err != nil {
		t.Fatal(err)
	}
	conf, _ := os.ReadFile(n.Conf)
	if !strings.Contains(string(conf), "server a iburst") || !strings.Contains(string(conf), "driftfile") {
		t.Fatalf("configuration written:\n%s", conf)
	}
	st := n.Status(ctx)
	if !st.Running || !slices.Equal(st.Peers, []Peer{{"10.0.0.1", PeerSync}}) {
		t.Fatalf("status %+v, want running with one peer", st)
	}
	if got := countStarts(t, starts); got != 1 {
		t.Fatalf("%d starts, want 1", got)
	}
	raw, _ := os.ReadFile(starts)
	if !strings.Contains(string(raw), "-n") || !strings.Contains(string(raw), "-c "+n.Conf) {
		t.Fatalf("ntpd started with %q, want it in the foreground with the written configuration", raw)
	}

	// The same configuration: left alone. Another one: restarted.
	if err := n.Apply("server a iburst\n"); err != nil {
		t.Fatal(err)
	}
	if err := n.Apply("server b iburst\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := countStarts(t, starts); got != 2 {
		t.Fatalf("%d starts, want 2", got)
	}

	// ntpd dies: the next status check starts it again.
	n.mu.Lock()
	p := n.proc
	n.mu.Unlock()
	_ = p.cmd.Process.Kill()
	<-p.exited
	if st := n.Status(ctx); !st.Running {
		t.Fatalf("status %+v, want ntpd started again", st)
	}
	time.Sleep(200 * time.Millisecond)
	if got := countStarts(t, starts); got != 3 {
		t.Fatalf("%d starts, want 3", got)
	}
}
