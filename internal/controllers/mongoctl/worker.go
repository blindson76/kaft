package mongoctl

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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// keyFileContent is the shared secret replica set members authenticate each
// other with. Fixed on purpose: this is a prototype.
const keyFileContent = "KEYFILE"

// WorkerConfig configures the node-local mongod.
type WorkerConfig struct {
	Node     string
	Addr     string // host:port mongod serves on
	Bin      string // mongod executable
	DataDir  string // this node's directory
	ReplSet  string
	User     string
	Password string
	Args     []string // extra mongod arguments
}

// Worker runs the node-local mongod.
//
// mongod runs in one of two modes:
//
//   - maintenance: without --replSet, on localhost only. Used by the probe to
//     read what is in the data files without taking part in any replica set.
//   - replica set: with --replSet and the key file, on the configured address.
//     Entered only on an OrderStart from the controller.
type Worker struct {
	cfg WorkerConfig

	// ops serializes the probe and the orders: one mongod, one operation.
	ops  sync.Mutex
	once sync.Once

	// mu guards the fields below.
	mu      sync.Mutex
	boot    *Boot
	status  Status
	proc    *exec.Cmd
	exited  chan struct{}
	replset bool           // proc runs in replica set mode
	client  *driver.Client // replica set mode connection, with the monitor
	stopMon context.CancelFunc

	metrics metricsState
}

func NewWorker(cfg WorkerConfig) *Worker {
	return &Worker{cfg: cfg, status: Status{Role: RoleStopped}}
}

func (w *Worker) Name() string { return Name }

// Probe returns the last boot probe together with the live status. It does no
// I/O: the boot part is refreshed by the probe itself and the status part by
// topology events, so calling it often is free.
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
	if w.boot == nil {
		return nil, control.ErrNotReady
	}
	return Report{Boot: *w.boot, Status: w.status}, nil
}

// Execute carries out one order. Every order checks the current state first,
// so running it again is harmless.
func (w *Worker) Execute(ctx context.Context, order control.Order) error {
	w.ops.Lock()
	defer w.ops.Unlock()

	switch order.Kind {
	case OrderStart:
		return w.startReplSet(ctx)
	case OrderStop:
		w.probe(ctx)
		return nil
	case OrderWipe:
		w.stop()
		if err := removeAll(w.cfg.DataDir); err != nil {
			return fmt.Errorf("wipe: %w", err)
		}
		log.Printf("[mongo] data wiped")
		if b := w.probe(ctx); b.State != StateFresh {
			return fmt.Errorf("node not fresh after wipe: %s %s", b.State, b.Reason)
		}
		return nil
	case OrderInitiate:
		return w.initiate(ctx, stringsArg(order.Args["members"]))
	case OrderReconfig:
		force, _ := order.Args["force"].(bool)
		return w.reconfig(ctx, stringsArg(order.Args["members"]), force)
	}
	return fmt.Errorf("unknown order kind %q", order.Kind)
}

// stringsArg reads a string list argument, which is a []string when placed
// and a []any after a snapshot restore.
func stringsArg(v any) []string {
	raw, _ := json.Marshal(v)
	var out []string
	_ = json.Unmarshal(raw, &out)
	return out
}

// --- probe -----------------------------------------------------------------

// probe stops mongod, starts it in maintenance mode, reads the replica set
// state out of the data files, makes sure a fresh node has the admin user, and
// stops it again. It never deletes anything: a node that cannot be probed is
// reported broken and the controller decides about the wipe.
func (w *Worker) probe(ctx context.Context) Boot {
	w.stop()

	id := make([]byte, 6)
	_, _ = rand.Read(id)
	b := Boot{ProbeID: hex.EncodeToString(id)}

	if err := w.inspect(ctx, &b); err != nil {
		b = Boot{ProbeID: b.ProbeID, State: StateBroken, Reason: err.Error()}
	}
	w.stop()

	log.Printf("[mongo] probe: state=%s replset=%s lastOp=%+v %s", b.State, b.ReplSetID, b.LastOp, b.Reason)
	w.mu.Lock()
	w.boot = &b
	w.mu.Unlock()
	return b
}

func (w *Worker) inspect(ctx context.Context, b *Boot) error {
	if err := w.startProc(false); err != nil {
		return err
	}
	// Without --auth every local connection has full access.
	c, err := w.connect(ctx, w.localAddr(), false, nil)
	if err != nil {
		return err
	}
	defer c.Disconnect(context.Background())

	local := c.Database("local")
	var conf struct {
		ID      string `bson:"_id"`
		Version int64  `bson:"version"`
		Term    int64  `bson:"term"`
		Members []struct {
			Host string `bson:"host"`
		} `bson:"members"`
		Settings struct {
			ReplicaSetID bson.ObjectID `bson:"replicaSetId"`
		} `bson:"settings"`
	}
	err = local.Collection("system.replset").FindOne(ctx, bson.D{}).Decode(&conf)
	switch {
	case errors.Is(err, driver.ErrNoDocuments):
		b.State = StateFresh
	case err != nil:
		return fmt.Errorf("read replica set config: %w", err)
	default:
		b.State = StateMember
		b.ReplSetID = conf.Settings.ReplicaSetID.Hex()
		b.SetName, b.ConfigVersion, b.ConfigTerm = conf.ID, conf.Version, conf.Term
		for _, m := range conf.Members {
			b.Members = append(b.Members, m.Host)
		}

		type entry struct {
			TS   bson.Timestamp `bson:"ts"`
			Term int64          `bson:"t"`
		}
		oplog := local.Collection("oplog.rs")
		read := func(order int) (OpTime, error) {
			var e entry
			err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: order}})).Decode(&e)
			if errors.Is(err, driver.ErrNoDocuments) {
				return OpTime{}, nil
			}
			return OpTime{Term: e.Term, T: e.TS.T, I: e.TS.I}, err
		}
		if b.LastOp, err = read(-1); err != nil {
			return fmt.Errorf("read oplog: %w", err)
		}
		if b.FirstOp, err = read(1); err != nil {
			return fmt.Errorf("read oplog: %w", err)
		}
		var election struct {
			Term int64 `bson:"term"`
		}
		if err := local.Collection("replset.election").FindOne(ctx, bson.D{}).Decode(&election); err == nil {
			b.Term = election.Term
		}
	}

	// The admin user. It is created on a fresh node only: writing to a node
	// that holds replica set data would bypass the oplog and make it diverge.
	admin := c.Database("admin")
	n, err := admin.Collection("system.users").CountDocuments(ctx, bson.D{{Key: "user", Value: w.cfg.User}})
	if err != nil {
		return fmt.Errorf("read users: %w", err)
	}
	if n == 0 {
		if b.State != StateFresh {
			return errors.New("replica set data without the admin user")
		}
		err := admin.RunCommand(ctx, bson.D{
			{Key: "createUser", Value: w.cfg.User},
			{Key: "pwd", Value: w.cfg.Password},
			{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}}}},
		}).Err()
		if err != nil {
			return fmt.Errorf("create admin user: %w", err)
		}
		log.Printf("[mongo] admin user created")
	}
	// Prove the credentials work before the node is offered to the controller.
	authed, err := w.connect(ctx, w.localAddr(), true, nil)
	if err != nil {
		return fmt.Errorf("authenticate as %s: %w", w.cfg.User, err)
	}
	return authed.Disconnect(context.Background())
}

// --- process ---------------------------------------------------------------

func (w *Worker) dbPath() string  { return filepath.Join(w.cfg.DataDir, "db") }
func (w *Worker) pidPath() string { return filepath.Join(w.cfg.DataDir, "mongod.pid") }
func (w *Worker) keyPath() string {
	return filepath.Join(os.TempDir(), "kaft-mongo-"+w.cfg.Node+".key")
}

func (w *Worker) port() string {
	_, port, _ := net.SplitHostPort(w.cfg.Addr)
	return port
}

func (w *Worker) localAddr() string { return net.JoinHostPort("127.0.0.1", w.port()) }

func portOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// startProc launches mongod and waits until it accepts connections.
func (w *Worker) startProc(replset bool) error {
	if err := os.MkdirAll(w.dbPath(), 0o755); err != nil {
		return err
	}
	args := []string{
		"--dbpath", w.dbPath(),
		"--port", w.port(),
		"--logpath", filepath.Join(w.cfg.DataDir, "mongod.log"), "--logappend",
		"--pidfilepath", w.pidPath(),
	}
	if replset {
		if err := os.WriteFile(w.keyPath(), []byte(keyFileContent), 0o600); err != nil {
			return err
		}
		bind := "127.0.0.1"
		if host, _, _ := net.SplitHostPort(w.cfg.Addr); host != "" && host != bind && host != "localhost" {
			bind += "," + host
		}
		args = append(args, "--replSet", w.cfg.ReplSet, "--keyFile", w.keyPath(), "--bind_ip", bind)
	} else {
		args = append(args, "--bind_ip", "127.0.0.1")
	}
	args = append(args, w.cfg.Args...)

	cmd := exec.Command(w.cfg.Bin, args...)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start mongod: %w", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
		// If this is still the current process, it died on its own.
		w.mu.Lock()
		if w.proc == cmd {
			w.proc = nil
			w.status = Status{Role: RoleStopped}
		}
		w.mu.Unlock()
	}()

	w.mu.Lock()
	w.proc, w.exited, w.replset = cmd, exited, replset
	w.mu.Unlock()

	deadline := time.After(90 * time.Second)
	for !portOpen(w.localAddr()) {
		select {
		case <-exited:
			return errors.New("mongod exited during startup (see mongod.log)")
		case <-deadline:
			return errors.New("mongod did not accept connections in time")
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

// stop shuts down mongod: the one this worker started, or one left behind by
// a previous run of this program on the same port.
func (w *Worker) stop() {
	w.mu.Lock()
	cmd, exited, replset := w.proc, w.exited, w.replset
	client, stopMon := w.client, w.stopMon
	w.proc, w.client, w.stopMon = nil, nil, nil
	w.status = Status{Role: RoleStopped}
	w.mu.Unlock()

	if stopMon != nil {
		stopMon()
	}
	if client != nil {
		_ = client.Disconnect(context.Background())
	}

	if cmd != nil {
		w.shutdown(replset)
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
	} else if portOpen(w.localAddr()) {
		// Not ours: try both modes, then the pid file.
		w.shutdown(true)
		w.shutdown(false)
		if raw, err := os.ReadFile(w.pidPath()); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				if p, err := os.FindProcess(pid); err == nil {
					time.Sleep(3 * time.Second)
					if portOpen(w.localAddr()) {
						_ = p.Kill()
					}
				}
			}
		}
	}
	for i := 0; i < 100 && portOpen(w.localAddr()); i++ {
		time.Sleep(200 * time.Millisecond)
	}
}

// shutdown asks mongod to exit cleanly. The connection drops while it does,
// so the error says nothing.
func (w *Worker) shutdown(auth bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := w.connect(ctx, w.localAddr(), auth, nil)
	if err != nil {
		return
	}
	defer c.Disconnect(context.Background())
	_ = c.Database("admin").RunCommand(ctx, bson.D{{Key: "shutdown", Value: 1}, {Key: "force", Value: true}}).Err()
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

// connect opens a direct connection to the local mongod and pings it.
func (w *Worker) connect(ctx context.Context, addr string, auth bool, monitor *event.ServerMonitor) (*driver.Client, error) {
	opts := options.Client().
		ApplyURI("mongodb://" + addr + "/?directConnection=true").
		SetServerSelectionTimeout(5 * time.Second).
		SetConnectTimeout(3 * time.Second)
	if auth {
		opts.SetAuth(options.Credential{Username: w.cfg.User, Password: w.cfg.Password, AuthSource: "admin"})
	}
	if monitor != nil {
		opts.SetServerMonitor(monitor)
	}
	c, err := driver.Connect(opts)
	if err != nil {
		return nil, err
	}
	if err := c.Ping(ctx, nil); err != nil {
		_ = c.Disconnect(context.Background())
		return nil, err
	}
	return c, nil
}

// --- replica set mode ------------------------------------------------------

// startReplSet starts mongod as a replica set member and begins watching its
// topology.
func (w *Worker) startReplSet(ctx context.Context) error {
	w.mu.Lock()
	running := w.proc != nil && w.replset && w.client != nil
	boot := w.boot
	w.mu.Unlock()
	if running {
		return nil
	}
	if boot == nil || boot.State == StateBroken {
		return errors.New("node is not ready to start: probe failed or pending")
	}

	w.stop()
	if err := w.startProc(true); err != nil {
		return err
	}

	// The driver reports every change of the server's hello response (role,
	// primary, config version, hosts) through this monitor. mongod pushes
	// those over a streaming hello, so nothing here polls.
	changed := make(chan struct{}, 1)
	monitor := &event.ServerMonitor{
		ServerDescriptionChanged: func(*event.ServerDescriptionChangedEvent) {
			select {
			case changed <- struct{}{}:
			default:
			}
		},
	}
	c, err := w.connect(ctx, w.localAddr(), true, monitor)
	if err != nil {
		w.stop()
		return fmt.Errorf("connect to mongod: %w", err)
	}

	monCtx, stopMon := context.WithCancel(context.Background())
	w.mu.Lock()
	w.client, w.stopMon = c, stopMon
	w.mu.Unlock()

	w.refreshStatus(ctx, c)
	go func() {
		for {
			select {
			case <-monCtx.Done():
				return
			case <-changed:
				w.refreshStatus(monCtx, c)
			}
		}
	}()
	log.Printf("[mongo] mongod started as replica set member")
	return nil
}

type helloReply struct {
	IsWritablePrimary bool     `bson:"isWritablePrimary"`
	Secondary         bool     `bson:"secondary"`
	SetName           string   `bson:"setName"`
	SetVersion        int64    `bson:"setVersion"`
	Primary           string   `bson:"primary"`
	Hosts             []string `bson:"hosts"`
}

func (w *Worker) hello(ctx context.Context, c *driver.Client) (helloReply, error) {
	var h helloReply
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h)
	return h, err
}

// refreshStatus reads the current role after a topology change.
func (w *Worker) refreshStatus(ctx context.Context, c *driver.Client) {
	h, err := w.hello(ctx, c)
	if err != nil {
		return
	}
	s := Status{Role: RoleOther, Primary: h.Primary, SetVersion: h.SetVersion, Hosts: slices.Sorted(slices.Values(h.Hosts))}
	switch {
	case h.IsWritablePrimary:
		s.Role = RolePrimary
	case h.Secondary:
		s.Role = RoleSecondary
	case h.SetName == "":
		s.Role = RoleNone
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.client != c {
		return // stopped or restarted meanwhile
	}
	if s.Role != w.status.Role || s.Primary != w.status.Primary {
		log.Printf("[mongo] status: role=%s primary=%s hosts=%v", s.Role, s.Primary, s.Hosts)
	}
	w.status = s
}

func (w *Worker) current() (*driver.Client, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.client == nil {
		return nil, errors.New("mongod is not running as a replica set member")
	}
	return w.client, nil
}

// session opens a freshly authenticated connection to the running local
// mongod, for commands that need a user. The long-lived client cannot be
// trusted for that: when a member joins by initial sync its users are
// replaced from the primary, and mongod drops the authentication of the
// connections that were already open.
func (w *Worker) session(ctx context.Context) (*driver.Client, func(), error) {
	if _, err := w.current(); err != nil {
		return nil, nil, err
	}
	c, err := w.connect(ctx, w.localAddr(), true, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to mongod: %w", err)
	}
	return c, func() { _ = c.Disconnect(context.Background()) }, nil
}

// rsConfig is the part of the replica set config this program edits; every
// other field is carried through unchanged.
type rsConfig struct {
	ID      string     `bson:"_id"`
	Version int64      `bson:"version"`
	Members []rsMember `bson:"members"`
	Rest    bson.M     `bson:",inline"`
}

type rsMember struct {
	ID   int    `bson:"_id"`
	Host string `bson:"host"`
	Rest bson.M `bson:",inline"`
}

func (c rsConfig) hosts() []string {
	out := make([]string, 0, len(c.Members))
	for _, m := range c.Members {
		out = append(out, m.Host)
	}
	slices.Sort(out)
	return out
}

func (c rsConfig) nextID() int {
	next := 0
	for _, m := range c.Members {
		next = max(next, m.ID+1)
	}
	return next
}

func sameHosts(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func getConfig(ctx context.Context, c *driver.Client) (rsConfig, error) {
	var reply struct {
		Config rsConfig `bson:"config"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&reply)
	return reply.Config, err
}

func commandCode(err error) int32 {
	var ce driver.CommandError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return 0
}

const (
	codeNotWritablePrimary = 10107
	codeAlreadyInitialized = 23
	codeNotYetInitialized  = 94
)

// initiate creates the replica set with this node as its only member, then
// grows it to members. Starting alone and adding the others one by one is the
// same path a later repair takes, and it does not depend on the other nodes
// being empty.
func (w *Worker) initiate(ctx context.Context, members []string) error {
	c, done, err := w.session(ctx)
	if err != nil {
		return err
	}
	defer done()
	err = c.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: bson.D{
		{Key: "_id", Value: w.cfg.ReplSet},
		{Key: "members", Value: bson.A{bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: w.cfg.Addr}}}},
	}}}).Err()
	if err != nil && commandCode(err) != codeAlreadyInitialized {
		return fmt.Errorf("replSetInitiate: %w", err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		if h, err := w.hello(ctx, c); err == nil && h.IsWritablePrimary {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("node did not become primary after replSetInitiate")
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf("[mongo] replica set initiated")
	return w.reconfig(ctx, members, false)
}

// reconfig brings the replica set config to members.
//
// Without force it runs on the primary and changes one member per step,
// removals first, waiting for each config to be committed: MongoDB accepts
// only single-member changes outside a forced reconfig.
//
// With force it replaces the member list in one step on this node, whatever
// its role. That is for a replica set that lost its majority; writes that
// only the lost members had are gone.
func (w *Worker) reconfig(ctx context.Context, members []string, force bool) error {
	if len(members) == 0 {
		return errors.New("reconfig without members")
	}
	if mon, err := w.current(); err == nil {
		defer w.refreshStatus(ctx, mon)
	}
	c, done, err := w.session(ctx)
	if err != nil {
		return err
	}
	defer done()

	admin := c.Database("admin")
	deadline := time.Now().Add(5 * time.Minute)
	var lastErr error
	for {
		conf, err := getConfig(ctx, c)
		if err != nil {
			return fmt.Errorf("replSetGetConfig: %w", err)
		}
		if sameHosts(conf.hosts(), members) {
			return nil
		}

		next := conf
		next.Version++
		if force {
			next.Members = nil
			for _, host := range members {
				m := rsMember{ID: -1, Host: host}
				for _, old := range conf.Members {
					if old.Host == host {
						m = old
					}
				}
				next.Members = append(next.Members, m)
			}
			for i := range next.Members {
				if next.Members[i].ID < 0 {
					next.Members[i].ID = max(conf.nextID(), next.nextID())
				}
			}
		} else if i := slices.IndexFunc(conf.Members, func(m rsMember) bool { return !slices.Contains(members, m.Host) }); i >= 0 {
			log.Printf("[mongo] reconfig: removing %s", conf.Members[i].Host)
			next.Members = slices.Delete(slices.Clone(conf.Members), i, i+1)
		} else {
			have := conf.hosts()
			i := slices.IndexFunc(members, func(h string) bool { return !slices.Contains(have, h) })
			log.Printf("[mongo] reconfig: adding %s", members[i])
			next.Members = append(slices.Clone(conf.Members), rsMember{ID: conf.nextID(), Host: members[i]})
		}

		cmd := bson.D{{Key: "replSetReconfig", Value: next}, {Key: "maxTimeMS", Value: 30000}}
		if force {
			log.Printf("[mongo] forced reconfig to %v", members)
			cmd = append(cmd, bson.E{Key: "force", Value: true})
		}
		err = admin.RunCommand(ctx, cmd).Err()
		switch {
		case err == nil:
			continue // re-read: done, or the next single step
		case !force && commandCode(err) == codeNotWritablePrimary:
			return fmt.Errorf("replSetReconfig: %w", err)
		}
		// Typically the previous config is not committed yet; retry.
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("replSetReconfig: %w", lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
