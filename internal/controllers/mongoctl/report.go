// Package mongoctl orchestrates a MongoDB replica set: the Worker runs and
// inspects the node-local mongod, the controller decides which nodes form the
// replica set and keeps it at the configured size.
package mongoctl

import "cmp"

// Name is the controller / worker name.
const Name = "mongo"

// Order kinds handled by the Worker.
const (
	// OrderStart starts mongod as a replica set member.
	OrderStart = "mongo.start"
	// OrderStop stops mongod and probes the data again, refreshing Boot.
	OrderStop = "mongo.stop"
	// OrderWipe stops mongod, deletes its data and prepares a fresh node.
	OrderWipe = "mongo.wipe"
	// OrderInitiate creates the replica set on the target node and grows it
	// to args "members". Runs on a fresh node.
	OrderInitiate = "mongo.initiate"
	// OrderReconfig changes the replica set to args "members". Without
	// "force" it must run on the primary; with "force" on any member.
	OrderReconfig = "mongo.reconfig"
)

// Boot states.
const (
	// StateFresh: mongod starts, holds no replica set data, admin user exists.
	StateFresh = "fresh"
	// StateMember: mongod starts and holds the data of a replica set.
	StateMember = "member"
	// StateBroken: mongod could not be started or inspected. Only a wipe
	// makes the node usable.
	StateBroken = "broken"
)

// Roles reported in Status.
const (
	RoleStopped   = "stopped" // mongod is not running
	RoleNone      = "none"    // running, no replica set config yet
	RolePrimary   = "primary"
	RoleSecondary = "secondary"
	RoleOther     = "other" // startup, recovering, removed, ...
)

// OpTime is a position in the oplog. MongoDB orders them by term first.
type OpTime struct {
	Term int64  `json:"term"`
	T    uint32 `json:"t"` // seconds
	I    uint32 `json:"i"` // ordinal within the second
}

// Compare orders by (term, timestamp), the order MongoDB uses to decide which
// member is more up to date.
func (a OpTime) Compare(b OpTime) int {
	return cmp.Or(cmp.Compare(a.Term, b.Term), a.CompareTS(b))
}

// CompareTS orders by timestamp only.
func (a OpTime) CompareTS(b OpTime) int {
	return cmp.Or(cmp.Compare(a.T, b.T), cmp.Compare(a.I, b.I))
}

func (a OpTime) IsZero() bool { return a == OpTime{} }

// Boot is what a probe found in the node's data files. It is taken with
// mongod out of the replica set and does not change until the next probe
// (node start, OrderStop, OrderWipe), so the oplog position in it is a fixed
// point the controller can compare across nodes.
type Boot struct {
	// ProbeID is unique per probe, so a new probe always produces a new
	// report even when it finds the same thing.
	ProbeID string `json:"probe_id"`
	State   string `json:"state"`
	// Reason says why the state is broken.
	Reason string `json:"reason,omitempty"`

	// Replica set config stored in the data files (StateMember only).
	ReplSetID     string   `json:"replset_id,omitempty"`
	SetName       string   `json:"set_name,omitempty"`
	ConfigVersion int64    `json:"config_version,omitempty"`
	ConfigTerm    int64    `json:"config_term,omitempty"`
	Members       []string `json:"members,omitempty"` // host:port

	// Term is the last election term the node voted in.
	Term int64 `json:"term,omitempty"`
	// LastOp / FirstOp are the newest and oldest oplog entries.
	LastOp  OpTime `json:"last_op"`
	FirstOp OpTime `json:"first_op"`
}

// Status is the live state of the running mongod, updated from the driver's
// topology change events. Nothing in it changes with ordinary writes.
type Status struct {
	Role       string   `json:"role"`
	Primary    string   `json:"primary,omitempty"` // host:port
	SetVersion int64    `json:"set_version,omitempty"`
	Hosts      []string `json:"hosts,omitempty"`
}

// Report is the worker's report to the controller.
type Report struct {
	Boot   Boot   `json:"boot"`
	Status Status `json:"status"`
}
