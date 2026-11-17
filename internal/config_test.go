package internal

import (
	"slices"
	"strings"
	"testing"
)

const rolesConfig = `
expected_bootstrap: 2
mongo: {bin: mongod}
time: {state_file: t.json}
nodes:
  - id: s1
    raft_addr: 127.0.0.1:7001
    http_addr: 127.0.0.1:8001
    mongo_addr: 127.0.0.1:27001
  - id: s2
    roles: [server]
    raft_addr: 127.0.0.1:7002
    http_addr: 127.0.0.1:8002
  - id: w1
    roles: [worker]
    controllers: [time]
    http_addr: 10.0.0.5:8003
`

func TestRoles(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(rolesConfig))
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := cfg.Node("s1")
	if !s1.IsServer() || !s1.IsWorker() || !slices.Equal(s1.Controllers, []string{"mongo", "time"}) {
		t.Errorf("s1 defaults: %+v", s1)
	}
	s2, _ := cfg.Node("s2")
	if !s2.IsServer() || s2.IsWorker() || s2.RunsWorker("time") {
		t.Errorf("s2 should be server only: %+v", s2)
	}
	w1, _ := cfg.Node("w1")
	if w1.IsServer() || !w1.RunsWorker("time") || w1.RunsWorker("mongo") {
		t.Errorf("w1 should run only the time worker: %+v", w1)
	}
	if w1.Host() != "10.0.0.5" {
		t.Errorf("w1 host %q", w1.Host())
	}
	if got := ids(cfg.WorkersOf("time")); !slices.Equal(got, []string{"s1", "w1"}) {
		t.Errorf("time workers %v", got)
	}
	if got := ids(cfg.Servers()); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Errorf("servers %v", got)
	}
}

func TestRolesInvalid(t *testing.T) {
	cases := map[string]string{
		"unknown role":               "nodes: [{id: a, roles: [boss], raft_addr: x:1, http_addr: x:2}]",
		"server needs raft":          "nodes: [{id: a, roles: [server], http_addr: x:2}]",
		"no server":                  "nodes: [{id: a, roles: [worker], http_addr: x:2}]",
		"not enabled":                "nodes: [{id: a, controllers: [kafka], raft_addr: x:1, http_addr: x:2}]",
		"mongo needs addr":           "mongo: {bin: m}\nnodes: [{id: a, raft_addr: x:1, http_addr: x:2}]",
		"bootstrap > servers":        "expected_bootstrap: 2\nnodes: [{id: a, raft_addr: x:1, http_addr: x:2}]",
		"controllers on server only": "time: {state_file: t}\nnodes: [{id: a, roles: [server], controllers: [time], raft_addr: x:1, http_addr: x:2}]",
	}
	for name, yml := range cases {
		if _, err := ParseConfigBytes([]byte(yml)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), "invalid config") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func ids(nodes []NodeConfig) []string {
	var out []string
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}
