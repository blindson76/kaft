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

const tasksConfig = `
expected_bootstrap: 1
nodes:
  - {id: s1, roles: [server], raft_addr: x:1, http_addr: x:2}
  - {id: w1, roles: [worker], controllers: [], http_addr: x:3}
  - {id: w2, roles: [worker], controllers: [], http_addr: x:4}
tasks:
  - name: api
    command: api.exe
    instances: 2
    nodes: [w2]
    depends_on: [{controller: kafka, states: [monitoring]}]
  - name: indexer
    command: idx.exe
`

func TestTasks(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(tasksConfig))
	if err != nil {
		t.Fatal(err)
	}
	api, idx := cfg.Tasks[0], cfg.Tasks[1]
	if got := ids(cfg.TaskNodes(api)); !slices.Equal(got, []string{"w2"}) {
		t.Errorf("api nodes %v", got)
	}
	if got := ids(cfg.TaskNodes(idx)); !slices.Equal(got, []string{"w1", "w2"}) {
		t.Errorf("indexer nodes %v, want every worker", got)
	}
	if idx.Instances != 1 {
		t.Errorf("indexer instances %d, want the default 1", idx.Instances)
	}
	if len(api.DependsOn) != 1 || api.DependsOn[0].Controller != "kafka" {
		t.Errorf("api depends_on %+v", api.DependsOn)
	}
}

func TestTasksInvalid(t *testing.T) {
	base := "nodes: [{id: a, raft_addr: x:1, http_addr: x:2}, {id: s, roles: [server], raft_addr: y:1, http_addr: y:2}]\n"
	cases := map[string]string{
		"no name":        "tasks: [{command: c}]",
		"bad name":       "tasks: [{name: My_Task, command: c}]",
		"builtin name":   "tasks: [{name: kafka, command: c}]",
		"twice":          "tasks: [{name: t, command: c}, {name: t, command: c}]",
		"no command":     "tasks: [{name: t}]",
		"unknown node":   "tasks: [{name: t, command: c, nodes: [zz]}]",
		"not a worker":   "tasks: [{name: t, command: c, nodes: [s]}]",
		"does not fit":   "tasks: [{name: t, command: c, instances: 3, max_per_node: 2}]",
		"negative":       "tasks: [{name: t, command: c, instances: -1}]",
		"listed on node": "tasks: [{name: t, command: c}]\nnodes: [{id: a, controllers: [t], raft_addr: x:1, http_addr: x:2}]",
	}
	for name, yml := range cases {
		doc := yml
		if !strings.Contains(yml, "nodes: [{") {
			doc = base + yml
		}
		if _, err := ParseConfigBytes([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), "invalid config") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

const rolesTaskConfig = `
expected_bootstrap: 1
nodes:
  - {id: s1, raft_addr: x:1, http_addr: x:2}
  - {id: s2, raft_addr: y:1, http_addr: y:2}
tasks:
  - name: cache
    command: redis-server.exe
    args: ["--port", "6379"]
    max_per_node: 1
    roles:
      - name: master
      - name: slave
        args: ["--replicaof", "{host:master}", "6379"]
`

func TestTaskRoles(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(rolesTaskConfig))
	if err != nil {
		t.Fatal(err)
	}
	task := cfg.Tasks[0]
	if task.Instances != 2 || task.Roles[0].Count != 1 {
		t.Fatalf("instances %d, roles %+v", task.Instances, task.Roles)
	}
	if got := task.RoleRefs(task.Roles[1]); !slices.Equal(got, []string{"master"}) {
		t.Errorf("slave refers to %v", got)
	}
	if got := task.RoleRefs(task.Roles[0]); len(got) != 0 {
		t.Errorf("master refers to %v", got)
	}
}

func TestTaskRolesInvalid(t *testing.T) {
	base := "nodes: [{id: a, raft_addr: x:1, http_addr: x:2}]\ntasks:\n  - name: t\n    command: c\n"
	cases := map[string]string{
		"ref without roles": `    args: ["{host:master}"]`,
		"unknown role":      `    roles: [{name: slave, args: ["{host:master}"]}]`,
		"self":              `    roles: [{name: master, args: ["{node:master}"]}]`,
		"many instances":    `    roles: [{name: master, count: 2}, {name: slave, args: ["{host:master}"]}]`,
		"cycle":             `    roles: [{name: a, args: ["{host:b}"]}, {name: b, env: {X: "{node:a}"}}]`,
		"count mismatch":    "    instances: 3\n    roles: [{name: a}, {name: b}]",
		"duplicate role":    "    roles: [{name: a}, {name: a}]",
		"bad role name":     "    roles: [{name: Master}]",
		"negative count":    "    roles: [{name: a, count: -1}]",
	}
	want := map[string]string{
		"ref without roles": "the task has no roles",
		"unknown role":      "unknown role",
		"self":              "its own placement",
		"many instances":    "only a role with one",
		"cycle":             "cycle",
		"count mismatch":    "add up to",
		"duplicate role":    "defined twice",
		"bad role name":     "lower-case",
		"negative count":    "at least 1",
	}
	for name, yml := range cases {
		if _, err := ParseConfigBytes([]byte(base + yml + "\n")); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), want[name]) {
			t.Errorf("%s: %v, want an error about %q", name, err, want[name])
		}
	}
}
