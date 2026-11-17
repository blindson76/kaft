package internal

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const envConfig = `
expected_bootstrap: ${KAFT_EXPECTED:-1}
mongo:
  bin: ${KAFT_MONGO_HOME}\bin\mongod.exe
  # comments are left alone: $NOT_SET
  min_members: ${KAFT_MIN}
  password: "pa$$word"
  user: ${KAFT_USER:-admin}
  decision_window: ${KAFT_WINDOW}
nodes:
  - id: node-1
    raft_addr: ${KAFT_HOST}:7001
    http_addr: $KAFT_HOST:8001
    mongo_addr: "${KAFT_HOST}:27001"
`

func TestConfigExpandsEnvironment(t *testing.T) {
	t.Setenv("KAFT_MONGO_HOME", `C:\mongo`)
	t.Setenv("KAFT_MIN", "1")
	t.Setenv("KAFT_HOST", "10.0.0.7")
	t.Setenv("KAFT_WINDOW", "45s")
	t.Setenv("KAFT_USER", "") // empty: the default applies

	cfg, err := ParseConfigBytes([]byte(envConfig))
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Mongo
	switch {
	case cfg.ExpectedBootstrap != 1:
		t.Errorf("expected_bootstrap = %d, want the default 1", cfg.ExpectedBootstrap)
	case m.Bin != `C:\mongo\bin\mongod.exe`:
		t.Errorf("bin = %q", m.Bin)
	case m.MinMembers != 1:
		t.Errorf("min_members = %d, want 1 (typed after expansion)", m.MinMembers)
	case m.Password != "pa$word":
		t.Errorf("password = %q, want $$ to give a literal $", m.Password)
	case m.User != "admin":
		t.Errorf("user = %q, want the default", m.User)
	case m.DecisionWindow != 45*time.Second:
		t.Errorf("decision_window = %s", m.DecisionWindow)
	}
	n := cfg.Nodes[0]
	if n.RaftAddr != "10.0.0.7:7001" || n.HTTPAddr != "10.0.0.7:8001" || n.MongoAddr != "10.0.0.7:27001" {
		t.Errorf("addresses %q %q %q", n.RaftAddr, n.HTTPAddr, n.MongoAddr)
	}
}

func TestConfigReportsMissingVariables(t *testing.T) {
	_, err := ParseConfigBytes([]byte("expected_bootstrap: ${KAFT_A}\nmongo: {bin: $KAFT_B, user: $KAFT_A}\n"))
	if err == nil || !strings.Contains(err.Error(), "KAFT_A, KAFT_B") {
		t.Fatalf("err = %v, want both missing variables named once", err)
	}
}

func TestExpandEnvLeavesKeysAlone(t *testing.T) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte("$KEY: $VAL\n"), &doc); err != nil {
		t.Fatal(err)
	}
	lookup := func(name string) (string, bool) { return "x", true }
	if err := expandEnv(&doc, lookup); err != nil {
		t.Fatal(err)
	}
	m := doc.Content[0]
	if m.Content[0].Value != "$KEY" || m.Content[1].Value != "x" {
		t.Fatalf("key %q value %q", m.Content[0].Value, m.Content[1].Value)
	}
}
