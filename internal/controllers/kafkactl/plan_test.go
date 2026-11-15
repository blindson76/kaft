package kafkactl

import (
	"slices"
	"strings"
	"testing"
)

var topo = Topology{
	Size:        3,
	IDs:         map[string]int{"n1": 1, "n2": 2, "n3": 3, "n4": 4, "n5": 5},
	Controllers: map[string]string{"n1": "h1:9", "n2": "h2:9", "n3": "h3:9", "n4": "h4:9", "n5": "h5:9"},
}

func ready(dir string) Report { return Report{ProbeID: "p", State: StateReady, DirID: dir} }

func running(dir string) Report {
	r := ready(dir)
	r.Running = true
	return r
}

func data(reports map[string]Report) Data {
	d := Data{Alive: map[string]bool{}, Reports: reports}
	for n := range reports {
		d.Alive[n] = true
	}
	return d
}

// summary renders a plan's steps as "kind@node" lists, one string per step.
func summary(p Plan) []string {
	var out []string
	for _, step := range p.Steps {
		var orders []string
		for _, o := range step {
			orders = append(orders, o.Kind+"@"+o.Node)
		}
		out = append(out, strings.Join(orders, " "))
	}
	return out
}

func TestFormPlan(t *testing.T) {
	if _, ok := formPlan(data(map[string]Report{"n1": ready("a"), "n2": ready("b")}), topo); ok {
		t.Fatal("planned with fewer nodes than the cluster size")
	}

	d := data(map[string]Report{
		"n4": ready("d"), "n2": ready("b"), "n1": {ProbeID: "p", State: StateBroken}, "n3": ready("c"), "n5": ready("e"),
	})
	d.Alive["n3"] = false
	p, ok := formPlan(d, topo)
	if !ok || !slices.Equal(p.Members, []string{"n2", "n4", "n5"}) {
		t.Fatalf("members = %v, want the healthy nodes with the lowest IDs", p.Members)
	}
	want := []string{
		"kafka.start@n2",
		"kafka.start@n4 kafka.start@n5",
		"kafka.addvoter@n4",
		"kafka.addvoter@n5",
	}
	if got := summary(p); !slices.Equal(got, want) {
		t.Fatalf("steps:\n got %q\nwant %q", got, want)
	}
	if p.Steps[0][0].Args["bootstrap"] != true {
		t.Fatal("first member is not started as the bootstrap node")
	}
}

func established() Data {
	d := data(map[string]Report{
		"n1": running("a"), "n2": running("b"), "n3": running("c"), "n4": ready("d"),
	})
	d.Members = []string{"n1", "n2", "n3"}
	d.Voters = map[string]string{"n1": "a", "n2": "b", "n3": "c"}
	return d
}

func TestRepairPlan(t *testing.T) {
	t.Run("healthy cluster needs nothing", func(t *testing.T) {
		if _, ok := repairPlan(established(), topo); ok {
			t.Fatal("repair planned for a healthy cluster")
		}
	})

	t.Run("dead member is replaced by a spare and partitions are moved", func(t *testing.T) {
		d := established()
		d.Alive["n2"] = false
		p, ok := repairPlan(d, topo)
		if !ok || !slices.Equal(p.Members, []string{"n1", "n4", "n3"}) {
			t.Fatalf("members = %v ok=%v", p.Members, ok)
		}
		want := []string{"kafka.start@n4", "kafka.removevoter@n1", "kafka.addvoter@n4", "kafka.reassign@n1"}
		if got := summary(p); !slices.Equal(got, want) {
			t.Fatalf("steps:\n got %q\nwant %q", got, want)
		}
		removed := p.Steps[1][0].Args["voters"].([]map[string]any)
		if removed[0]["id"] != 2 || removed[0]["dir"] != "b" {
			t.Fatalf("removes %v, want the old voter 2/b", removed)
		}
		if got := p.Steps[3][0].Args["brokers"]; got != "1,4,3" {
			t.Fatalf("reassign brokers = %v", got)
		}
	})

	t.Run("stopped member with its directory is just restarted", func(t *testing.T) {
		d := established()
		d.Reports["n2"] = ready("b")
		p, ok := repairPlan(d, topo)
		if got := summary(p); !ok || !slices.Equal(got, []string{"kafka.start@n2"}) {
			t.Fatalf("steps = %q ok=%v", got, ok)
		}
	})

	t.Run("reformatted member rejoins as a new voter, no reassignment", func(t *testing.T) {
		d := established()
		d.Reports["n2"] = ready("b2")
		p, ok := repairPlan(d, topo)
		want := []string{"kafka.start@n2", "kafka.removevoter@n1", "kafka.addvoter@n2"}
		if got := summary(p); !ok || !slices.Equal(got, want) {
			t.Fatalf("steps:\n got %q\nwant %q", got, want)
		}
		if !slices.Equal(p.Members, d.Members) {
			t.Fatalf("members changed: %v", p.Members)
		}
	})

	t.Run("dead member without a spare is left alone", func(t *testing.T) {
		d := established()
		d.Alive["n2"], d.Alive["n4"] = false, false
		if _, ok := repairPlan(d, topo); ok {
			t.Fatal("repair planned without a spare")
		}
	})

	t.Run("running non-member is stopped", func(t *testing.T) {
		d := established()
		d.Reports["n4"] = running("d")
		p, ok := repairPlan(d, topo)
		if got := summary(p); !ok || !slices.Equal(got, []string{"kafka.stop@n4"}) {
			t.Fatalf("steps = %q ok=%v", got, ok)
		}
	})

	t.Run("lost quorum is not repaired", func(t *testing.T) {
		d := established()
		d.Alive["n2"], d.Alive["n3"] = false, false
		if !d.quorumLost() {
			t.Fatal("quorum loss not detected with 1 of 3 members healthy")
		}
		if _, ok := repairPlan(d, topo); ok {
			t.Fatal("repair planned without a quorum")
		}
	})
}
