package mongoctl

import (
	"slices"
	"testing"
)

var topo = Topology{Size: 3, Addrs: map[string]string{
	"n1": "h1:1", "n2": "h2:1", "n3": "h3:1", "n4": "h4:1", "n5": "h5:1", "n6": "h6:1",
}}

func fresh() Report { return Report{Boot: Boot{ProbeID: "p", State: StateFresh}} }

func broken() Report { return Report{Boot: Boot{ProbeID: "p", State: StateBroken}} }

// member builds a report of a node holding data of replica set rs, whose
// stored config lists hosts, with an oplog spanning seconds first..last.
func member(rs string, term int64, first, last uint32, hosts ...string) Report {
	return Report{Boot: Boot{
		ProbeID: "p", State: StateMember, ReplSetID: rs, Members: hosts,
		FirstOp: OpTime{Term: 1, T: first}, LastOp: OpTime{Term: term, T: last},
	}}
}

func data(reports map[string]Report) Data {
	d := Data{Alive: map[string]bool{}, Reports: reports}
	for n := range reports {
		d.Alive[n] = true
	}
	return d
}

func TestFormPlan(t *testing.T) {
	abc := []string{"h1:1", "h2:1", "h3:1"}

	tests := []struct {
		name    string
		reports map[string]Report
		want    Plan
		wantOK  bool
	}{
		{
			name:    "too few reports",
			reports: map[string]Report{"n1": fresh(), "n2": fresh()},
		},
		{
			name:    "all fresh: initiate on the lowest IDs",
			reports: map[string]Report{"n4": fresh(), "n2": fresh(), "n1": fresh(), "n3": fresh()},
			want:    Plan{Members: []string{"n1", "n2", "n3"}, Mode: ModeInitiate, Target: "n1"},
			wantOK:  true,
		},
		{
			name: "existing set intact: adopt, most up-to-date first",
			reports: map[string]Report{
				"n1": member("A", 3, 10, 100, abc...),
				"n2": member("A", 3, 10, 120, abc...),
				"n3": member("A", 3, 10, 110, abc...),
				"n4": fresh(),
			},
			want:   Plan{Members: []string{"n2", "n3", "n1"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "higher term beats later timestamp",
			reports: map[string]Report{
				"n1": member("A", 4, 10, 100, abc...),
				"n2": member("A", 3, 10, 500, abc...),
				"n3": member("A", 3, 10, 90, abc...),
			},
			want:   Plan{Members: []string{"n1", "n2", "n3"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "one member lost: majority kept, fresh node fills in",
			reports: map[string]Report{
				"n1": member("A", 3, 10, 100, abc...),
				"n2": member("A", 3, 10, 100, abc...),
				"n4": fresh(),
			},
			want:   Plan{Members: []string{"n1", "n2", "n4"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "majority of the old config lost: force on the survivor",
			reports: map[string]Report{
				"n3": member("A", 3, 10, 100, abc...),
				"n4": fresh(),
				"n5": fresh(),
			},
			want:   Plan{Members: []string{"n3", "n4", "n5"}, Mode: ModeForce, Target: "n3"},
			wantOK: true,
		},
		{
			name: "node behind the oplog window is wiped, and only used last",
			reports: map[string]Report{
				"n1": member("A", 3, 50, 100, abc...),
				"n2": member("A", 3, 50, 100, abc...),
				"n3": member("A", 2, 5, 40, abc...), // newest entry older than n1's oldest
			},
			want:   Plan{Members: []string{"n1", "n2", "n3"}, Wipe: []string{"n3"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "stale node is skipped when a fresh one is available",
			reports: map[string]Report{
				"n1": member("A", 3, 50, 100, abc...),
				"n2": member("A", 3, 50, 100, abc...),
				"n3": member("A", 2, 5, 40, abc...),
				"n4": fresh(),
			},
			want:   Plan{Members: []string{"n1", "n2", "n4"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "node inside the oplog window catches up without a wipe",
			reports: map[string]Report{
				"n1": member("A", 3, 50, 100, abc...),
				"n2": member("A", 3, 50, 100, abc...),
				"n3": member("A", 3, 5, 60, abc...),
			},
			want:   Plan{Members: []string{"n1", "n2", "n3"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "two replica sets: the newer data wins, the other is wiped",
			reports: map[string]Report{
				"n1": member("OLD", 9, 10, 100, "h1:1"),
				"n2": member("A", 2, 10, 300, "h2:1", "h3:1"),
				"n3": member("A", 2, 10, 300, "h2:1", "h3:1"),
			},
			// Term decides first, so OLD (term 9) is the newer one here.
			want:   Plan{Members: []string{"n1", "n2", "n3"}, Wipe: []string{"n2", "n3"}, Mode: ModeAdopt},
			wantOK: true,
		},
		{
			name: "broken nodes are wiped, and only used last",
			reports: map[string]Report{
				"n1": broken(), "n2": fresh(), "n3": fresh(), "n4": broken(),
			},
			want:   Plan{Members: []string{"n2", "n3", "n1"}, Wipe: []string{"n1"}, Mode: ModeInitiate, Target: "n2"},
			wantOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := formPlan(data(tc.reports), topo)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			tc.want.Start = tc.want.Members
			if !samePlan(got, tc.want) {
				t.Fatalf("\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func samePlan(a, b Plan) bool {
	return slices.Equal(a.Members, b.Members) && slices.Equal(a.Wipe, b.Wipe) &&
		slices.Equal(a.Start, b.Start) && slices.Equal(a.Stop, b.Stop) &&
		a.Mode == b.Mode && a.Target == b.Target
}

func TestFormPlanIgnoresDeadNodes(t *testing.T) {
	d := data(map[string]Report{"n1": fresh(), "n2": fresh(), "n3": fresh(), "n4": fresh()})
	d.Alive["n1"] = false
	got, ok := formPlan(d, topo)
	if !ok || !slices.Equal(got.Members, []string{"n2", "n3", "n4"}) {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestRepairPlan(t *testing.T) {
	d := data(map[string]Report{
		"n1": fresh(), "n2": fresh(), "n3": fresh(),
		"n4": member("OLD", 1, 1, 2, "h4:1"), "n5": fresh(),
	})
	d.Members = []string{"n1", "n2", "n3"}

	if _, ok := repairPlan(d, topo); ok {
		t.Fatal("repair planned although every member is alive")
	}

	d.Alive["n2"] = false
	got, ok := repairPlan(d, topo)
	want := Plan{Members: []string{"n1", "n3", "n5"}, Start: []string{"n5"}, Stop: []string{"n2"}, Mode: ModeReconfig}
	if !ok || !samePlan(got, want) {
		t.Fatalf("fresh spare preferred:\n got %+v\nwant %+v", got, want)
	}

	d.Alive["n5"] = false
	got, ok = repairPlan(d, topo)
	want = Plan{Members: []string{"n1", "n3", "n4"}, Wipe: []string{"n4"}, Start: []string{"n4"}, Stop: []string{"n2"}, Mode: ModeReconfig}
	if !ok || !samePlan(got, want) {
		t.Fatalf("non-fresh spare is wiped:\n got %+v\nwant %+v", got, want)
	}

	d.Alive["n4"] = false
	if _, ok := repairPlan(d, topo); ok {
		t.Fatal("repair planned without a spare")
	}

	d.Alive["n3"] = false
	if !d.quorumLost() {
		t.Fatal("quorum loss not detected with 1 of 3 members alive")
	}
	if _, ok := repairPlan(d, topo); ok {
		t.Fatal("repair planned without a majority")
	}
}

func TestRejoinPlan(t *testing.T) {
	stopped := fresh()
	stopped.Status.Role = RoleStopped
	running := fresh()
	running.Status.Role = RoleSecondary

	d := data(map[string]Report{"n1": running, "n2": stopped, "n3": running, "n4": stopped})
	d.Members = []string{"n1", "n2", "n3"}

	got, ok := rejoinPlan(d)
	if !ok || !slices.Equal(got.Start, []string{"n2"}) || got.Mode != ModeNone {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}
