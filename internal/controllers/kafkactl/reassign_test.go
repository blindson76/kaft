package kafkactl

import (
	"encoding/json"
	"slices"
	"testing"
)

// Output of kafka-reassign-partitions --generate, as printed by Kafka 4.1.
const generateOutput = `Current partition replica assignment
{"version":1,"partitions":[{"topic":"demo","partition":0,"replicas":[3,1,2],"log_dirs":["any","any","any"]},{"topic":"demo","partition":1,"replicas":[1,3,4],"log_dirs":["any","any","any"]}]}

Proposed partition reassignment configuration
{"version":1,"partitions":[{"topic":"demo","partition":0,"replicas":[3,4,1],"log_dirs":["any","any","any"]},{"topic":"demo","partition":1,"replicas":[4,1,3],"log_dirs":["any","any","any"]}]}
`

func TestReassignParsing(t *testing.T) {
	cur, ok := jsonAfter(generateOutput, "Current partition replica assignment")
	if !ok {
		t.Fatal("current assignment not found")
	}
	var a assignment
	if err := json.Unmarshal([]byte(cur), &a); err != nil {
		t.Fatal(err)
	}
	if got := a.misplaced([]int{1, 3, 4}); !slices.Equal(got, []string{"demo-0[3 1 2]"}) {
		t.Fatalf("misplaced = %v, want only the partition with a replica on broker 2", got)
	}
	if got := a.misplaced([]int{1, 2, 3, 4}); len(got) != 0 {
		t.Fatalf("misplaced = %v, want none", got)
	}

	prop, ok := jsonAfter(generateOutput, "Proposed partition reassignment configuration")
	if !ok || json.Unmarshal([]byte(prop), &a) != nil || len(a.misplaced([]int{1, 3, 4})) != 0 {
		t.Fatalf("proposed assignment not parsed or not on target brokers: %q", prop)
	}
	if _, ok := jsonAfter("Error: something", "Current partition replica assignment"); ok {
		t.Fatal("found an assignment in an error output")
	}
}
