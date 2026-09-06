package engine

import "testing"

var queryRowAllocationSink queryRow

func TestQueryRowCloneKeepsBindingsIsolated(t *testing.T) {
	row := queryRow{slots: make([]boundValue, 2), index: map[string]int{"value": 0, "other": 1}}
	row.set("value", boundValue{Value: nil, HasValue: true})
	clone := row.clone()
	clone.set("other", boundValue{Value: int64(1), HasValue: true})

	if value, ok := row.get("value"); !ok || !value.HasValue || value.Value != nil {
		t.Fatalf("original value = %#v, %v", value, ok)
	}
	if _, ok := row.get("other"); ok {
		t.Fatal("clone mutation changed original row")
	}
	if value, ok := clone.get("other"); !ok || value.Value != int64(1) {
		t.Fatalf("clone value = %#v, %v", value, ok)
	}
}

func TestQueryRowCloneAllocatesOneSlotSlice(t *testing.T) {
	row := queryRow{slots: make([]boundValue, 3)}
	if allocations := testing.AllocsPerRun(100, func() { queryRowAllocationSink = row.clone() }); allocations != 1 {
		t.Fatalf("clone allocations = %v, want 1", allocations)
	}
}

func TestQueryPlanNewRowAllocatesOneSlotSlice(t *testing.T) {
	plan := queryPlan{slots: map[string]int{"value": 0, "other": 1}}
	if allocations := testing.AllocsPerRun(100, func() { queryRowAllocationSink = plan.newRow() }); allocations != 1 {
		t.Fatalf("new row allocations = %v, want 1", allocations)
	}
}
