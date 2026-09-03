package store

import "testing"

func TestEntityIDDomain(t *testing.T) {
	for _, id := range []uint64{1, MaxEntityID} {
		if err := ValidateEntityID(id); err != nil {
			t.Fatalf("valid ID %d: %v", id, err)
		}
	}
	for _, id := range []uint64{0, EntityIDExhausted} {
		if err := ValidateEntityID(id); err == nil {
			t.Fatalf("accepted invalid ID %d", id)
		}
	}
	if err := ValidateIDHighWater(EntityIDExhausted); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIDHighWater(EntityIDExhausted + 1); err == nil {
		t.Fatal("accepted overflowing high-water mark")
	}
}
