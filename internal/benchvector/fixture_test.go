package benchvector

import "testing"

func TestZigHarnessRNGMatchesZig016(t *testing.T) {
	rng := newZigHarnessRNG(0)
	for i, want := range [...]uint64{
		0x53175d61490b23df,
		0x61da6f3dc380d507,
		0x5c0fdf91ec9a7bfc,
		0x02eebf8c3bbe5e1a,
	} {
		if got := rng.next(); got != want {
			t.Fatalf("value %d = %#x, want %#x", i, got, want)
		}
	}
}
