package store

import (
	"fmt"
	"math"
)

// Entity IDs are signed-range values kept in uint64 storage. The value above
// MaxInt64 is reserved as the exhausted high-water sentinel.
const (
	MaxEntityID       = uint64(math.MaxInt64)
	EntityIDExhausted = MaxEntityID + 1
)

func ValidateEntityID(id uint64) error {
	if id == 0 || id > MaxEntityID {
		return fmt.Errorf("entity ID must be in 1..%d", MaxEntityID)
	}
	return nil
}

func ValidateIDHighWater(next uint64) error {
	if next == 0 || next > EntityIDExhausted {
		return fmt.Errorf("entity ID high-water mark must be in 1..%d", EntityIDExhausted)
	}
	return nil
}
