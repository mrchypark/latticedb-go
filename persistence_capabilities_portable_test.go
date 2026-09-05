//go:build js || plan9 || wasip1

package latticedb

import "testing"

func TestPlatformPersistenceCapabilitiesPortable(t *testing.T) {
	if got := PlatformPersistenceCapabilities(); got != (PersistenceCapabilities{}) {
		t.Fatalf("capabilities = %+v", got)
	}
}
