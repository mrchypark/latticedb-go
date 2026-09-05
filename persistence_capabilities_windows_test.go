//go:build windows

package latticedb

import "testing"

func TestPlatformPersistenceCapabilitiesWindows(t *testing.T) {
	got := PlatformPersistenceCapabilities()
	if !got.FileLocking || !got.LinkIdentityProtection || got.DirectorySync || got.FullDurability {
		t.Fatalf("capabilities = %+v", got)
	}
}
