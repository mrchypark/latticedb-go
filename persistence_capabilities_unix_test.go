//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package latticedb

import "testing"

func TestPlatformPersistenceCapabilitiesUnix(t *testing.T) {
	got := PlatformPersistenceCapabilities()
	if !got.FileLocking || !got.LinkIdentityProtection || !got.DirectorySync || !got.FullDurability {
		t.Fatalf("capabilities = %+v", got)
	}
}
