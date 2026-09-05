//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package latticedb

func platformPersistenceCapabilities() PersistenceCapabilities {
	return PersistenceCapabilities{
		FileLocking:            true,
		LinkIdentityProtection: true,
		DirectorySync:          true,
		FullDurability:         true,
	}
}
