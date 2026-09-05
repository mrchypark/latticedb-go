//go:build windows

package latticedb

func platformPersistenceCapabilities() PersistenceCapabilities {
	return PersistenceCapabilities{
		FileLocking:            true,
		LinkIdentityProtection: true,
	}
}
