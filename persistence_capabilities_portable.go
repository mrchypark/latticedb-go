//go:build js || plan9 || wasip1

package latticedb

func platformPersistenceCapabilities() PersistenceCapabilities {
	return PersistenceCapabilities{}
}
