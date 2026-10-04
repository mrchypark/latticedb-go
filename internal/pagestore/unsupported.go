//go:build !aix && !android && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package pagestore

import "context"

type DB struct{}
type Tx struct{}

func Open(string, Options) (*DB, error)                       { return nil, ErrUnsupportedPlatform }
func (*DB) Begin(bool) (*Tx, error)                           { return nil, ErrUnsupportedPlatform }
func (*DB) Close() error                                      { return ErrUnsupportedPlatform }
func (*DB) Sync() error                                       { return ErrUnsupportedPlatform }
func (*Tx) Get(string, []byte) ([]byte, error)                { return nil, ErrUnsupportedPlatform }
func (*Tx) Has(string, []byte) (bool, error)                  { return false, ErrUnsupportedPlatform }
func (*Tx) GetBounded(string, []byte, uint64) ([]byte, error) { return nil, ErrUnsupportedPlatform }
func (*Tx) GetBoundedWithCharge(string, []byte, uint64, func(uint64) error) ([]byte, error) {
	return nil, ErrUnsupportedPlatform
}
func (*Tx) Put(string, []byte, []byte) error { return ErrUnsupportedPlatform }
func (*Tx) Delete(string, []byte) error      { return ErrUnsupportedPlatform }
func (*Tx) Scan(context.Context, string, []byte, []byte, func([]byte, []byte) error) error {
	return ErrUnsupportedPlatform
}
func (*Tx) ScanKeys(context.Context, string, []byte, []byte, func([]byte) error) error {
	return ErrUnsupportedPlatform
}
func (*Tx) Commit() error                  { return ErrUnsupportedPlatform }
func (*Tx) Rollback() error                { return ErrUnsupportedPlatform }
func (*Tx) BufferedBytes() (uint64, error) { return 0, ErrUnsupportedPlatform }

func (*DB) Compact(context.Context) error { return ErrUnsupportedPlatform }

func (*Tx) ScanBounded(context.Context, string, []byte, []byte, uint64, func([]byte, []byte) error) error {
	return ErrUnsupportedPlatform
}

func (*Tx) ScanKeysWithCharge(context.Context, string, []byte, []byte, uint64, func(uint64) error, func([]byte) error) error {
	return ErrUnsupportedPlatform
}
