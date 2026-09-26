//go:build !aix && !android && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package pagestore

import "context"

type DB struct{}
type Tx struct{}

func Open(string, Options) (*DB, error)        { return nil, ErrUnsupportedPlatform }
func (*DB) Begin(bool) (*Tx, error)            { return nil, ErrUnsupportedPlatform }
func (*DB) Close() error                       { return ErrUnsupportedPlatform }
func (*DB) Sync() error                        { return ErrUnsupportedPlatform }
func (*Tx) Get(string, []byte) ([]byte, error) { return nil, ErrUnsupportedPlatform }
func (*Tx) Put(string, []byte, []byte) error   { return ErrUnsupportedPlatform }
func (*Tx) Delete(string, []byte) error        { return ErrUnsupportedPlatform }
func (*Tx) Scan(context.Context, string, []byte, []byte, func([]byte, []byte) error) error {
	return ErrUnsupportedPlatform
}
func (*Tx) Commit() error                  { return ErrUnsupportedPlatform }
func (*Tx) Rollback() error                { return ErrUnsupportedPlatform }
func (*Tx) BufferedBytes() (uint64, error) { return 0, ErrUnsupportedPlatform }
