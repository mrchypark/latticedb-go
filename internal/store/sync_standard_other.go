//go:build js || plan9 || wasip1 || windows

package store

import "os"

func syncFileStandard(file *os.File) error {
	return file.Sync()
}
