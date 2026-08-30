//go:build windows

package store

import "os"

func syncFileStandard(file *os.File) error {
	return file.Sync()
}
