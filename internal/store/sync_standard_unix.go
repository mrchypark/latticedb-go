//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd

package store

import (
	"os"
	"syscall"
)

func syncFileStandard(file *os.File) error {
	return syscall.Fsync(int(file.Fd()))
}
