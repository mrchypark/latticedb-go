//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package store

import (
	"os"
	"syscall"
)

func syncFileStandard(file *os.File) error {
	return syscall.Fsync(int(file.Fd()))
}
