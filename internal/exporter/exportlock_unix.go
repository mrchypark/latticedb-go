//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package exporter

import (
	"errors"
	"os"
	"syscall"
)

func tryLockExportFile(file *os.File) (bool, error) {
	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 1}
	err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock)
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EACCES) {
		return false, nil
	}
	return err == nil, err
}

func unlockExportFile(file *os.File) error {
	lock := syscall.Flock_t{Type: syscall.F_UNLCK, Whence: 0, Start: 0, Len: 1}
	return syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock)
}
