//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package engine

import (
	"os"
	"syscall"
)

func tryLockFile(file *os.File, shared bool) error {
	typ := int16(syscall.F_WRLCK)
	if shared {
		typ = syscall.F_RDLCK
	}
	lock := syscall.Flock_t{Type: typ, Whence: 0, Start: 0, Len: 1}
	return syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock)
}

func unlockFile(file *os.File) error {
	lock := syscall.Flock_t{Type: syscall.F_UNLCK, Whence: 0, Start: 0, Len: 1}
	return syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock)
}
