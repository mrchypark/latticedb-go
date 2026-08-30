//go:build windows

package exporter

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	exportKernel32     = syscall.NewLazyDLL("kernel32.dll")
	exportLockFileEx   = exportKernel32.NewProc("LockFileEx")
	exportUnlockFileEx = exportKernel32.NewProc("UnlockFileEx")
)

func tryLockExportFile(file *os.File) (bool, error) {
	var overlapped syscall.Overlapped
	result, _, err := exportLockFileEx.Call(file.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if result == 0 {
		if err == syscall.Errno(33) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func unlockExportFile(file *os.File) error {
	var overlapped syscall.Overlapped
	result, _, err := exportUnlockFileEx.Call(file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if result == 0 {
		return err
	}
	return nil
}
