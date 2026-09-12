//go:build windows

package store

import (
	"os"
	"syscall"
)

// FileHasMultipleLinks queries metadata without blocking concurrent replacement.
func FileHasMultipleLinks(path string) (bool, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}
	handle, err := syscall.CreateFile(name, 0, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return false, &os.PathError{Op: "open", Path: path, Err: err}
	}
	defer syscall.CloseHandle(handle)
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(handle, &info); err != nil {
		return false, err
	}
	return info.NumberOfLinks > 1, nil
}
