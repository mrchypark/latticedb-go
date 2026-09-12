//go:build windows

package store

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFileLinksQueryAllowsDeleteHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	// A concurrent replacement needs DELETE access. A normal os.Open metadata
	// probe omits FILE_SHARE_DELETE and conflicts with this existing handle.
	const deleteAccess = 0x00010000
	handle, err := syscall.CreateFile(name, deleteAccess, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(handle)
	if multiple, err := FileHasMultipleLinks(path); err != nil || multiple {
		t.Fatalf("single link: multiple=%v error=%v", multiple, err)
	}
	if err := os.Link(path, path+"-link"); err != nil {
		t.Fatal(err)
	}
	if multiple, err := FileHasMultipleLinks(path); err != nil || !multiple {
		t.Fatalf("hard link: multiple=%v error=%v", multiple, err)
	}
}
