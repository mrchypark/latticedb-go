//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd

package engine

import (
	"errors"
	"os"
	"syscall"
)

func regularFileHasMultipleLinks(_ string, info os.FileInfo) (bool, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, errors.New("inspect database file links")
	}
	return stat.Nlink > 1, nil
}
