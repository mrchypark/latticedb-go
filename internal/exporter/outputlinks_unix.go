//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package exporter

import (
	"errors"
	"os"
	"syscall"
)

func exportOutputHasMultipleLinks(_ string, info os.FileInfo) (bool, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, errors.New("inspect export output links")
	}
	return stat.Nlink > 1, nil
}
