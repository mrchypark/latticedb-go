//go:build !windows

package exporter

import "os"

func replaceOutput(source, target string) error { return os.Rename(source, target) }
