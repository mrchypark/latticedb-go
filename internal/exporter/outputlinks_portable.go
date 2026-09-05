//go:build js || plan9 || wasip1

package exporter

import "os"

func exportOutputHasMultipleLinks(string, os.FileInfo) (bool, error) { return false, nil }
