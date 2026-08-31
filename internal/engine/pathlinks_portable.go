//go:build js || plan9 || wasip1

package engine

import "os"

func regularFileHasMultipleLinks(string, os.FileInfo) (bool, error) { return false, nil }
