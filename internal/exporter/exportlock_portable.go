//go:build js || plan9 || wasip1

package exporter

import "os"

// ponytail: js, Plan 9, and WASI lack the file-lock primitive used elsewhere;
// the process-local registry still serializes writers in their usual runtimes.
func tryLockExportFile(*os.File) (bool, error) { return true, nil }

func unlockExportFile(*os.File) error { return nil }
