//go:build js || plan9 || wasip1

package engine

import "os"

// ponytail: js, Plan 9, and WASI use the process-local registry because the
// shared file-lock primitive used by the other targets is unavailable.
func tryLockFile(*os.File, bool) error { return nil }

func unlockFile(*os.File) error { return nil }
