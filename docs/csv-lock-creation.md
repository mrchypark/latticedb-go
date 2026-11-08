# CSV lock creation on macOS

Issue #254 reproduced on darwin/arm64 with Go 1.27.1. The existing subprocess
publication and owner-death test failed 4 times in 200 runs. All failures were
`openat graph.csv.lock: no such file or directory`.

Failure-only instrumentation in `acquireExportLockContext` established that the
error came from `os.Root.OpenFile` with `O_CREATE|O_RDWR`. Immediately after
failure, the opened root and its pathname had the same device and inode, both
stat calls succeeded, and the directory contained `.ready` and `graph.csv.lock`.
A second open succeeded. This excludes a missing parent in those observations.
The temporary diagnostic retry still returned the original failure and was removed.

The fix separates exclusive creation (`O_CREATE|O_EXCL|O_RDWR`, mode 0600) from
opening an existing file (`O_RDWR`, only after `ErrExist`). It does not retry
arbitrary errors or recreate a disappearing namespace. The existing root identity,
file type, process registry, cancellation, fcntl lock and owner-death checks remain.
The application never unlinks these lock files during normal operation.

An independent implementation reports the same macOS primitive failure:
[Entire CLI diagnostic commit](https://github.com/entireio/cli/commit/fb597b386a1fa2b20adc90f4603c99ba7ccb2edf).
The local diagnosis and implementation do not depend on copying that implementation.

Subprocess test cleanup now kills and reaps started children on every failure
path. Previously, the first helper failure could leave siblings running while
TempDir cleanup removed their shared directory. That cleanup cascade did not
explain the instrumented first failure.
