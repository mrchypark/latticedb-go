# Stream notifications

`ReadStreamContext` captures the immutable graph generation and requested
stream next sequence under `db.mu`, then reads without holding the database
lock. If that stream has no result, it rechecks the requested stream's next
sequence under `db.mu` and joins a lazily created subscription before waiting.
Unrelated commits therefore cannot force retries, while a publication between
the read and subscription check is retried and cannot be missed. Trimming
does not introduce records, but it still wakes existing waiters. Offset-only
commits do not wake stream readers.

Legacy timed reads start their timer before entering the retry loop and check
expiration before each retry, so unrelated commits cannot extend the timeout.

Subscriptions contain a waiter count and are removed when the last canceled,
timed-out, or completed reader leaves. Database close removes and closes every
subscription, waking all readers with `ErrDatabaseClosed`.

The notification fanout benchmark prestarts 128 readers across 16 streams with
eight consumers per stream. It compares the target stream's eight-consumer
fanout with a 128-consumer global-channel baseline. Ready barriers arm all
readers before timing; each timed iteration closes the notification and waits
for acknowledgements. Rearming happens outside the timed interval, and no
goroutines are created in the loop:

```sh
go test ./internal/engine -run '^$' -bench BenchmarkStreamNotificationFanout -benchmem -benchtime=100x -count=3 -timeout=60s
```

On an Apple M3, three 100-iteration samples produced these medians:

| Case | Median |
| --- | ---: |
| Per-stream, 8 target readers | 2,926 ns/op |
| Global baseline, 128 readers | 30,686 ns/op |

The timed result measures notification-to-acknowledgement wakeup processing;
registry rearming, stream reads, payload copies, and WAL work are excluded.
The global baseline is the existing one-channel fanout shape with the same
128 prestarted readers, so the comparison intentionally measures the cost of
waking 8 versus 128 readers. Results depend on the machine, scheduler, and Go
runtime.
