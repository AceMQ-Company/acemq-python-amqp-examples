# Graceful shutdown

Closing within a grace period — the handler in hand finished when there is time,
cut off and redelivered when there is not.

```bash
.venv/bin/python intermediate/10-graceful-shutdown/main.py
```

## What to look for

```
  enough time    drained=True took=0.50s in_flight=0 finished=['ORD-1'] cut_off=[]
                 left on the queue: [('ORD-2', True), ('ORD-3', True), ('ORD-4', False), ('ORD-5', False)]
  not enough     drained=False took=0.11s in_flight=0 finished=[] cut_off=['ORD-1', 'ORD-2', 'ORD-3']
                 left on the queue: [('ORD-1', True), ('ORD-2', True), ('ORD-3', True), ('ORD-4', False), ('ORD-5', False)]
  wait_for 0.1s  drained=False took=0.10s in_flight=0 finished=[] cut_off=['ORD-1']
                 left on the queue: [('ORD-1', True), ('ORD-2', True), ('ORD-3', True), ('ORD-4', False), ('ORD-5', False)]
```

The example checks every one of these claims and exits non-zero if the library
stops making any of them.

Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
killed, and whatever is still inside a handler dies with it. Those messages were
never acknowledged, so the broker redelivers them — correct, and the reason a
deployment shows up as a spike of duplicate work when nobody arranged otherwise.

**`close(timeout=...)` is the drain.** It cancels the subscription, gives back
what had been delivered but not started, and waits up to the timeout for the
handlers already running. ORD-1 was half done, so it is finished and
acknowledged. ORD-2 and ORD-3 were sitting in the prefetch, so they go straight
back — `True` is the broker saying it has handed them over before — rather than
being worked through first, which would make closing take as long as the
backlog.

**`drained=False` is the line worth alerting on.** `close()` returns `True` if
every running handler finished and `False` if the deadline passed first. False
says one of two things: the grace period is shorter than the slowest handler, or
a handler is stuck. Both are worth knowing before they turn into a redelivery
spike nobody can explain.

**One deadline for every handler.** The second shutdown runs `concurrency=3`
and closes the consumer with `consumer.close(timeout=0.1)`. All three handlers
are cut off at the one deadline, and closing takes a tenth of a second, not the
second each handler would have needed. On a connection, `mq.close(timeout=...)`
is likewise one deadline across every handler of every consumer.

**`cut_off`, and those messages are back on the queue.** A handler still running
at the deadline is cancelled at its next `await`. Nothing settles its message —
it is not acknowledged, rejected or dead-lettered because shutdown cut it off —
so the broker takes it back when the channel closes, and whoever starts next
does that work again.

## Bounding it

```python
drained = await mq.close(timeout=25)
if not drained:
    log.warning("shut down with work still in flight; it will be redelivered")
```

The default is `DEFAULT_DRAIN_TIMEOUT`, twenty seconds; `timeout=None` waits for
as long as the handlers take. Set it **slightly under**
`terminationGracePeriodSeconds`, so the shutdown loses the race to your own log
line rather than to SIGKILL.

An outside bound works too. The third shutdown wraps an unbounded close in
`asyncio.wait_for(mq.close(timeout=None), 0.1)`: at the deadline the handler is
cancelled, the connection released, and `wait_for` raises `TimeoutError`. Any
cancellation of the task calling `close()` behaves the same way.

## Where the bound stops holding

A handler that blocks rather than awaits — a synchronous call, a CPU-bound loop —
never reaches an `await` to be cancelled at. No bound in the event loop stops
it; only the process ending does.

## What redelivery costs you

Nothing, if the handler is idempotent. Everything, if it charges a card. This is
the same argument as [intermediate/02](../02-idempotent-consumer) — a graceful
shutdown reduces duplicates, it does not eliminate them. A power cut has no
SIGTERM.
