# Graceful shutdown

Closing within a grace period — the handler in hand finished when there is time,
cut off and redelivered when there is not.

```bash
.venv/bin/python intermediate/10-graceful-shutdown/main.py
```

## What to look for

```
  enough time drained=True in_flight=0 finished=['ORD-1'] cut_off=[]
              left on the queue: [('ORD-2', True), ('ORD-3', True), ('ORD-4', False), ('ORD-5', False)]
  not enough  drained=False in_flight=0 finished=[] cut_off=['ORD-1']
              left on the queue: [('ORD-1', True), ('ORD-2', True), ('ORD-3', True), ('ORD-4', False), ('ORD-5', False)]
```

Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
killed, and whatever is still inside a handler dies with it. Those messages were
never acknowledged, so the broker redelivers them — correct, and the reason a
deployment shows up as a spike of duplicate work when nobody arranged otherwise.

**`close()` is the drain.** It cancels the subscription, gives back what had been
delivered but not started, and waits for the handlers already running. ORD-1 was
half done, so it is finished and acknowledged. ORD-2 and ORD-3 were sitting in
the prefetch, so they go straight back — `True` is the broker saying it has
handed them over before — rather than being worked through first, which would
make closing take as long as the backlog.

**`drained=False` is the line worth alerting on.** It says one of two things: the
grace period is shorter than the slowest handler, or a handler is stuck. Both are
worth knowing before they turn into a redelivery spike nobody can explain.

**`cut_off=['ORD-1']`, and ORD-1 is back on the queue.** A handler that outlives
the bound is cancelled at its next `await`. Nothing settles its message, so the
broker takes it back when the channel closes, and whoever starts next does that
work again.

## The bound is yours to put on

`close()` takes no timeout; it waits for as long as the handlers take. The
obvious way to bound it is the wrong one:

```python
await asyncio.wait_for(mq.close(), timeout=25)   # reports success either way
```

`close()` swallows the cancellation the timeout sends it — deliberately, so that
a cancelled worker does not make closing look like the caller was cancelled — and
carries on releasing the connection. `wait_for` then sees a normal return, raises
nothing, and a shutdown that abandoned a message reads as a clean one. Read the
answer off the task instead:

```python
closing = asyncio.create_task(mq.close())
done, _ = await asyncio.wait({closing}, timeout=25)
if closing not in done:
    log.warning("shut down with work still in flight; it will be redelivered")
    closing.cancel()
    await asyncio.wait({closing})
```

Set the timeout **slightly under** `terminationGracePeriodSeconds`, so the
shutdown loses the race to your own log line rather than to SIGKILL.

## Where the bound stops holding

The cancellation reaches **one handler**: the one `close()` happens to be waiting
for. This example runs the default `concurrency=1`, so that is all of them. With
`concurrency=3`, a 0.2-second bound against 2-second handlers cuts one off and
then waits for the other two to finish however long they take — unbounded again.
For more than one handler, let the process exit when the clock runs out:
`asyncio.run` cancels every task still running as it returns, and the broker
takes back whatever was unsettled when the socket closes.

A handler that blocks rather than awaits — a synchronous call, a CPU-bound loop —
never reaches an `await` to be cancelled at. No bound in the event loop stops
it; only the process ending does.

## What redelivery costs you

Nothing, if the handler is idempotent. Everything, if it charges a card. This is
the same argument as [intermediate/02](../02-idempotent-consumer) — a graceful
shutdown reduces duplicates, it does not eliminate them. A power cut has no
SIGTERM.
