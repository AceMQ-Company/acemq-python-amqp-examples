# Metrics and health

`/acemq-metrics`, `/acemq-health` and `/acemq-info`, on the same paths as Java,
Go and .NET.

```bash
.venv/bin/python advanced/03-metrics-and-health/main.py
```

It binds `127.0.0.1:9464`. Set `ACEMQ_HTTP_PORT` if that is taken.

## What to look for

**The counters, with the same names in every language**, so a dashboard built
against Java reads against Python. Ten consumed, nine accepted, one dead
lettered, ten published — and the labels (`queue`, `exchange`, `key`) are the
same too.

**`acemq.handler.duration: 10 samples, 0ms fastest, 10ms mean, 22ms slowest`.**
A distribution rather than an average, because the handler that takes thirty
seconds once an hour is invisible in a mean.

**`aggregate: up {'broker': 'up', 'projections': 'up'}`.** The application's own
checks and the broker's, worst wins, run at once under a deadline — a probe that
hangs is a pod that never comes back.

**Three endpoints, answered.** The library does not ship an HTTP server, because
a library that opened a port would be a library that opened a port in every
process that imported it. Wiring one up is the twenty lines at the bottom of the
file, and the paths are the ones the other three libraries use so that one
scrape configuration reads all four.

`/acemq-health` is served from a thread and hands its work back to the event
loop, which is the shape any synchronous web framework will need. Making that
request *from* the loop is a deadlock, which is why the example fetches on a
worker thread.

**`prometheus_text with nothing installed: 7 sample lines`.** The same numbers,
rendered as a scrape body by the library itself. `Metrics` and
`prometheus_text` need nothing installed at all; `PrometheusObserver` writes
into a registry the rest of the application already exports. Taking a hard
dependency on a metrics client would put every user of the package on whichever
one was picked.

The `Both` class in this file is the evidence that the seam is real: an
`Observer` is three methods, so an application can have two of them, and nothing
the library reports goes anywhere else.

## The one to alert on

`acemq.retry.rung.missing`. It means a long retry had to wait in the consumer
because its rung queue is not on the broker. The message is still retried and
the wait still happens, so every dashboard reads as normal — while the reason
the rung exists is gone, and a consumer restart mid-wait shortens a five-minute
backoff to nothing.

## Down, and degraded

They are different answers. A consumer whose workers have died without it being
closed is one the broker is still sending messages to and nothing is reading —
indistinguishable from a quiet queue from outside, and reported as **degraded**:
the connection works, and a replacement instance would almost certainly stall
the same way, so it is worth an alert and not worth taking out of rotation.
`report.healthy` is what a readiness probe should return, and degraded passes it.
