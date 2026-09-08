# Interceptors

A tenant on every message and every handler timed, without either appearing in a
handler.

```bash
.venv/bin/python intermediate/04-interceptors/main.py
```

## What to look for

**`the handler saw tenant='acme'`, and nothing in the handler mentions a
tenant.** That is the measure of whether the seam is worth having. Without it,
the tenant gets copied into every call site, one of them is eventually
forgotten, and nobody finds out until the message that needed it is the one that
went without.

**`the order in and out: ['stamp in', 'check in', 'check out', 'stamp out']`.**
Interceptors run in the order they were registered, first registered outermost:
it sees the message first on the way in and last on the way out. That matters as
soon as one reads what another wrote — the check here only works because the
stamp ran before it.

**`refused before it reached the broker`.** Refusing is raising. A publish
interceptor that raises stops the publish and the caller sees the exception,
which is the whole point of intercepting rather than observing. A consume
interceptor that raises is treated exactly as a handler that raised: retried,
then dead-lettered with the reason it gave — the alternative is acknowledging a
message nothing processed.

**`reserved: these header names belong to AceMQ`.** A reserved `x-acemq-` name is
refused rather than dropped. Silently discarding a header somebody set is worse
than saying no, and an application header that could impersonate an envelope
field is a way to forge one.

## The shape

One function handed the message and the rest of the work, rather than the pair
of before-and-after hooks the Java library uses — because Python already has the
construct. `try`/`finally` runs the way out in the reverse of the way in, nests
correctly without anybody reversing a list, and makes an interceptor that opens
something and closes it **one** function instead of two halves that have to
agree. It composes the way ASGI middleware does.

`PublishContext` is seen **before the codec runs**, so an interceptor can change
the payload and not only its metadata. Whatever the interceptors left is what
gets used — an envelope rewritten on the way in is the one the handler is given
*and* the one that gets dead-lettered, so an operator reading the dead-letter
queue is not missing the very field the interceptor exists to add.
