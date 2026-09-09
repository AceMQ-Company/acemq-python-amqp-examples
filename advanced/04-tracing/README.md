# Tracing

A consumer's span joined to the publish that caused it, minutes and processes
apart.

> `acemq_amqp.tracing` needs the `[opentelemetry]` extra plus an SDK, both of
> which `requirements.txt` installs.

```bash
.venv/bin/python advanced/04-tracing/main.py
```

## What to look for

**`parent=py-tracing.orders publish` on every `process` span.** This is the
example. A consumer's span is a child of the publish that caused it, taken from
the message's own headers rather than from whatever context happened to be
current when the delivery arrived. Those are different processes and often
minutes apart, and joining them is the one thing a messaging system needs from
tracing that an HTTP client does not.

**`5 spans across 3 traces`.** Each publish starts a trace and its consume joins
it. Nothing here is correlating by hand.

**`the delivered message carried traceparent: True`.** The context travels in
`traceparent` and `tracestate` — deliberately **not** `x-acemq-` prefixed,
unlike every other header this library writes, because they are the W3C names
every other piece of tracing tooling already reads. Java, Go and .NET write the
same two.

**`status=ERROR` on the unroutable publish and `status=UNSET` on the retry.**
`unroutable`, `failed` and `dead_lettered` set the span status to `ERROR`; the
others, `retried` included, do not. A retry is the system working, and a wall of
red traces that turned out fine is how people learn to ignore the colour.

**Span kinds.** `publish` is `PRODUCER`, `process` is `CONSUMER`, and a
request-reply `request` span is `CLIENT` — because that one *waits*, so its
duration measures a responder rather than a broker.

## Nothing is exported without an SDK

The library depends on `opentelemetry-api`, not the SDK. That is the package a
library is supposed to depend on: it is inert until an application installs an
SDK and configures it, so importing the adapter cannot start exporting anything
an application did not ask for. This example is that application — the exporter
is in memory, so the spans can be printed rather than shipped somewhere.

`OpenTelemetryTracing().install(mq)` with no arguments uses whatever provider
the application has already configured globally, which is what a service does.
