# Publish and consume

A durable queue, a confirmed publish, and a consumer that says what it did.

```bash
.venv/bin/python basic/01-publish-and-consume/main.py
```

## What to look for

**`confirmed=True` on every publish.** That is the broker saying it has the
message, not the client saying it wrote to a socket. Without confirms a publish
returns as soon as the bytes leave the process, and a broker that dies a
millisecond later loses a message the publisher believes it sent.

`routed` is the other half of the same result and a different question — whether
anything was bound to take it. A publish can be confirmed and unrouted at once,
which is what a typo in a routing key looks like.

**`attempt=1` and `origin=checkout@example` on the consumed message.** The
envelope travels with the message and is the same set of headers a Java, Go or
.NET service would have written. `origin` defaults to the process and host;
naming it is better, because a pod name is not a service name.

**`left on the queue: 0`, and then the queues are deleted.** Run it twice: the
second run reports the same numbers as the first. An example that leaves
messages behind reports different numbers every time, and the queue it left is
the one that collides with the next example that wanted the name.

## The topology

`Topology().queue(QUEUE, dead_letter=True)` declares three queues, not one:
`py-orders.placed`, `py-orders.placed.dlq` and `py-orders.placed.parked`, plus
the `acemq.dlx` exchange and the bindings that reach them. The answer to "where
does a failure go" exists before the first failure does.
