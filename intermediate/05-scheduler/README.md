# Scheduler

Deliver this message later — and the long one does not hold up the short one.

```bash
.venv/bin/python intermediate/05-scheduler/main.py
```

Takes about nine seconds, most of which is the eight-second reminder waiting.

## What to look for

**The eight-second message is scheduled first and arrives last.** That is the
whole example. The obvious way to delay a message is to set an expiration on it,
drop it in a queue nobody consumes, and let it dead-letter to its destination —
and it is wrong for anything but a single fixed delay, because **a classic queue
expires messages only at its head**. Put an eight-second message in, then a
two-second message behind it, and the two-second message is delivered in eight.
Nothing reports it: the queue looks healthy, the message is not lost, it is
simply late by a factor nobody predicted. It is the most common way a home-made
scheduler fails, and it fails in production under mixed load rather than in
testing under uniform load.

**`delivered 3 after 11 hops`.** The ladder is five queues with *uniform* times
to live — 1s, 10s, 1m, 10m, 1h — and a message hops through them until it is
due. Every message in a rung has the same delay, so the head is always the
message due soonest and head-of-line expiry stops being a problem. A four-hour
delay is four one-hour hops.

**The topology, printed.** Six queues and six bindings, all classic, all
declared with the same arguments the Java library uses. A difference would not
be a difference in behaviour; it would be a `PRECONDITION_FAILED` on whichever
service started second.

## The cost, stated honestly

A long delay is several broker round trips rather than one, and delivery is
accurate to about the smallest rung rather than to the second — which is why the
two-second reminder above arrives at about one second and the eight-second one
at about seven. A scheduler that must fire at 09:00:00.000 exactly is a
scheduler, not a message broker.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

## What is left behind

The `acemq.schedule.*` queues stay. They are shared with every other service
scheduling on the broker, Java and Go included, and `schedule_topology()` is
public so a deployment can declare them from a migration and run its services
with a login that has no `configure` permission at all.
