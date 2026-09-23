# A blocked broker

A real memory alarm, a connection RabbitMQ has stopped reading, and the health
report that comes back in microseconds saying so.

This example has a broker to itself, because the alarm it provokes is
broker-wide:

```bash
docker compose up -d
.venv/bin/python advanced/05-blocked-broker/main.py
```

`ACEMQ_BLOCKED_URL` points it somewhere else, and `ACEMQ_BLOCKED_CONTAINER`
names the container the alarm is raised in — it reaches the broker with
`rabbitmqctl`, because nothing an AMQP client sends can raise or clear an alarm.

## What it does

`rabbitmqctl set_vm_memory_high_watermark 0` puts the broker over its own limit
immediately, and publishes on this connection then make the block land. RabbitMQ
blocks connections that publish and leaves connections that only consume alone,
which is why the example publishes once before the alarm as well: a connection
that had never published would sit through the whole thing correctly reporting
itself unblocked.

The watermark is read off the broker first and put back in a `finally`. Reading
it matters — RabbitMQ 3.13 defaults to `0.4` and 4.x to `0.6`, and CI runs every
example against both, so a number written into this file would be wrong on half
the runs. The `finally` matters because every check below is a way to leave with
the alarm still on, and the next thing to use this broker would find it blocked
for a reason nothing in its own output explains.

## What to look for

**`while blocked: up`** — not `degraded`. This is the part worth reading twice,
because it is a change of behaviour rather than a new feature: the same
connection reported `degraded` up to 0.6.0.

A blocked connection is the broker protecting itself from a producer that is
doing nothing wrong. An application that fails its own readiness check for it is
one an orchestrator restarts into the same blocked broker, having thrown away
whatever it was holding — and doing that to every replica at once turns a broker
under memory pressure into an outage with a crash loop on top. The state still
has to be *visible*, so it is a detail on an `up` report: `parts["blocked"]` is
`True`, a dashboard shows it, an alert rule matches it, and nothing is taken out
of rotation for it.

**The detail is a fixed sentence.** `the broker has blocked this connection;
publishing is paused` is the same wording in Java, Go, Ruby and .NET, so one
alert rule reads a blocked broker whatever the service happens to be written in.
It is a contract, not a phrasing, which is why the check at the bottom of the
file compares against the library's own `BLOCKED_DETAIL` rather than a copy.

**`up in 33us`, against `up in 2242us` before the alarm.** The ordinary health
check proves the connection by asking the broker something, and the round trip
shows up in `parts["round-trip"]`. A blocked broker will not answer that, so an
unbounded check would spend its full three-second deadline arriving at `down`
for a broker that is up and talking. The block is read first instead, off the
notification the broker already sent, and the probe is skipped: no `round-trip`
in the parts, because there was not one. The check at the bottom is the one that
would catch that regression — it fails if the blocked report carries a
`round-trip`, or if it took longer than a quarter of a second.

**`1 of 1 stalled publishes went through`.** The publishes made during the alarm
never failed and were never lost. They waited, and were confirmed once the
broker started reading the socket again. That is the behaviour the `up` report
is protecting: there was something in flight worth not throwing away.

## Where Python differs from the rest of the family

`the broker's reason, as this library can see it: None`.

RabbitMQ sends its reason — `low on memory` — and aio-pika reads it, logs it
(`was blocked by: 'low on memory'` in the run above is aio-pika's line, not this
example's) and then keeps no accessor for it. So `Connection.blocked_reason` is
`None` on RabbitMQ and the detail is the shared sentence with nothing appended.

That is the honest half rather than a gap papered over: a reason invented here
would read exactly like one the broker sent. Java, Go, Ruby and .NET all append
the broker's own words after a colon, and the same release note reads slightly
differently in Python because of it. `parts["blocked"]` — the thing an alert
matches on — is the same everywhere.
