# Streams

Six readings written once and read three times, from three different places.

```bash
.venv/bin/python basic/06-streams/main.py
```

## What to look for

**`from_first()` reads the history.** A queue hands a message to one consumer
and forgets it. A stream keeps it, so a projection that has to be built from
nothing can be built — which is the reason to declare one rather than a queue.
Note *the history it still holds*: retention has already discarded whatever it
was told to, so `from_first()` is the oldest surviving message and not the first
one ever written.

**`from_offset(3)` starts at the fourth message.** Offsets count from zero and
are stable: message four is message four for ever, for every consumer. That is
what a consumer recording its own progress writes down, and what it hands back
to carry on after a restart. `from_timestamp(...)` answers the same question
with a clock instead — "everything since the incident started".

**`from_next()` reads nothing that was already there.** The default, and what
most consumers want. It sits for a second while six messages it will never see
sit in front of it, then reports the seventh the moment it is published.

**`from_first() again` finds all seven.** Three consumers acknowledged every
message they read before this one started. Acknowledging a stream message
advances *that consumer's* position; it removes nothing, and there is nothing to
remove it from. This is the assertion the example would fail on if streams
behaved like queues.

**`message_count` is not the question to ask.** RabbitMQ answers a passive
declare on a stream with zero however much it is holding, because a stream has
no single "how many are left" — it has a position per consumer. Reading it again
is the only honest way to ask what it still has, which is what the fourth reader
is doing.

## Retention, and why `segment_bytes` is in the example

A queue's messages leave when somebody handles them. A stream's leave when the
retention policy says so, and a stream declared without one grows until the disk
is full — a mistake an ordinary queue cannot make, so it is worth making the
policy the visible part of the declaration:

```python
StreamRetention(
    max_age=timedelta(hours=1),
    max_bytes=20 * 1024 * 1024,
    segment_bytes=1024 * 1024,
)
```

`segment_bytes` is `x-stream-max-segment-size-bytes`, and it is not a tuning
knob to leave alone. A stream is stored as a series of files and retention
discards a whole file at a time, so nothing at all is discarded until a whole
segment can be. With RabbitMQ's 500 MB default, a stream whose `max_age` is an
hour keeps everything until it has half a gigabyte to drop.

## Failures are the handler's problem

There is no retry policy here on purpose, and `read_stream` will not take one.
Retrying on a stream means *republishing* to it, which appends a second copy for
every other consumer to read as well — a projection reading that stream would
count the message twice. Rejecting is no better: there is no dead-letter hop,
because there is nothing to remove the message from. A stream message that
cannot be handled has to be logged, counted or copied somewhere by the handler
itself, and the stream moves on regardless. That is the trade: nothing is lost,
and nothing is retried for you.

## A stream is declared, not converted

`x-queue-type` is part of a queue's identity to the broker, so a name that
already exists as a quorum queue cannot be re-declared as a stream — the answer
is `PRECONDITION_FAILED`, and it does not mention streams. This example deletes
the name before declaring it for that reason, and picks one nothing else on the
broker uses.
