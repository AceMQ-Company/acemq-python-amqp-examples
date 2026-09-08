# Replay

Dead-lettered invoices put back one tenant at a time, and the rest afterwards.

```bash
.venv/bin/python basic/04-replay/main.py
```

## What to look for

**`acme: moved 3, skipped 2, stopped because drained`.** A replay is not "put it
all back". The tenant on the phone goes first, and the two nobody has finished
investigating stay exactly where they are — on the dead-letter queue, in the
order they arrived. `skipped` is what makes running a filtered replay safe to do
at all, and `reason` says whether it stopped because it ran out of messages, hit
its `limit` or ran out of `deadline`.

**The filter is handed `(envelope, body)`, not a decoded payload.** A
dead-letter queue is exactly where a message nothing could decode ends up, so a
replay that insisted on decoding would fail on the messages it is most needed
for. Filtering on the envelope alone — `envelope.error`, `envelope.type`,
`envelope.first_seen` — needs no decoding at all.

**`the rest: moved 2`.** The second replay has no filter and takes what is left.
Together the two runs move five and lose none.

**`dead letters left: 0`.** Replay reads with a manual acknowledgement and
acknowledges only after the republish is confirmed, so a crash mid-replay leaves
a message on the dead-letter queue rather than nowhere.

## Why this is a pattern rather than a script

A replay written by hand usually consumes the dead-letter queue in a loop and
stops at the first message its filter declines — because RabbitMQ returns a
declined message to the *head* of the queue, the loop sees it again, decides the
queue is drained, and leaves everything behind it. Five messages filtered to
three then move one. That bug was found by the Go version of this example and
fixed in the Go library; the same shape is in every language's version for a
reason.
