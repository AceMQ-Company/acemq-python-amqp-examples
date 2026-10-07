# Event-sourced ledger

The log **is** the system of record. Balances are not stored; they are what you
get by adding up the log, and can be deleted and rebuilt at any time.

```bash
.venv/bin/python apps/03-ledger/main.py
```

[apps/01](../01-order-fulfilment) and [apps/02](../02-policy-administration)
publish events describing what happened to a system of record that lives in a
database. Here there is no such database. Every entry is appended to a stream and
nothing is ever updated or deleted: money moved wrongly is corrected by posting
the opposite entry, as a paper ledger does, and both entries stay. It is the
Python port of the Java examples' `apps/03-ledger`, with the same stream, queues,
routing keys and payloads, and it has been run mixed with the Java half (below).

## Why a stream and not a queue

**A queue is emptied by being read. A stream is not.**

- the writer reads the whole journal at start-up to recompute balances;
- a statement projection reads the same journal, from the same offset, at the
  same time, and neither affects the other;
- a projection written next year starts at offset zero and gets all of history.

On a queue exactly one of those readers would get each entry — right for a
*command*, which is why transfers arrive on `ledger.commands`, and wrong for a
*fact*, which is why entries go to the `ledger.journal` stream.

| File | |
|---|---|
| `ledger.py` | The only writer. Decides whether a transfer is allowed, appends two entries that sum to zero, and rebuilds its balances from the journal on start-up |
| `projections.py` | A statement per account, built by reading from offset zero. Stores nothing the log does not contain |
| `transfers.py` | Where transfers are asked for, and refusals noticed |
| `contracts.py` | The entry and command shapes, the topology, and the journal's declaration |
| `main.py` | Starts it and checks what it claims |

## What to look for

```
double entry
  alice 7500, bob 2500
insufficient funds
  refused: insufficient funds: carol holds 1000
a projection agrees with the writer
  erin 13000, frank 7000, from the writer and from offset zero
a later projection sees everything
  heidi's entry, read by a projection started after it was written
readers do not compete
  judy's entry, seen by both readers
a restarted writer rebuilds what it had
  18 entries replayed, 11 balances identical
all 6 held
```

The first five are the Java system test's five cases with its assertions. As in
Java, each scenario starts a new writer, which rebuilds from everything the
earlier scenarios appended, so every scenario after the first is also a rebuild.

The sixth is new here. It closes the writer mid-run, starts another from nothing
but the journal, and checks every balance is identical; then posts a transfer
through the new writer and checks no account moved by more than it should. That
is the bug the Java README describes — a writer that kept following the stream
*and* applied its own entries, counting each twice — ruled out rather than
described. It also checks that an independent projection agrees with every balance
and that the money in the ledger equals the opening balances.

Any broken claim prints what was expected and what happened, and the run exits 1.

**Read to the end, then stop.** The writer's rebuild reads the journal from
offset zero until it has been quiet for 400 ms, closes the reader, and maintains
the balances itself from then on. That is safe *because* there is one writer.
The quiet period is the crude way to find the end of a stream, and Java uses the
same one; the precise way is to read the last offset first.

**Amounts are integers** — whole minor units, signed. A ledger in `float`
disagrees with itself after enough additions.

## What porting it found

**Python and Java spell the journal's retention differently, and the broker
refuses the second one.** `declare_stream(..., StreamRetention(max_age=1 hour))`
writes `x-max-age: 1h`; Java's `declareStream(name, Duration.ofHours(1), ...)`
writes `3600s`. RabbitMQ compares the two as strings, so whichever side declares
second fails with `PRECONDITION_FAILED - inequivalent arg 'x-max-age'`. Found by
the mixed run below. The libraries split three to two — Go, Python and Ruby use
the largest whole unit, Java and .NET always write seconds — so it is reported
rather than changed in one of them. `contracts.journal()` declares the journal
with Java's spelling so this app mixes; with the library's own call it would not.

**`read_stream` declares `ledger.journal.dlq` and `.parked` unless told not to.**
Not a bug, but worth knowing: the projections and the rebuild pass
`declare=False`, because a reader has no business creating queues beside a
stream it does not own.

## Mixing it with the Java half

Checked against RabbitMQ 4 with the Java app on acemq-java-amqp 0.7.11 and this
library at 0.7.8, sharing one vhost and one journal:

- Java's writer funded alice, moved 2,500 to bob and refused an overdraft.
- A Python projection read Java's entries from offset zero and agreed. A Python
  writer rebuilt alice 7,500 and bob 2,500 from them, then applied a Python
  transfer of 500.
- A new Java writer rebuilt alice 8,000 and bob 2,000 from the mixed journal, a
  Java projection agreed, and a transfer requested by Java's gateway was applied
  by the Python writer and seen by the Java projection.

## Running it beside other things

The run deletes the journal and both queues (with their `.dlq` and `.parked`)
when it starts and when it finishes. A second run against a journal the first
one left would find alice already holding 7,500 and fail on the first claim —
which is the ledger being right. Give it a vhost of its own on a shared broker:

```bash
ACEMQ_URL=amqp://guest:guest@localhost:5672/py-ledger .venv/bin/python apps/03-ledger/main.py
```

Streams need no plugin: `x-queue-type: stream` is core since RabbitMQ 3.9 and
reachable over AMQP 0-9-1, which is what this uses.

## What is honestly not here

- **Snapshots.** A rebuild is O(history). The answer is "the balance at offset N,
  plus everything after N", and it is the second thing to build.
- **Atomic double entry.** The two halves of a transfer are appended one after
  the other. A crash between them would leave the journal unbalanced; a real
  ledger appends them as one record.
- **Retention.** The journal keeps an hour. If retention is shorter than
  "forever", the projection is the system of record after all.

## Related

- [basic/06](../../basic/06-streams) — offsets and replay, one idea at a time
- [apps/02](../02-policy-administration) — events about a system of record, rather than the record itself
