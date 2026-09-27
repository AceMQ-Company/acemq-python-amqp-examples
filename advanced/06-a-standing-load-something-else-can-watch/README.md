# advanced/06 — a standing load something else can watch

A load that does not finish. It publishes and consumes at a steady rate and prints
one JSON object per second saying what has happened to it, so that something outside
the process can read what the client saw rather than what the broker did.

Every other example here runs, proves a point and exits. This one is the thing a
fault drill breaks the cluster underneath.

## What it shows

- **A client reporting its own state**, in a shape another program can read: one JSON
  object per line on stdout, oldest first.
- **`blocked` left off the line entirely when it cannot be answered.** This library
  reports `Connection.blocked` as `None` when the transport cannot be asked, and that
  is a different fact from `False`. A reader that finds no `blocked` knows the client
  could not say; one that finds `false` knows it said no. Collapsing the two would let
  a transport that cannot report back-pressure look like one that never met any.
- **Publishes bounded by a timeout.** A publish on a blocked connection does not
  fail — it waits, because RabbitMQ has stopped reading the socket. Without a
  deadline the sampler would wait with it, and a client that went quiet looks exactly
  like a client that was never running.
- **Deltas, not just totals.** `publishRate` and `consumeRate` cover the last
  interval, because a total on its own cannot tell a client that is working from one
  that worked earlier and has since stopped.
- **Readings flushed as they are taken**, because a drill reads this output while the
  process is still running.

## Running it

```bash
docker compose up -d

.venv/bin/python advanced/06-a-standing-load-something-else-can-watch/main.py \
    > readings.jsonl
```

Environment: `ACEMQ_URL`, `ACEMQ_LOAD_RATE` (per second), `ACEMQ_LOAD_INTERVAL`
(seconds between readings), and `ACEMQ_EXAMPLE_SECONDS`, which defaults to **60**.

Set `ACEMQ_EXAMPLE_SECONDS=0` to run until interrupted — which is what a drill
campaign wants, and what `chaos-drill.sh workload up` passes. The default is bounded
rather than endless on purpose: CI runs every example in this repository with no
arguments and waits for each to finish, so an endless one is not a failing example,
it is a job that runs to the six-hour ceiling and is cancelled. That happened, in
three repositories at once.

## What it prints

```json
{"at": "2026-09-26T19:28:05Z", "elapsedMs": 3000, "published": 362, "confirmed": 362, "consumed": 362, "failed": 0, "blocked": false, "publishRate": 120.98, "consumeRate": 120.98}
```

Under a memory alarm the same line reads:

```json
{"at": "...", "elapsedMs": 585696, "published": 58633, "confirmed": 58611, "consumed": 58612, "failed": 21, "blocked": true, "reason": "low on memory", "publishRate": 0.0, "consumeRate": 0.0}
```

`confirmed` has stopped moving while `published` keeps climbing, `failed` counts the
sends that never completed, and `reason` is the broker's own words. None of that is
visible to anything that asks the broker how it is doing — the cluster is healthy,
and this application is not publishing.

## The fields

| Field | Is |
|---|---|
| `blocked` | the broker is refusing to read from this connection now. Absent when the transport cannot be asked |
| `published` | sends attempted since the start |
| `confirmed` | sends the broker has acknowledged |
| `consumed` | deliveries handled |
| `failed` | sends that did not complete |
| `publishRate` / `consumeRate` | confirms and deliveries per second over the last interval |
| `reason` | what the broker said when it blocked the connection |

The first seven names are a contract with whatever reads them, so they are not
renamed for tidiness: a reader looking for `confirmed` and finding `acked` sees a
client reporting nothing, which is indistinguishable from a well-behaved client on a
quiet cluster.

## On the achieved rate

Each send is awaited before the next is offered, so the rate this reaches is lower
than the rate asked for — a few hundred a second rather than thousands. That is the
right trade for what this example is for: a drill asks whether the counters moved, not
how fast, and one confirm at a time is what makes `confirmed` a statement about the
broker rather than about a buffer.

## Why a drill cannot use a probe instead

A probe that connects to the broker answers "is the cluster usable". It cannot answer
whether the application was told the broker had stopped reading from it, whether it
stopped publishing, or whether it recovered on its own — and those differ between
client libraries that are otherwise equivalent. The only thing that can report them is
a client.

## Also see

- `advanced/05-blocked-broker` — the same condition asserted rather than reported,
  including what `health()` says about it.
- `advanced/03-metrics-and-health` — these counters exposed to a scraper instead of a
  log.
