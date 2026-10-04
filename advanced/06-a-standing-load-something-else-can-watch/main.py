"""A load that keeps running and says what is happening to it, one line at a time.

Every other example here finishes. This one does not: it publishes and consumes at
a steady rate and prints one JSON object per second describing what it has seen.
That makes it the thing a fault drill breaks the cluster underneath -- the drill
kills a node or raises a memory alarm, reads these lines, and judges what the
client did about it.

Why a drill needs this rather than a probe of its own
----------------------------------------------------
A probe that connects to the broker can answer "is the cluster usable". It cannot
answer what an application saw: whether it was told the broker had stopped reading
from it, whether it stopped publishing, whether it started again on its own or sat
there. Those are properties of a client library, they differ between libraries
that are otherwise equivalent, and the only thing that can report them is a
client.

What a line contains
--------------------
One JSON object per line, oldest first, on stdout:

    blocked      the broker is refusing to read from this connection now
    published    sends attempted since the start
    confirmed    sends the broker has acknowledged
    consumed     deliveries handled
    refused      sends the library declined because the broker had blocked the
                 connection: nothing was sent, so nothing was lost
    failed       sends that failed, and may or may not have arrived
    publishRate  confirms per second over the last interval
    consumeRate  deliveries per second over the last interval

`blocked` is omitted from a line when the connection cannot be asked -- which this
library reports as ``None`` rather than as ``False``, and which a reader must not
mistake for a connection that answered and said no. A line that carries no
`blocked` is a client that could not say; a line carrying ``false`` is a client
saying it is not blocked. Collapsing the two would let a transport that cannot
report back-pressure look like one that never experienced any.

Running it
----------
    python advanced/06-a-standing-load-something-else-can-watch/main.py > readings.jsonl

Then read the last few lines at any point to see what the client is seeing. Under a
fault drill that file is the client's testimony, and `tail -n 60` on it is how the
drill asks.

It stops on Ctrl-C, or after ACEMQ_EXAMPLE_SECONDS seconds, which defaults to 60.
Set ACEMQ_EXAMPLE_SECONDS=0 to run until interrupted -- what a drill campaign wants.
The default is bounded rather than endless because CI runs every example here with no
arguments and waits: an endless one is not a failing example, it is a job that runs to
the six-hour ceiling and is cancelled.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import time

from acemq_amqp import Ack, Message, PublishingPausedError, Topology, accept, connect

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

# Quorum, because a drill's faults are about losing a node. A classic queue lives
# on one node, and when that node is the one the drill stops, the load stops with
# it -- which reports a client that gave up when the truth is that the queue went
# away.
QUEUE = "py-standing-load.orders"

RATE = int(os.environ.get("ACEMQ_LOAD_RATE", "200"))
INTERVAL = float(os.environ.get("ACEMQ_LOAD_INTERVAL", "1"))

# How long to run when nobody said.
#
# A minute, not for ever, and that default is deliberate. CI runs every example in
# this repository with no arguments and waits for each to finish, so an unbounded
# default is not a failing example -- it is a job that runs to the six-hour ceiling
# and is then cancelled. That happened, in three repositories at once, and cost
# about eighteen hours of runner time before anybody looked.
#
# So a forgotten setting gives a short run, and "until interrupted" has to be asked
# for: ACEMQ_EXAMPLE_SECONDS=0, which is what a drill campaign passes.
DEFAULT_RUN_FOR = 60


def run_for_seconds() -> int:
    """Seconds to run, where 0 means until interrupted."""
    raw = os.environ.get("ACEMQ_EXAMPLE_SECONDS", "")
    if not raw:
        return DEFAULT_RUN_FOR
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_RUN_FOR


class Counters:
    """What the publisher and the consumer have done so far.

    Plain integers under a single-threaded event loop: every one of them is
    incremented from one coroutine and read from another, and asyncio gives no
    opportunity for a torn read between the two.
    """

    def __init__(self) -> None:
        self.published = 0
        self.confirmed = 0
        self.consumed = 0
        self.refused = 0
        self.failed = 0


async def publish(mq, counters: Counters, stop: asyncio.Event) -> None:
    """Offers messages at a steady rate and records what became of each one."""
    publisher = mq.publisher(routing_key=QUEUE)
    interval = 1 / max(RATE, 1)
    number = 0

    while not stop.is_set():
        number += 1
        counters.published += 1
        try:
            # Bounded, because the point of a reading is that it arrives. A
            # publish made while the broker is blocking the connection is refused
            # at once, but one already written when the block arrives waits for
            # the broker to read again, and without a timeout the sampler's next
            # line would wait with it. A client that went quiet then looks
            # exactly like a client that was never running.
            await asyncio.wait_for(
                publisher.send({"order": f"o-{number}"}),
                timeout=5,
            )
            counters.confirmed += 1
        except PublishingPausedError:
            # Declined before anything was written, so this is back pressure
            # rather than a loss -- the distinction Go, .NET and Java loads
            # already draw. Before 0.7.4 these were counted as failed.
            counters.refused += 1
        except asyncio.TimeoutError:
            # Expected while the broker is blocking this connection, and counted
            # rather than hidden: a send that never completed is a fact about the
            # run, and the `blocked` field on the same line is what says why.
            counters.failed += 1
        except asyncio.CancelledError:
            raise
        except Exception:  # a standing load reports what happened to it, it does not stop
            counters.failed += 1

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)


async def sample(mq, counters: Counters, stop: asyncio.Event) -> None:
    """Prints one reading per interval until asked to stop."""
    started = time.monotonic()
    last = started
    last_confirmed = 0
    last_consumed = 0

    def emit() -> None:
        nonlocal last, last_confirmed, last_consumed
        now = time.monotonic()
        elapsed = now - last

        reading: dict[str, object] = {
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsedMs": int((now - started) * 1000),
            "published": counters.published,
            "confirmed": counters.confirmed,
            "consumed": counters.consumed,
            "refused": counters.refused,
            "failed": counters.failed,
        }

        # None means the transport could not be asked, which is a different fact
        # from False and is reported as one: by leaving the field off the line
        # entirely rather than by guessing. A reader that finds no `blocked` knows
        # the client could not say; one that finds `false` knows it said no.
        blocked = mq.blocked
        if blocked is not None:
            reading["blocked"] = blocked
        reason = mq.blocked_reason
        if reason:
            reading["reason"] = reason

        if elapsed > 0:
            reading["publishRate"] = (counters.confirmed - last_confirmed) / elapsed
            reading["consumeRate"] = (counters.consumed - last_consumed) / elapsed

        last, last_confirmed, last_consumed = now, counters.confirmed, counters.consumed

        # Flushed, because stdout to a file is block-buffered and a drill reads
        # this file while the process is still running. Without the flush the last
        # readings sit in a buffer for minutes and the drill reports a client that
        # went quiet.
        print(json.dumps(reading), flush=True)

    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=INTERVAL)
        emit()


async def main() -> None:
    # Logs go to stderr so that stdout carries nothing but readings. A reader skips
    # whatever is not a JSON object, so mixing them would work -- and it would also
    # mean every diagnostic line here had to stay un-JSON-like for ever, which is
    # not a property anybody would remember to preserve.
    print(f"standing load: {URL}, {QUEUE} at {RATE}/s", file=sys.stderr)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    seconds = run_for_seconds()
    if seconds > 0:
        loop.call_later(seconds, stop.set)

    async with await connect(URL, origin="examples@06-a-standing-load") as mq:
        await mq.declare(Topology().queue(QUEUE, quorum=True, dead_letter=True))

        counters = Counters()

        async def handle(_: Message) -> Ack:
            counters.consumed += 1
            return accept()

        consumer = await mq.consume(QUEUE, handle)
        publishing = asyncio.create_task(publish(mq, counters, stop))
        try:
            await sample(mq, counters, stop)
        finally:
            publishing.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await publishing
            await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
