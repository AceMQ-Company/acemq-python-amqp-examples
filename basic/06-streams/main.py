"""Six readings written once and read three times, from three different places.

A queue forgets a message the moment somebody acknowledges it. A stream does
not: acknowledging moves *this consumer's* position and nothing else, so the
messages are still there for the next reader, and for the one after that. That
is the whole difference, and everything else here follows from it — the offset a
consumer starts at, and the retention policy that is now the only thing deciding
when a message goes away.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from acemq_amqp import Ack, Envelope, Message, accept, connect
from acemq_amqp.patterns import (
    StreamRetention,
    declare_stream,
    from_first,
    from_next,
    from_offset,
    read_stream,
)

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

STREAM = "py-stream-sensor-readings"

READINGS = [
    {"sensor": "roof-1", "celsius": 19.0},
    {"sensor": "roof-1", "celsius": 19.5},
    {"sensor": "roof-2", "celsius": 21.0},
    {"sensor": "roof-2", "celsius": 21.5},
    {"sensor": "roof-3", "celsius": 17.0},
    {"sensor": "roof-3", "celsius": 17.5},
]


async def collect(read: list[dict], how_many: int, within: float = 15.0) -> None:
    """Waits for a reader to have seen what it was supposed to see.

    A deadline rather than a sleep: a stream that stopped delivering should fail
    the example rather than pass it slowly.
    """
    deadline = asyncio.get_running_loop().time() + within
    while len(read) < how_many and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)


async def main() -> None:
    async with await connect(URL) as mq:
        # A stream is declared with arguments that are part of its identity, so
        # a leftover queue of another shape is a PRECONDITION_FAILED rather than
        # a reuse. This broker is shared with four other languages' examples.
        if await mq.queue_exists(STREAM):
            await mq.delete_queue(STREAM)

        # Retention is not optional in the way it looks. A queue's messages
        # leave when they are handled; a stream's leave when the policy says so,
        # and a stream with no policy grows until the disk is full.
        #
        # `segment_bytes` is how large each file on disk gets, and it is here
        # because retention happens a whole segment at a time: with one enormous
        # segment nothing is ever discarded, whatever `max_age` says.
        await declare_stream(
            mq,
            STREAM,
            StreamRetention(
                max_age=timedelta(hours=1),
                max_bytes=20 * 1024 * 1024,
                segment_bytes=1024 * 1024,
            ),
        )

        publisher = mq.publisher(routing_key=STREAM)
        for reading in READINGS:
            await publisher.send(reading, envelope=Envelope(type="ReadingTaken"))
        print(f"published {len(READINGS)} readings to {STREAM}")

        # ------------------------------------------------------------------
        # A projection being built from nothing, which is the reason to reach
        # for a stream at all: the history is still there to be read.
        projection: list[float] = []

        async def project(message: Message) -> Ack:
            projection.append(message.payload["celsius"])
            return accept()

        # `declare=False` because the stream is already declared above, and a
        # reader that declares is a reader that can disagree about the shape of
        # something it did not create.
        first = await read_stream(
            mq, STREAM, project, offset=from_first(), consumer_name="py-projection",
            declare=False,
        )
        await collect(projection, len(READINGS))
        await first.close()
        print(f"from_first():     {projection}")

        # ------------------------------------------------------------------
        # A consumer that recorded its own progress and is carrying on. Offsets
        # count from zero, so three means the fourth message.
        resumed: list[float] = []

        async def carry_on(message: Message) -> Ack:
            resumed.append(message.payload["celsius"])
            return accept()

        second = await read_stream(
            mq, STREAM, carry_on, offset=from_offset(3), consumer_name="py-resumed",
            declare=False,
        )
        await collect(resumed, 3)
        await second.close()
        print(f"from_offset(3):   {resumed}")

        # ------------------------------------------------------------------
        # And the ordinary case: everything already written is somebody else's
        # problem, this one wants what happens from now on.
        tail: list[float] = []

        async def follow(message: Message) -> Ack:
            tail.append(message.payload["celsius"])
            return accept()

        third = await read_stream(
            mq, STREAM, follow, offset=from_next(), consumer_name="py-tail",
            declare=False,
        )
        # Long enough that a from_next() consumer wrongly reading the history
        # would have shown it before the new reading is published.
        await asyncio.sleep(1.0)
        await publisher.send({"sensor": "roof-4", "celsius": 25.0})
        await collect(tail, 1)
        await third.close()
        print(f"from_next():      {tail}")

        # ------------------------------------------------------------------
        # And the proof that none of that removed anything. Three consumers
        # have now acknowledged every message they read; a fourth starting at
        # the beginning still finds all of them, the new one included.
        #
        # `message_count` is no use here and is worth knowing about: RabbitMQ
        # answers a passive declare on a stream with zero however much it holds,
        # because a stream has no single "how many are left" — it has a position
        # per consumer. Reading it again is the only honest way to ask.
        again: list[float] = []

        async def reread(message: Message) -> Ack:
            again.append(message.payload["celsius"])
            return accept()

        fourth = await read_stream(
            mq, STREAM, reread, offset=from_first(), consumer_name="py-reread",
            declare=False,
        )
        await collect(again, len(READINGS) + 1)
        await fourth.close()
        print(f"from_first() again: {again}")

        await mq.delete_queue(STREAM)

    expected = [reading["celsius"] for reading in READINGS]
    if projection != expected:
        raise SystemExit(f"from_first() did not read the stream in order: {projection}")
    if resumed != expected[3:]:
        raise SystemExit(f"from_offset(3) started somewhere else: {resumed}")
    if tail != [25.0]:
        raise SystemExit(f"from_next() did not start at the next message: {tail}")
    # The point of the whole thing. Three consumers acknowledged every message
    # they read, and the fourth still finds all seven — acknowledging a stream
    # message moves that consumer's position and removes nothing.
    if again != [*expected, 25.0]:
        raise SystemExit(
            f"the stream did not survive being read: {again} after three readers "
            f"acknowledged everything"
        )


if __name__ == "__main__":
    asyncio.run(main())
