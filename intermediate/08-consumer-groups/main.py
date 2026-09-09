"""Four slow invoices, handled four times faster by four consumers than by one.

`concurrency=4` and a group of four look like the same thing and are not. One
consumer is one channel with one prefetch, however many handlers run behind it,
so four handlers still take their messages one at a time. Four consumers are
four channels with four prefetches, and the broker round-robins between them.

The example runs the same four messages both ways and prints how long each took.
"""

from __future__ import annotations

import asyncio
import os
import time

from acemq_amqp import Ack, Message, Topology, accept, connect
from acemq_amqp.patterns import ConsumerGroup

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-group-invoices"

INVOICES = ["INV-1", "INV-2", "INV-3", "INV-4"]

# Long enough that two handlers overlapping is not a coincidence, and short
# enough that the slow half of the example is still under three seconds.
WORK = 0.5


class Watcher:
    """Counts handlers that are running at the same moment.

    The number this example is about. Everything else — elapsed time, messages
    handled — follows from it.
    """

    def __init__(self) -> None:
        self.running = 0
        self.most = 0
        self.handled: list[str] = []
        self.done = asyncio.Event()

    async def handle(self, message: Message) -> Ack:
        self.running += 1
        self.most = max(self.most, self.running)
        try:
            await asyncio.sleep(WORK)
            self.handled.append(message.payload["invoice"])
            if len(self.handled) == len(INVOICES):
                self.done.set()
            return accept()
        finally:
            self.running -= 1


async def fill(mq, queue: str) -> None:
    publisher = mq.publisher(routing_key=queue)
    for invoice in INVOICES:
        await publisher.send({"invoice": invoice})


async def finished(watcher: Watcher, what: str) -> float:
    started = time.monotonic()
    try:
        await asyncio.wait_for(watcher.done.wait(), timeout=30)
    except asyncio.TimeoutError:
        raise SystemExit(
            f"{what} handled {len(watcher.handled)} of {len(INVOICES)} invoices"
        ) from None
    return time.monotonic() - started


async def main() -> None:
    async with await connect(URL) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        # ------------------------------------------------------------------
        # Four consumers, each with its own channel and its own prefetch of one.
        group_watcher = Watcher()
        await fill(mq, QUEUE)

        group = await ConsumerGroup.start(
            mq, QUEUE, 4, group_watcher.handle, prefetch=1, tag="py-invoices"
        )
        print(f"group of {group.size} on {group.queue}")
        group_took = await finished(group_watcher, "the group")

        # One call, and every one of them is stopped — including the handlers
        # already running, which are waited for. Starting four consumers by hand
        # means remembering to close four, and a partial shutdown leaves messages
        # held by a consumer nobody is waiting for.
        await group.close()
        print(f"consumers left on the connection: {len(mq.consumers)}")

        # ------------------------------------------------------------------
        # One consumer running four handlers, over the same four messages.
        single_watcher = Watcher()
        await fill(mq, QUEUE)

        one = await mq.consume(
            QUEUE, single_watcher.handle, prefetch=1, concurrency=4, tag="py-invoices-1"
        )
        single_took = await finished(single_watcher, "the single consumer")
        await one.close()

        print()
        print(f"{'':<28} {'at once':>8} {'seconds':>8}")
        print(f"{'group of 4, prefetch 1':<28} {group_watcher.most:>8} {group_took:>8.1f}")
        print(f"{'1 consumer, concurrency 4':<28} {single_watcher.most:>8} {single_took:>8.1f}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    if sorted(group_watcher.handled) != INVOICES:
        raise SystemExit(f"the group lost or duplicated an invoice: {group_watcher.handled}")
    if sorted(single_watcher.handled) != INVOICES:
        raise SystemExit(f"the single consumer lost an invoice: {single_watcher.handled}")

    # The claim the pattern makes: four consumers hold four messages at once
    # because there are four prefetches, and one consumer holds one however many
    # handlers are behind it.
    if group_watcher.most != len(INVOICES):
        raise SystemExit(
            f"a group of four held {group_watcher.most} messages at once, not four"
        )
    if single_watcher.most != 1:
        raise SystemExit(
            f"one consumer with a prefetch of one held {single_watcher.most} messages "
            "at once; the prefetch is per consumer and this is no longer true"
        )
    if group_took >= single_took:
        raise SystemExit(
            f"the group took {group_took:.1f}s and the single consumer {single_took:.1f}s, "
            "which is not what four prefetches buy"
        )


if __name__ == "__main__":
    asyncio.run(main())
