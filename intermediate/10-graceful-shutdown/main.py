"""What happens to the message being handled when the pod is told to stop.

Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
killed, and anything still in a handler dies with it. Those messages were never
acknowledged, so the broker redelivers them — correct, and the reason a
deployment shows up as a spike of duplicate work if nobody arranged otherwise.

`close()` is the drain here: it stops the subscription, gives back what was
delivered but not started, and waits for the handlers already running. It waits
for as long as they take, so the bound is the caller's to put on it. The example
closes twice — once with time to spare and once without — and prints what each
left behind.
"""

from __future__ import annotations

import asyncio
import json
import os

from acemq_amqp import Ack, Message, Topology, accept, connect

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-graceful-shutdown.orders"

ORDERS = [f"ORD-{n}" for n in range(1, 6)]

# Long enough that a message is genuinely still being handled at shutdown.
WORK = 1.0


class Worker:
    """A slow handler that remembers how each message it started ended."""

    def __init__(self) -> None:
        self.finished: list[str] = []
        self.cut_off: list[str] = []
        self.started = asyncio.Event()

    async def handle(self, message: Message) -> Ack:
        order = message.payload["order"]
        self.started.set()
        try:
            await asyncio.sleep(WORK)
        except asyncio.CancelledError:
            # Where a handler that outlives the bound finds out: at its next
            # await, as a cancellation. Nothing has settled the message and
            # nothing will — re-raise, and let the broker have it back.
            self.cut_off.append(order)
            raise
        self.finished.append(order)
        return accept()


async def shutdown(label: str, grace: float) -> tuple[bool, Worker]:
    worker = Worker()

    mq = await connect(URL)
    await mq.declare(Topology().queue(QUEUE, dead_letter=True))
    publisher = mq.publisher(routing_key=QUEUE)
    for order in ORDERS:
        await publisher.send({"order": order})

    # A prefetch of three, so that two messages are sitting in this process
    # unstarted when the shutdown comes. close() hands those back straight away
    # rather than working through them first.
    consumer = await mq.consume(QUEUE, worker.handle, prefetch=3)

    # SIGTERM arrives here, with a message half handled.
    await worker.started.wait()
    await asyncio.sleep(WORK / 2)

    # Not asyncio.wait_for(mq.close(), grace), which reads as the same thing and
    # is not: close() swallows the cancellation the timeout sends it, carries on
    # releasing the connection, and returns normally — so wait_for reports a
    # clean drain even when a handler was cut off. Whether the clock ran out has
    # to be read from the task instead.
    closing = asyncio.create_task(mq.close())
    done, _ = await asyncio.wait({closing}, timeout=grace)
    drained = closing in done
    if not drained:
        # The clock ran out. The cancellation goes on into the handler close()
        # is waiting for, and close() still finishes releasing everything.
        closing.cancel()
        await asyncio.wait({closing})

    print(
        f"  {label:<11} drained={drained} in_flight={consumer.in_flight} "
        f"finished={worker.finished} cut_off={worker.cut_off}"
    )
    return drained, worker


async def settled(mq, expected: int) -> list[tuple[str, bool]]:
    """Everything left on the queue, and whether the broker had handed it over before.

    Requeueing on a closed channel is the broker's work and not instant, so
    this waits briefly for the depth the example expects before reading.
    """
    for _ in range(50):
        if await mq.message_count(QUEUE) == expected:
            break
        await asyncio.sleep(0.1)

    left = []
    while (delivery := await mq.pull(QUEUE)) is not None:
        left.append((json.loads(delivery.body)["order"], delivery.redelivered))
        await delivery.ack()
    return sorted(left)


async def main() -> None:
    # Enough time for the handler in hand to finish. This is the shape a
    # service wants: SIGTERM, close within the grace period, exit.
    enough, patient = await shutdown("enough time", grace=5.0)

    async with await connect(URL) as mq:
        after_enough = await settled(mq, len(ORDERS) - 1)
    print(f"  {'':<11} left on the queue: {after_enough}")

    # The same shutdown with a grace period shorter than the handler. drained
    # coming back False is the signal worth logging and alerting on: a message
    # was abandoned mid-flight and will be redelivered to whoever starts next.
    short, hasty = await shutdown("not enough", grace=0.1)

    async with await connect(URL) as mq:
        after_short = await settled(mq, len(ORDERS))
        print(f"  {'':<11} left on the queue: {after_short}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    # The claims the example makes, so that a library that changes any of them
    # fails here rather than printing something different that nobody reads.
    if not enough or patient.finished != ["ORD-1"] or patient.cut_off:
        raise SystemExit(f"a long grace period did not let ORD-1 finish: {patient.__dict__}")
    if [order for order, _ in after_enough] != ORDERS[1:]:
        raise SystemExit(f"a clean close did not settle ORD-1 alone: {after_enough}")
    if short or hasty.finished or hasty.cut_off != ["ORD-1"]:
        raise SystemExit(f"a short grace period did not cut ORD-1 off: {hasty.__dict__}")
    if ("ORD-1", True) not in after_short or len(after_short) != len(ORDERS):
        raise SystemExit(f"the abandoned ORD-1 did not go back, redelivered: {after_short}")


if __name__ == "__main__":
    asyncio.run(main())
