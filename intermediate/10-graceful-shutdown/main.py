"""What happens to the message being handled when the pod is told to stop.

Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
killed, and anything still in a handler dies with it. Those messages were never
acknowledged, so the broker redelivers them — correct, and the reason a
deployment shows up as a spike of duplicate work if nobody arranged otherwise.

`close(timeout=...)` is the drain: it stops the subscription, gives back what
was delivered but not started, waits up to the timeout for the handlers already
running, cancels whatever is still running at the deadline, and says which of
the two happened. The example shuts down three ways — with time to spare, with
too little time and three handlers running, and under an outside
`asyncio.wait_for` — and prints what each left behind.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable

from acemq_amqp import Ack, Connection, Consumer, Message, Topology, accept, connect

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


Close = Callable[[Connection, Consumer], Awaitable[bool]]


async def shutdown(
    label: str, close: Close, *, concurrency: int = 1
) -> tuple[bool, float, Worker]:
    worker = Worker()

    mq = await connect(URL)
    await mq.declare(Topology().queue(QUEUE, dead_letter=True))
    publisher = mq.publisher(routing_key=QUEUE)
    for order in ORDERS:
        await publisher.send({"order": order})

    # A prefetch of three. With one handler, two messages sit in this process
    # unstarted when the shutdown comes, and close() hands those back straight
    # away rather than working through them first. With three, all three are
    # being handled.
    consumer = await mq.consume(QUEUE, worker.handle, prefetch=3, concurrency=concurrency)

    # SIGTERM arrives here, with work half done.
    await worker.started.wait()
    await asyncio.sleep(WORK / 2)

    began = time.monotonic()
    drained = await close(mq, consumer)
    took = time.monotonic() - began
    await mq.close()  # already drained or cut off above; releases the rest

    print(
        f"  {label:<14} drained={drained} took={took:.2f}s in_flight={consumer.in_flight} "
        f"finished={worker.finished} cut_off={sorted(worker.cut_off)}"
    )
    return drained, took, worker


async def wait_for_close(mq: Connection, _: Consumer) -> bool:
    """An outside bound on a close that would otherwise wait for ever."""
    try:
        return await asyncio.wait_for(mq.close(timeout=None), timeout=0.1)
    except TimeoutError:
        # The deadline is now reported, not swallowed: the handler was
        # cancelled, the connection released, and then TimeoutError raised.
        return False


async def settled(mq: Connection, expected: int) -> list[tuple[str, bool]]:
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


async def left_behind(expected: int) -> list[tuple[str, bool]]:
    async with await connect(URL) as mq:
        left = await settled(mq, expected)
    print(f"  {'':<14} left on the queue: {left}")
    return left


async def main() -> None:
    failures: list[str] = []

    def check(ok: bool, claim: str, seen: object) -> None:
        if not ok:
            failures.append(f"{claim}: {seen}")

    # Enough time for the handler in hand to finish. This is the shape a
    # service wants: SIGTERM, close within the grace period, exit.
    drained, _, w = await shutdown("enough time", lambda mq, _: mq.close(timeout=5.0))
    left = await left_behind(len(ORDERS) - 1)
    check(
        drained and w.finished == ["ORD-1"] and not w.cut_off,
        "a long grace period did not let ORD-1 finish",
        w.__dict__,
    )
    check([o for o, _ in left] == ORDERS[1:], "a clean close did not settle ORD-1 alone", left)

    # Too little time, three handlers running, closed at the consumer. All
    # three are cut off at the one deadline — not one cut off and the other
    # two left to run on — and drained=False is the line worth alerting on.
    drained, took, w = await shutdown(
        "not enough", lambda _, consumer: consumer.close(timeout=0.1), concurrency=3
    )
    left = await left_behind(len(ORDERS))
    check(
        not drained and not w.finished and sorted(w.cut_off) == ORDERS[:3],
        "a short grace period did not cut off all three handlers",
        w.__dict__,
    )
    check(took < WORK / 2, "the deadline did not bound every handler", f"took {took:.2f}s")
    check(
        left == [(o, o in ORDERS[:3]) for o in ORDERS],
        "the cut-off messages did not go back, redelivered",
        left,
    )

    # An outside bound around close() is honoured too: wait_for raises
    # TimeoutError at its deadline instead of returning as if all was well.
    drained, took, w = await shutdown("wait_for 0.1s", wait_for_close)
    left = await left_behind(len(ORDERS))
    check(
        not drained and w.cut_off == ["ORD-1"] and took < WORK / 2,
        "wait_for around close() did not raise TimeoutError at its deadline",
        w.__dict__,
    )
    check(
        ("ORD-1", True) in left and len(left) == len(ORDERS),
        "the abandoned ORD-1 did not go back, redelivered",
        left,
    )

    async with await connect(URL) as mq:
        for name in (QUEUE, f"{QUEUE}.dlq", f"{QUEUE}.parked"):
            await mq.delete_queue(name)

    # The claims the example makes, so that a library that changes any of them
    # fails here rather than printing something different that nobody reads.
    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    asyncio.run(main())
