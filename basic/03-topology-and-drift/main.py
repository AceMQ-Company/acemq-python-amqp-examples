"""Declaring a topology, printing it before applying it, and catching a broker that disagrees.

The interesting half is the second one. A queue's arguments are fixed when it is
created and AMQP has no way to alter them, so a service whose idea of a queue
has moved on cannot declare it — it is answered PRECONDITION_FAILED and cannot
consume at all. That is the library refusing to let a service and its broker
disagree quietly, and it is much better read here than at three in the morning.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from acemq_amqp import Topology, connect, exponential_retry

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

EXCHANGE = "py-shipping-events"
QUEUE = "py-shipping.labels"

POLICY = exponential_retry(4, timedelta(seconds=45), timedelta(minutes=5))


def wanted() -> Topology:
    """Everything this service needs the broker to have."""
    return (
        Topology()
        .exchange(EXCHANGE, "topic")
        .queue(QUEUE, dead_letter=True, retry=POLICY)
        .binding(QUEUE, EXCHANGE, "label.#")
    )


async def main() -> None:
    topology = wanted()

    # plan() is the dry run: what would be declared, in the order it would be
    # declared, without touching the broker. A retry policy above the 30-second
    # line turns into a queue per delay, and this is where somebody notices
    # that a six-attempt policy asked for five queues they did not expect.
    print("the plan:")
    for action in topology.plan():
        print(f"  {action.kind} {action.name} {action.detail}".rstrip())

    async with await connect(URL) as mq:
        await mq.declare(topology)
        print(f"applied. {QUEUE} exists: {await mq.queue_exists(QUEUE)}")

        # Applying the same topology again is fine — declaring is idempotent as
        # long as nothing about the declaration changed.
        await mq.declare(wanted())
        print("re-applied the same topology, unchanged")

        # Now the drift, on a connection of its own because that is what it
        # really is: a second service starting up with a different idea of the
        # same queue. A release that decided the queue should be classic rather
        # than quorum is a one-word change in the source and an impossible one
        # on the broker.
        drifted = Topology().queue(QUEUE, dead_letter=True, retry=POLICY, quorum=False)
        async with await connect(URL, origin="the-other-service@example") as other:
            try:
                await other.declare(drifted)
                print("the broker took the drifted declaration, which it should not have")
            except Exception as refused:  # the broker's own word for it
                print(f"refused: {type(refused).__name__}: {_first_line(refused)}")

        for queue in (QUEUE, f"{QUEUE}.dlq", f"{QUEUE}.parked"):
            await mq.delete_queue(queue)
        for delay in POLICY.broker_rungs():
            await mq.delete_queue(f"{QUEUE}.retry.{_spell(delay)}")


def _first_line(error: BaseException) -> str:
    return str(error).splitlines()[0]


def _spell(delay: timedelta) -> str:
    """The way the library spells a delay in a rung queue name."""
    millis = int(delay.total_seconds() * 1000)
    if millis % 3_600_000 == 0:
        return f"{millis // 3_600_000}h"
    if millis % 60_000 == 0:
        return f"{millis // 60_000}m"
    return f"{millis // 1_000}s"


if __name__ == "__main__":
    asyncio.run(main())
