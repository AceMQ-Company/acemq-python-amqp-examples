"""Deliver this message later — and the long one does not hold up the short one.

The obvious way to delay a message is to set an expiration on it, drop it in a
queue nobody consumes, and let it dead-letter to its destination. It is what
most write-ups suggest and it is wrong for anything but a single fixed delay,
because **a classic queue expires messages only at its head**. Put an eight
second message in, then a two second message behind it, and the two second
message is delivered in eight. Nothing reports it: the queue looks healthy and
the message is simply late by a factor nobody predicted.

What to watch: the eight second message is scheduled first and arrives last.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from acemq_amqp import Ack, Message, Topology, accept, connect
from acemq_amqp.patterns import SCHEDULE_RUNGS, Scheduler, schedule_topology

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-schedule.reminders"


async def main() -> None:
    print(f"the ladder: {[str(rung) for rung in SCHEDULE_RUNGS]}")
    print("what the scheduler puts on a broker:")
    for action in schedule_topology().plan():
        print(f"  {action.kind} {action.name} {action.detail}".rstrip())

    async with await connect(URL) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        arrived: list[tuple[str, float]] = []
        started = asyncio.get_running_loop().time()

        async def remind(message: Message) -> Ack:
            arrived.append(
                (message.payload["reminder"], asyncio.get_running_loop().time() - started)
            )
            return accept()

        consumer = await mq.consume(QUEUE, remind)

        # open() declares the ladder and starts the consumer on the control
        # queue. Several schedulers in a deployment are harmless — they consume
        # the same queue and the broker gives each expired message to one.
        async with await Scheduler.open(mq) as scheduler:
            await scheduler.after(timedelta(seconds=8), "", QUEUE, {"reminder": "the long"})
            await scheduler.after(timedelta(seconds=2), "", QUEUE, {"reminder": "the short"})
            # `at` is the same thing said as a wall-clock time, which is what a
            # renewal date or an escalation deadline actually is.
            await scheduler.at(
                datetime.now(timezone.utc) + timedelta(seconds=4),
                "",
                QUEUE,
                {"reminder": "the dated one"},
            )
            print(f"scheduled {scheduler.scheduled}")

            while len(arrived) < 3:
                await asyncio.sleep(0.2)

            print(f"delivered {scheduler.delivered} after {scheduler.hops} hops")

        for reminder, seconds in arrived:
            print(f"  {reminder} arrived after {seconds:.1f}s")

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")
        # The acemq.schedule.* queues are deliberately left where they are.
        # They are shared with every other service scheduling on this broker,
        # Java and Go included, and are declared identically by all of them.


if __name__ == "__main__":
    asyncio.run(main())
