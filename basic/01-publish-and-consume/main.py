"""A durable queue, a confirmed publish, and a consumer that says what it did.

Run it twice. The second run reports the same numbers as the first, which is
only true because the example deletes what it declared on the way out.
"""

from __future__ import annotations

import asyncio
import os

from acemq_amqp import Ack, Envelope, Message, Topology, accept, connect

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

EXCHANGE = "py-orders-events"
QUEUE = "py-orders.placed"


async def main() -> None:
    # `origin` is stamped on every message this connection publishes and is what
    # a consumer three services away reads to find out who started this. It
    # defaults to the process and host; naming it is better, because a pod name
    # is not a service name.
    async with await connect(URL, origin="checkout@example") as mq:
        # Everything this service needs the broker to have, described in one
        # place and applied in one call. `dead_letter=True` also declares
        # py-orders.placed.dlq and the exchange that reaches it, so the answer
        # to "where does a failure go" exists before the first failure does.
        await mq.declare(
            Topology()
            .exchange(EXCHANGE, "topic")
            .queue(QUEUE, dead_letter=True)
            .binding(QUEUE, EXCHANGE, "order.placed")
        )

        received: asyncio.Queue[Message] = asyncio.Queue()

        async def handle(message: Message) -> Ack:
            await received.put(message)
            return accept()

        consumer = await mq.consume(QUEUE, handle)

        publisher = mq.publisher(EXCHANGE, "order.placed")
        for number in range(1, 4):
            result = await publisher.send(
                {"order": f"A-{number}", "total_cents": 1250 * number},
                envelope=Envelope(type="OrderPlaced"),
            )
            # `confirmed` is the broker saying it has the message, not the
            # client saying it wrote to a socket. `routed` is a different
            # question — whether anything was bound to take it — and a publish
            # can be confirmed and unrouted at the same time.
            print(f"published {result.message_id[:8]} confirmed={result.confirmed}")

        for _ in range(3):
            message = await asyncio.wait_for(received.get(), timeout=10)
            envelope = message.envelope
            print(
                f"consumed {envelope.type} {message.payload['order']} "
                f"attempt={envelope.attempt} origin={envelope.origin}"
            )

        await consumer.close()

        # An example that leaves messages behind reports different numbers on
        # its second run, and the queue it left behind is the one that collides
        # with the next example that wanted the name.
        print(f"left on the queue: {await mq.message_count(QUEUE)}")
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
