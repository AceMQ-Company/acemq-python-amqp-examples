"""Three systems, no shared transaction, and what happens when the third one says no.

Reserving stock, taking a payment and booking a courier are three services with
three databases. There is no transaction across them, so "roll it back" is not
something a database can be asked to do — it has to be done by running the
opposite of each step that succeeded, in reverse.

Two things to watch. The compensations run backwards, newest first, because the
later steps are the ones built on the earlier ones. And when a compensation
*itself* fails, the saga reports it as unresolved rather than raising: something
is now half-undone and needs a person, and an exception thrown into a message
handler is a poor way to tell anyone that.
"""

from __future__ import annotations

import asyncio
import os

from acemq_amqp import Ack, Envelope, Message, Topology, accept, connect
from acemq_amqp.patterns import Saga

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

EXCHANGE = "py-saga-events"
LEDGER = "py-saga.ledger"


async def main() -> None:
    async with await connect(URL) as mq:
        await mq.declare(
            Topology()
            .exchange(EXCHANGE, "topic")
            .queue(LEDGER, dead_letter=True)
            .binding(LEDGER, EXCHANGE, "#")
        )

        recorded: list[str] = []

        async def note(message: Message) -> Ack:
            recorded.append(message.routing_key)
            return accept()

        consumer = await mq.consume(LEDGER, note)

        async def announce(key: str, order: dict[str, object]) -> None:
            await mq.publisher(EXCHANGE, key).send(order, envelope=Envelope(type=key))

        def step(key: str, fails: bool = False):
            async def run(order: dict[str, object]) -> None:
                if fails:
                    raise RuntimeError(f"{key} was refused")
                await announce(key, order)

            return run

        booking = (
            Saga[dict[str, object]]("place order")
            .step("reserve stock", step("stock.reserved"), step("stock.released"))
            .step("take payment", step("payment.taken"), step("payment.refunded"))
            .step("book courier", step("courier.booked"))
        )

        happy = await booking.run({"order": "ORD-1"})
        await _settle(recorded, 3)
        print(f"complete={happy.complete} steps={happy.completed}")
        print(f"  the broker saw: {recorded}")

        recorded.clear()
        refused = (
            Saga[dict[str, object]]("place order")
            .step("reserve stock", step("stock.reserved"), step("stock.released"))
            .step("take payment", step("payment.taken"), step("payment.refunded"))
            .step("book courier", step("courier.booked", fails=True))
        )
        unhappy = await refused.run({"order": "ORD-2"})
        await _settle(recorded, 4)
        print(
            f"complete={unhappy.complete} compensated={unhappy.compensated} "
            f"failed_at={unhappy.failed_at!r} because {unhappy.failure}"
        )
        print(f"  the broker saw: {recorded}")

        recorded.clear()

        async def cannot_undo(order: dict[str, object]) -> None:
            raise RuntimeError("the warehouse will not release a picked reservation")

        stuck = (
            Saga[dict[str, object]]("place order")
            .step("reserve stock", step("stock.reserved"), cannot_undo)
            .step("take payment", step("payment.taken"), step("payment.refunded"))
            .step("book courier", step("courier.booked", fails=True))
        )
        half = await stuck.run({"order": "ORD-3"})
        await _settle(recorded, 3)
        print(f"unresolved={half.unresolved} — that is the row a person has to look at")
        print(f"  the broker saw: {recorded}")

        await consumer.close()
        await mq.delete_queue(LEDGER)
        await mq.delete_queue(f"{LEDGER}.dlq")
        await mq.delete_queue(f"{LEDGER}.parked")


async def _settle(recorded: list[str], expected: int) -> None:
    """Wait for the broker to have delivered everything the saga published."""
    for _ in range(100):
        if len(recorded) >= expected:
            await asyncio.sleep(0.1)
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"expected {expected} messages, saw {recorded}")


if __name__ == "__main__":
    asyncio.run(main())
