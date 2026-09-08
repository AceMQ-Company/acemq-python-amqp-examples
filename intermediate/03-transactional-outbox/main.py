"""A message written in the same transaction as the work, and a relay publishing it after.

Placing an order and telling everyone about it are two systems and one decision.
Publish first and a rollback leaves a message about an order that does not
exist; publish after the commit and a crash in between leaves an order nobody
was told about. Neither is rare enough to ignore.

The outbox makes it one write. What you should see is the rolled-back order
leaving nothing behind — not "nothing published", but nothing in the outbox at
all, because it never was.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from pathlib import Path

from acemq_amqp import Ack, Envelope, Message, Topology, accept, connect
from acemq_amqp.patterns import OutboxRelay, SqlOutboxStore, create_schema, record

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-outbox.orders"
DATABASE = Path(__file__).with_name("orders.db")


def connections() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE)


def place_order(database: sqlite3.Connection, order: str) -> None:
    database.execute("INSERT INTO orders (id) VALUES (?)", (order,))


async def main() -> None:
    DATABASE.unlink(missing_ok=True)
    create_schema(connections)
    with connections() as setup:
        setup.execute("CREATE TABLE orders (id TEXT PRIMARY KEY)")

    outbox = SqlOutboxStore(connections)

    async with await connect(URL) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        published: list[str] = []

        async def receive(message: Message) -> Ack:
            published.append(message.payload["order"])
            return accept()

        consumer = await mq.consume(QUEUE, receive)

        # The order that works. One connection, one transaction, two writes:
        # the order and the message about it.
        database = connections()
        try:
            place_order(database, "ORD-1")
            # `add` writes on the connection you hand it. It does not commit it,
            # does not roll it back and does not close it — that is the
            # guarantee, not an oversight.
            await outbox.add(
                record(mq, "", QUEUE, {"order": "ORD-1"}, envelope=Envelope("OrderPlaced")),
                connection=database,
            )
            database.commit()
        finally:
            database.close()

        # The order that does not. Same two writes, and then the payment fails.
        database = connections()
        try:
            place_order(database, "ORD-2")
            await outbox.add(
                record(mq, "", QUEUE, {"order": "ORD-2"}, envelope=Envelope("OrderPlaced")),
                connection=database,
            )
            raise RuntimeError("the card was declined")
        except RuntimeError as failed:
            database.rollback()
            print(f"rolled back: {failed}")
        finally:
            database.close()

        print(f"orders committed: {_orders()}")
        print(f"messages in the outbox: {await outbox.count()}")

        # The relay is a separate concern from the transaction: it publishes
        # what was committed, whenever it gets round to it. sweep() is one pass,
        # called here so the example does not have to wait for the timer.
        relay = OutboxRelay(mq, outbox)
        print(f"the relay published {await relay.sweep()}")
        await relay.close()

        while not published:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.3)
        print(f"received: {published}")

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    DATABASE.unlink(missing_ok=True)


def _orders() -> list[str]:
    with connections() as database:
        return [row[0] for row in database.execute("SELECT id FROM orders ORDER BY id")]


if __name__ == "__main__":
    asyncio.run(main())
