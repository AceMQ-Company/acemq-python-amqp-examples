"""One logical payment delivered four times and charged once — and still once after a restart.

At-least-once delivery is the guarantee a broker gives you, so a duplicate is
not a fault to be prevented; it is a normal Tuesday. The retry that arrived
after a handler succeeded but before its acknowledgement got out looks exactly
like a fresh message, because it is one.

The store here is SQLite, on disk. That is the half most write-ups leave out: an
in-memory set deduplicates until the pod restarts, and a pod restarts most often
right after the incident that produced the duplicates.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from pathlib import Path

from acemq_amqp import Ack, Envelope, Message, Topology, accept, connect
from acemq_amqp.patterns import SqlIdempotencyStore, create_schema, idempotent

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-idempotent.charges"
DATABASE = Path(__file__).with_name("charges.db")


def connections() -> sqlite3.Connection:
    """How the store gets a database connection: a callable, called per use.

    A pool's checkout is the same shape, which is the point — nothing in
    `acemq_amqp.patterns.sql` imports a driver. It is written against the
    DB-API 2.0 protocols, so sqlite3 from the standard library works with
    nothing installed and psycopg works if you have it.
    """
    return sqlite3.connect(DATABASE)


async def main() -> None:
    DATABASE.unlink(missing_ok=True)
    create_schema(connections)  # development only; a real one comes from a migration

    store = SqlIdempotencyStore(connections)
    charged: list[str] = []

    async def charge(message: Message) -> Ack:
        charged.append(message.payload["payment"])
        return accept()

    async with await connect(URL) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        # The key is the payment identifier out of the payload, not the
        # envelope id. Four separate publishes are four different messages as
        # far as the broker is concerned; what must not happen twice is the
        # charge, and the charge is identified by what is in the body.
        consumer = await mq.consume(
            QUEUE, idempotent(store, charge, key=lambda m: m.payload["payment"])
        )

        publisher = mq.publisher(routing_key=QUEUE)
        for _ in range(4):
            await publisher.send(
                {"payment": "PAY-42", "amount_cents": 9900},
                envelope=Envelope(type="ChargeRequested"),
            )

        while await mq.message_count(QUEUE) > 0 or len(charged) == 0:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        print(f"delivered 4, charged {len(charged)}: {charged}")
        print(f"keys remembered: {await store.size()}")

        await consumer.close()

    # A different process, in every way that matters: a new store over the same
    # database, and a duplicate that arrives after the restart.
    restarted = SqlIdempotencyStore(connections)
    print(f"after a restart, PAY-42 is still known: {not await restarted.first_time('PAY-42')}")

    async with await connect(URL) as mq:
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")
    DATABASE.unlink(missing_ok=True)


if __name__ == "__main__":
    asyncio.run(main())
