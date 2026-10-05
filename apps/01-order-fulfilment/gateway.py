"""Where orders enter the system.

The edge of a system is where the dual-write problem lives: an order has to be
saved *and* announced, and doing those as two writes means a crash between them
either loses the announcement or announces something that was never saved.
Neither is recoverable by retrying, because the process that would retry is the
one that died.

So the gateway does one write. The event is inserted in the same transaction as
the order, and a relay publishes it afterwards.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import timedelta
from functools import partial
from pathlib import Path

import contracts
from acemq_amqp import Connection, Envelope, connect
from acemq_amqp.patterns import OutboxRelay, SqlOutboxStore, create_schema, record


class GatewayService:
    def __init__(self, mq: Connection, database: Path) -> None:
        self._mq = mq
        self._connections = partial(sqlite3.connect, database)
        # The relay's own connections come from here, because it runs on its own
        # schedule and must not be inside anybody's request transaction.
        self._outbox = SqlOutboxStore(self._connections)
        self._relay = OutboxRelay(
            mq, self._outbox, interval=timedelta(milliseconds=200), batch=20
        )

    @classmethod
    async def start(cls, url: str, database: Path) -> GatewayService:
        mq = await connect(url)
        # Every service applies the whole topology. Applying it five times is
        # safe and means there is no deployment order to get wrong.
        await mq.declare(contracts.topology())

        gateway = cls(mq, database)
        create_schema(gateway._connections, idempotency=None, registry=None)
        with gateway._connections() as setup:
            setup.execute(
                "CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, customer TEXT,"
                " sku TEXT, quantity INTEGER, total REAL, status TEXT)"
            )
        gateway._relay.start()
        return gateway

    async def place_order(self, customer: str, sku: str, quantity: int, total: float) -> str:
        """Takes an order, and returns the id the customer is given.

        In a real gateway this is the body of an HTTP handler. The two writes
        look exactly like this.
        """
        order_id = f"ord-{uuid.uuid4().hex[:8]}"
        event = contracts.OrderPlaced(order_id, customer, sku, quantity, total)

        transaction = self._connections()
        try:
            transaction.execute(
                "INSERT INTO orders (id, customer, sku, quantity, total, status)"
                " VALUES (?, ?, ?, ?, ?, 'PLACED')",
                (order_id, customer, sku, quantity, total),
            )
            # The outbox writes through the caller's connection. That is the
            # whole trick: there is no second commit that can fail on its own.
            # The payload is encoded here, inside the transaction, because the
            # outbox stores bytes and the relay republishes exactly those bytes:
            # this is the wire format, and it is the one Java writes.
            await self._outbox.add(
                record(
                    self._mq,
                    contracts.EXCHANGE,
                    contracts.ORDER_PLACED,
                    contracts.to_wire(event),
                    envelope=Envelope(
                        type="OrderPlaced",
                        id=order_id,
                        correlation_id=order_id,
                        origin=self._mq.origin,
                    ),
                ),
                connection=transaction,
            )
            transaction.commit()
        except BaseException:
            transaction.rollback()
            raise
        finally:
            transaction.close()
        return order_id

    async def pending_in_outbox(self) -> int:
        return await self._outbox.count()

    async def close(self) -> None:
        await self._relay.close()
        await self._mq.close()
