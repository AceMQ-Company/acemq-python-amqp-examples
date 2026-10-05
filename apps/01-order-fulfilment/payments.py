"""Takes the money.

This is the service where at-least-once delivery stops being a technicality.
Every other service in this system can handle a message twice and produce the
same outcome; this one cannot, because the second charge is real money
belonging to a real customer.

So it claims each order in a shared store before charging, and confirms
afterwards. The store is shared rather than in-memory because there is more than
one instance of this service in production, and an in-memory store makes each
instance individually idempotent while the fleet is not.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from functools import partial
from pathlib import Path

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept, connect, exponential_retry
from acemq_amqp.patterns import SqlIdempotencyStore, create_schema

#: Over this, a human has to look at it. Every payment system has one of these.
AUTOMATIC_LIMIT = 1_000.00

RETRY = exponential_retry(4, timedelta(milliseconds=200), timedelta(seconds=5))


class PaymentsService:
    def __init__(self, mq: Connection, database: Path) -> None:
        self._mq = mq
        connections = partial(sqlite3.connect, database)
        create_schema(connections, idempotency="payments_handled", outbox=None, registry=None)
        # A generous claim timeout: it has to outlast the slowest charge, because
        # a claim that expires while the payment gateway is still thinking is a
        # claim another instance will take, and then the customer pays twice.
        self._charged = SqlIdempotencyStore(
            connections,
            table="payments_handled",
            claim_timeout=timedelta(minutes=2),
            retention=timedelta(days=7),
        )
        self._captured = mq.publisher(contracts.EXCHANGE, contracts.PAYMENT_CAPTURED)
        self._declined = mq.publisher(contracts.EXCHANGE, contracts.PAYMENT_DECLINED)
        self.captured = 0
        self.declined = 0
        #: How many redeliveries were recognised and refused. Worth graphing.
        self.duplicates_refused = 0

    @classmethod
    async def start(cls, url: str, database: Path) -> PaymentsService:
        mq = await connect(url)
        await mq.declare(contracts.topology())
        payments = cls(mq, database)
        payments._consumer = await mq.consume(
            contracts.PAYMENTS, payments._charge, retry=RETRY, prefetch=20
        )
        return payments

    async def _charge(self, message: Message) -> Ack:
        order = contracts.from_wire(contracts.OrderPlaced, message.payload)

        # The claim is the whole safety net. A redelivery -- from a broker
        # restart, a consumer that died mid-handle, or a relay that published
        # twice -- loses here.
        if not await self._charged.first_time(order.order_id):
            self.duplicates_refused += 1
            return accept()

        # The correlation id is what makes five services one story in a log
        # aggregator. Carrying it forward is not optional.
        correlation = message.envelope.correlation_id

        if order.total > AUTOMATIC_LIMIT:
            await self._declined.send(
                contracts.to_wire(
                    contracts.PaymentDeclined(
                        order.order_id, order.customer, "over the automatic limit"
                    )
                ),
                envelope=Envelope(type="PaymentDeclined", correlation_id=correlation),
            )
            self.declined += 1
            await self._charged.confirm(order.order_id)
            return accept()

        await self._captured.send(
            contracts.to_wire(
                contracts.PaymentCaptured(
                    order.order_id, order.customer, order.sku, order.quantity, order.total
                )
            ),
            envelope=Envelope(type="PaymentCaptured", correlation_id=correlation),
        )
        self.captured += 1

        # Confirmed only after the outcome is published. Confirming first would
        # mean a crash in between leaves the order marked as charged with nothing
        # downstream ever told -- an order that took the money and stopped.
        await self._charged.confirm(order.order_id)
        return accept()

    async def close(self) -> None:
        # Drains: the charge in hand is finished before the connection goes.
        await self._consumer.close(timeout=10)
        await self._mq.close()
