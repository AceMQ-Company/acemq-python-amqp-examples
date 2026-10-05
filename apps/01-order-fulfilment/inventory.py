"""Holds stock for orders that have been paid for.

The service that talks to something unreliable. A warehouse system that times
out is the ordinary case, not the exception, and the two failures have to be
told apart:

- the warehouse did not answer — retry, it will probably work in a moment;
- there are three left and the order wants ten — retrying changes nothing.

The first is a plain exception and goes up the retry ladder. The second is not
an error at all: it is an outcome, published as ``StockUnavailable`` so the
customer is told now rather than after four pointless attempts.
"""

from __future__ import annotations

from datetime import timedelta

import contracts
from acemq_amqp import (
    METRIC_RETRIED_TOTAL,
    Ack,
    Connection,
    Envelope,
    Message,
    Metrics,
    accept,
    connect,
    exponential_retry,
)

RETRY = exponential_retry(4, timedelta(milliseconds=200), timedelta(seconds=5))


class InventoryService:
    def __init__(self, mq: Connection, metrics: Metrics) -> None:
        self._mq = mq
        self._metrics = metrics
        self._stock: dict[str, int] = {}
        self._reserved = mq.publisher(contracts.EXCHANGE, contracts.STOCK_RESERVED)
        self._unavailable = mq.publisher(contracts.EXCHANGE, contracts.STOCK_UNAVAILABLE)
        self._warehouse_calls = 0
        #: How many warehouse calls fail before it starts working.
        self._failures_to_simulate = 0
        self.reserved = 0
        self.rejected = 0

    @classmethod
    async def start(cls, url: str) -> InventoryService:
        # The retry count is read from the connection's metrics rather than kept
        # by hand, because the library is what decides a message goes round
        # again — and a handler counting its own exceptions would count the last
        # one, which was dead-lettered rather than retried.
        metrics = Metrics()
        mq = await connect(url, observer=metrics)
        await mq.declare(contracts.topology())
        inventory = cls(mq, metrics)
        inventory._consumer = await mq.consume(
            contracts.INVENTORY, inventory._reserve, retry=RETRY, prefetch=20
        )
        return inventory

    def with_stock(self, sku: str, quantity: int) -> InventoryService:
        self._stock[sku] = quantity
        return self

    def with_flaky_warehouse(self, count: int) -> InventoryService:
        """Makes the next ``count`` warehouse calls fail, the way a real one does."""
        self._failures_to_simulate = count
        return self

    async def _reserve(self, message: Message) -> Ack:
        payment = contracts.from_wire(contracts.PaymentCaptured, message.payload)
        correlation = message.envelope.correlation_id

        # The transient failure. Nothing is wrong with the message, so it goes
        # back on the ladder and arrives again shortly.
        self._warehouse_calls += 1
        if self._warehouse_calls <= self._failures_to_simulate:
            raise TimeoutError("warehouse did not respond")

        available = self._stock.get(payment.sku, 0)
        if available < payment.quantity:
            # The permanent one. Retrying will not conjure stock, and four more
            # attempts only delay telling the customer.
            await self._unavailable.send(
                contracts.to_wire(
                    contracts.StockUnavailable(
                        payment.order_id,
                        payment.customer,
                        payment.sku,
                        f"only {available} left",
                    )
                ),
                envelope=Envelope(type="StockUnavailable", correlation_id=correlation),
            )
            self.rejected += 1
            return accept()

        self._stock[payment.sku] = available - payment.quantity
        await self._reserved.send(
            contracts.to_wire(
                contracts.StockReserved(
                    payment.order_id, payment.customer, payment.sku, payment.quantity
                )
            ),
            envelope=Envelope(type="StockReserved", correlation_id=correlation),
        )
        self.reserved += 1
        return accept()

    def stock_of(self, sku: str) -> int:
        return self._stock.get(sku, 0)

    @property
    def retried(self) -> int:
        return sum(
            value
            for key, value in self._metrics.counts.items()
            if key.startswith(METRIC_RETRIED_TOTAL)
        )

    async def close(self) -> None:
        await self._consumer.close(timeout=10)
        await self._mq.close()
