"""Dispatches what has been paid for and reserved.

The simplest service in the system, and it is worth noticing why: it reacts to
one event, does one thing, and publishes one event. It knows nothing about
payments, nothing about stock levels, and nothing about who else cares that an
order shipped.

That is the property the whole architecture is buying. Adding a service that
also reacts to ``stock.reserved`` requires no change here at all.
"""

from __future__ import annotations

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept, connect


class ShippingService:
    def __init__(self, mq: Connection) -> None:
        self._mq = mq
        self._shipped = mq.publisher(contracts.EXCHANGE, contracts.ORDER_SHIPPED)
        self.shipped = 0

    @classmethod
    async def start(cls, url: str) -> ShippingService:
        mq = await connect(url)
        await mq.declare(contracts.topology())
        shipping = cls(mq)
        shipping._consumer = await mq.consume(
            contracts.SHIPPING, shipping._dispatch, prefetch=10
        )
        return shipping

    async def _dispatch(self, message: Message) -> Ack:
        reservation = contracts.from_wire(contracts.StockReserved, message.payload)
        tracking = "TRK-" + reservation.order_id[4:].upper()
        await self._shipped.send(
            contracts.to_wire(
                contracts.OrderShipped(reservation.order_id, reservation.customer, tracking)
            ),
            envelope=Envelope(
                type="OrderShipped", correlation_id=message.envelope.correlation_id
            ),
        )
        self.shipped += 1
        return accept()

    async def close(self) -> None:
        await self._consumer.close(timeout=10)
        await self._mq.close()
