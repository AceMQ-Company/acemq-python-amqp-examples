"""Tells the customer what happened.

Bound to ``fulfilment.#`` — everything. This is the service that shows why a
topic exchange is worth more than a queue per pair of services: it was added
without a single change to any publisher, and the next one will be too.

It reads six event types of six shapes from one queue, so it does not decode
them at all. The text codec hands over the body as it arrived, and what this
service needs — the type and the correlation id — travels in the envelope.
"""

from __future__ import annotations

import contracts
from acemq_amqp import Ack, Connection, Message, TextCodec, accept, connect


class NotificationsService:
    def __init__(self, mq: Connection) -> None:
        self._mq = mq
        self._timeline: dict[str, list[str]] = {}

    @classmethod
    async def start(cls, url: str) -> NotificationsService:
        mq = await connect(url)
        await mq.declare(contracts.topology())
        notifications = cls(mq)
        notifications._consumer = await mq.consume(
            contracts.NOTIFICATIONS, notifications._record, codec=TextCodec(), prefetch=50
        )
        return notifications

    def _record(self, message: Message) -> Ack:
        # The correlation id is the order it belongs to, set by whichever service
        # published it and carried forward by all of them.
        order = message.envelope.correlation_id
        self._timeline.setdefault(order, []).append(message.envelope.type)
        return accept()

    def timeline_of(self, order_id: str) -> list[str]:
        """What a customer looking at "where is my order" would be shown."""
        return list(self._timeline.get(order_id, []))

    async def close(self) -> None:
        await self._consumer.close(timeout=10)
        await self._mq.close()
