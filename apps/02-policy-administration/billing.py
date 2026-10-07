"""Taking the first premium: the one module where handling a message twice is real money.

The monolith makes it easy to assume the problem went away. It did not: the
retry ladder still redelivers, a redeploy mid-handler still leaves a message
unacknowledged, and both produce a second delivery of a message that took money.

The idempotency store is SQL rather than in-memory even though this is one
process, because "one process" is a fact about today. The moment this module is
lifted out, an in-memory store becomes two stores that each think they are the
only one.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept
from acemq_amqp.patterns import SqlIdempotencyStore, create_schema, idempotent


class BillingModule:
    def __init__(self, mq: Connection, database: Callable[[], sqlite3.Connection]) -> None:
        create_schema(database, idempotency="billing_handled", outbox=None, registry=None)
        self._seen = SqlIdempotencyStore(database, table="billing_handled")
        self._charged = mq.publisher(
            contracts.EXCHANGE, contracts.PREMIUM_CHARGED, mandatory=True
        )
        #: One entry per charge actually taken; a duplicate here would be the bug.
        self.charges: list[str] = []

    @classmethod
    async def start(
        cls, mq: Connection, database: Callable[[], sqlite3.Connection]
    ) -> BillingModule:
        billing = cls(mq, database)
        # The store is handed to the consumer rather than used by hand: the claim
        # is taken before the handler and confirmed after it returns, keyed on
        # the message id — Java's ``ConsumerOptions.idempotent(seen)``.
        billing._issued = await mq.consume(
            contracts.BILLING, idempotent(billing._seen, billing._charge), prefetch=10
        )
        return billing

    async def _charge(self, message: Message) -> Ack:
        policy = contracts.from_wire(contracts.PolicyIssued, message.payload)
        self.charges.append(policy.policy_id)
        await self._charged.send(
            contracts.to_wire(
                contracts.PremiumCharged(
                    policy.policy_id, policy.applicant, policy.annual_premium
                )
            ),
            envelope=Envelope(type="PremiumCharged", correlation_id=policy.application_id),
        )
        return accept()

    async def close(self) -> None:
        await self._issued.close(timeout=10)
