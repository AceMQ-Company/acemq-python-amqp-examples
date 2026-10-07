"""Where transfers are asked for, and where refusals are noticed.

Deliberately thin. A transfer is a **command**, sent to a queue, which may be
refused — and a refusal is a normal outcome rather than an error. Events are
past tense and cannot be argued with; commands are requests and can be turned
down. Systems that blur the two publish ``TransferMade`` before knowing whether
it was.
"""

from __future__ import annotations

import uuid

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept


class TransferGateway:
    def __init__(self, mq: Connection) -> None:
        self._requested = mq.publisher(
            contracts.EXCHANGE, contracts.TRANSFER_REQUESTED, mandatory=True
        )
        #: The transfers the ledger refused, and why.
        self.refused: list[contracts.TransferRejected] = []

    @classmethod
    async def start(cls, mq: Connection) -> TransferGateway:
        gateway = cls(mq)
        gateway._rejections = await mq.consume(contracts.REJECTIONS, gateway._noticed)
        return gateway

    def _noticed(self, message: Message) -> Ack:
        self.refused.append(contracts.from_wire(contracts.TransferRejected, message.payload))
        return accept()

    async def request(self, from_: str, to: str, amount_minor: int, description: str) -> str:
        """Asks for money to move; the id returned correlates every entry and refusal."""
        transfer_id = f"T-{uuid.uuid4().hex[:8]}"
        await self._requested.send(
            contracts.to_wire(
                contracts.Transfer(transfer_id, from_, to, amount_minor, description)
            ),
            envelope=Envelope(type="Transfer", correlation_id=transfer_id),
        )
        return transfer_id

    async def close(self) -> None:
        await self._rejections.close(timeout=10)
