"""Claims: the module that has to ask another module a question.

Before settling a claim it must know whether the policy is in force, and it
needs the answer *now*. An event cannot answer a question, so it asks over the
broker. The callee is in the same process and a function call would work today;
it would also make the two modules one, and the seam is the point.

The timeout is the part not to skip. And when it expires the claim is **neither
settled nor rejected**: a lookup that did not answer is not a "no".
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept
from acemq_amqp.patterns import Requester, RequestTimeoutError

#: Generous for an in-process hop, and still bounded.
LOOKUP_TIMEOUT = timedelta(seconds=5)


class ClaimUndecidedError(Exception):
    """Whether the policy is in force could not be established. Retry the claim."""


class ClaimsModule:
    def __init__(self, mq: Connection) -> None:
        self._settled = mq.publisher(
            contracts.EXCHANGE, contracts.CLAIM_SETTLED, mandatory=True
        )
        self._rejected = mq.publisher(
            contracts.EXCHANGE, contracts.CLAIM_REJECTED, mandatory=True
        )
        self.settled = 0
        self.rejected = 0

    @classmethod
    async def start(
        cls, mq: Connection, *, timeout: timedelta = LOOKUP_TIMEOUT
    ) -> ClaimsModule:
        claims = cls(mq)
        claims.requester = await Requester.open(
            mq, "", contracts.POLICY_LOOKUP, timeout=timeout
        )
        # Listens for issued policies only to know they exist at all; the
        # authoritative answer still comes from the lookup, because this module
        # deliberately keeps no copy of another module's state.
        claims._issued = await mq.consume(contracts.CLAIMS, claims._noted)
        return claims

    def _noted(self, message: Message) -> Ack:
        return accept()

    async def submit(self, policy_id: str, amount: int, description: str) -> str:
        """Assesses a claim against a policy, and returns the claim id."""
        claim_id = f"CLM-{uuid.uuid4().hex[:8]}"
        envelope = Envelope(type="Claim", correlation_id=policy_id)
        try:
            answer = await self.requester.ask(
                contracts.to_wire(contracts.PolicyQuery(policy_id))
            )
        except RequestTimeoutError as silence:
            # Not an answer, and must not be treated as "no".
            raise ClaimUndecidedError(
                f"could not establish whether {policy_id} is in force, so claim "
                f"{claim_id} was neither settled nor rejected; it must be retried"
            ) from silence
        status = contracts.from_wire(contracts.PolicyStatus, answer)

        if not status.in_force:
            await self._rejected.send(
                contracts.to_wire(
                    contracts.ClaimRejected(claim_id, policy_id, "no policy in force")
                ),
                envelope=envelope,
            )
            self.rejected += 1
            return claim_id

        await self._settled.send(
            contracts.to_wire(contracts.ClaimSettled(claim_id, policy_id, amount)),
            envelope=envelope,
        )
        self.settled += 1
        return claim_id

    async def close(self) -> None:
        await self._issued.close(timeout=10)
        await self.requester.close()
