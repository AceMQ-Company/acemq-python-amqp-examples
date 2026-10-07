"""Documents: the claim-check pattern.

A medical report scanned at 300 dpi is tens of megabytes. Putting it on a queue
fills the broker's memory, is copied to every bound queue, and makes a
dead-letter queue impossible to inspect. What travels instead is a **claim
check**: the document goes to a store, and the message carries the key.

The store is a dict, as Java's is a ``ConcurrentHashMap``, so the key can name
the policy and the kind the way Java's does. The library's own
``ClaimCheckCodec`` does this transparently for any payload over a threshold;
this module does it by hand because the event is the claim check here, not an
implementation detail of encoding one.

**Retention is the part people forget.** A message replayed a month later
carries a key, and if the store expired it the replay produces a message nobody
can read — worse than a lost message, because it looks like a message.
"""

from __future__ import annotations

import uuid

import contracts
from acemq_amqp import Connection, Envelope


class DocumentModule:
    def __init__(self, mq: Connection) -> None:
        self._stored = mq.publisher(
            contracts.EXCHANGE, contracts.DOCUMENT_STORED, mandatory=True
        )
        self._store: dict[str, bytes] = {}

    async def store(self, policy_id: str, kind: str, content: bytes) -> str:
        """Stores a document and announces it. The bytes never go near the broker."""
        key = f"doc/{policy_id}/{kind}/{uuid.uuid4().hex[:8]}"
        self._store[key] = content
        await self._stored.send(
            contracts.to_wire(contracts.DocumentStored(policy_id, key, kind, len(content))),
            envelope=Envelope(type="DocumentStored", correlation_id=policy_id),
        )
        return key

    def fetch(self, key: str) -> bytes | None:
        """Redeems a claim check."""
        return self._store.get(key)

    @property
    def held(self) -> int:
        return len(self._store)
