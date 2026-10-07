"""A statement per account, built by reading the journal.

This module makes one claim checkable: **a projection is disposable.** It stores
nothing the log does not contain, it is built by reading from offset zero, and
throwing it away costs nothing but the time to read the log again.

It also proves the log is shared. The ledger reads the same stream from the
same offset for its own purposes, and neither reader affects the other — no
competing consumption, no "who got the message". That is the property a queue
does not have.
"""

from __future__ import annotations

import contracts
from acemq_amqp import Ack, Connection, Consumer, Message, accept
from acemq_amqp.patterns import from_first, from_next, read_stream


class StatementProjection:
    def __init__(self) -> None:
        self._statements: dict[str, list[contracts.EntryPosted]] = {}
        self._reader: Consumer | None = None

    @classmethod
    async def start(
        cls, mq: Connection, *, from_the_beginning: bool = True
    ) -> StatementProjection:
        projection = cls()
        projection._reader = await read_stream(
            mq,
            contracts.JOURNAL,
            projection._record,
            offset=from_first() if from_the_beginning else from_next(),
            declare=False,
        )
        return projection

    def _record(self, message: Message) -> Ack:
        entry = contracts.from_wire(contracts.EntryPosted, message.payload)
        self._statements.setdefault(entry.account, []).append(entry)
        return accept()

    def statement_of(self, account: str) -> list[contracts.EntryPosted]:
        """The entries seen for an account, oldest first."""
        return list(self._statements.get(account, []))

    def balance_of(self, account: str) -> int:
        """The sum of the entries, which is what a balance is."""
        return sum(entry.amount_minor for entry in self._statements.get(account, []))

    @property
    def entries(self) -> int:
        return sum(len(entries) for entries in self._statements.values())

    async def close(self) -> None:
        if self._reader is not None:
            await self._reader.close()

    async def __aenter__(self) -> StatementProjection:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
