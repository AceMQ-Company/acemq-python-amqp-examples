"""The only thing allowed to append to the journal, and the balances it decides with.

**One writer, deliberately.** Every transfer produces two entries that sum to
zero, and that cannot be enforced by two processes appending independently — a
stream will happily accept an unbalanced pair from each. Making the writer
singular is what makes the invariant checkable at all.

**Read to the end, then stop.** The writer rebuilds its balances by reading the
journal from offset zero, then closes the reader and maintains them itself. The
first Java version kept following the stream *and* applied each entry as it
wrote it, so every entry was counted twice. Keeping only the stream has the
opposite problem: a transfer decided against a balance that does not yet
include the transfer before it.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept
from acemq_amqp.patterns import from_first, read_stream

#: How long without an entry counts as "caught up". Crude, and honest about it:
#: the precise way is to read the last offset first and stop there.
QUIET_PERIOD = 0.4
REBUILD_LIMIT = 30.0


class Balances:
    """Balances computed by reading the journal. Holds nothing anybody wrote down."""

    def __init__(self) -> None:
        self._accounts: dict[str, int] = defaultdict(int)
        self.replayed = 0

    @classmethod
    async def rebuilt_from(cls, mq: Connection) -> Balances:
        balances = cls()
        last_seen = time.monotonic()

        def replay(message: Message) -> Ack:
            nonlocal last_seen
            balances.apply(contracts.from_wire(contracts.EntryPosted, message.payload))
            balances.replayed += 1
            last_seen = time.monotonic()
            return accept()

        # declare=False: the journal is declared by the writer, and a reader has
        # no business creating ledger.journal.dlq beside it.
        reader = await read_stream(
            mq, contracts.JOURNAL, replay, offset=from_first(), declare=False
        )
        try:
            deadline = time.monotonic() + REBUILD_LIMIT
            while time.monotonic() - last_seen < QUIET_PERIOD:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"the journal did not stop producing entries within {REBUILD_LIMIT}s;"
                        " a rebuild cannot finish while somebody is still writing"
                    )
                await asyncio.sleep(0.02)
        finally:
            await reader.close()
        return balances

    def apply(self, entry: contracts.EntryPosted) -> None:
        """Applied by the writer as it appends — safe precisely because there is one writer."""
        self._accounts[entry.account] += entry.amount_minor

    def of(self, account: str) -> int:
        return self._accounts.get(account, 0)

    def snapshot(self) -> dict[str, int]:
        return dict(self._accounts)


class LedgerModule:
    def __init__(self, mq: Connection) -> None:
        self._mq = mq
        # Straight at the stream by name: a stream is addressed as a queue.
        self._journal = mq.publisher("", contracts.JOURNAL, mandatory=True)
        self._rejected = mq.publisher(
            contracts.EXCHANGE, contracts.TRANSFER_REJECTED, mandatory=True
        )
        self.posted = 0
        self.rejected = 0

    @classmethod
    async def start(cls, mq: Connection) -> LedgerModule:
        ledger = cls(mq)
        await mq.declare(contracts.journal())
        # The writer's own view, derived from the log, for the one decision this
        # module has to make.
        ledger._balances = await Balances.rebuilt_from(mq)
        ledger._commands = await mq.consume(contracts.COMMANDS, ledger._apply)
        return ledger

    @property
    def replayed(self) -> int:
        return self._balances.replayed

    async def _apply(self, message: Message) -> Ack:
        transfer = contracts.from_wire(contracts.Transfer, message.payload)
        available = self._balances.of(transfer.from_)
        if transfer.amount_minor <= 0:
            await self._reject(transfer, "a transfer must be for a positive amount")
            return accept()
        if available < transfer.amount_minor:
            # Refused, and the refusal recorded. A ledger that silently drops what
            # it will not do cannot explain itself later.
            await self._reject(
                transfer, f"insufficient funds: {transfer.from_} holds {available}"
            )
            return accept()

        envelope = Envelope(type="EntryPosted", correlation_id=transfer.transfer_id)
        # Two entries, summing to zero, appended one after the other by the only
        # writer. A real ledger appends them as one record so that a crash
        # between them is impossible; that is the honest limitation here.
        await self._post(
            contracts.EntryPosted(
                _entry_id(),
                transfer.transfer_id,
                transfer.from_,
                -transfer.amount_minor,
                transfer.description,
            ),
            envelope,
        )
        await self._post(
            contracts.EntryPosted(
                _entry_id(),
                transfer.transfer_id,
                transfer.to,
                transfer.amount_minor,
                transfer.description,
            ),
            envelope,
        )
        return accept()

    async def _post(self, entry: contracts.EntryPosted, envelope: Envelope) -> None:
        await self._journal.send(contracts.to_wire(entry), envelope=envelope)
        self._balances.apply(entry)
        self.posted += 1

    async def _reject(self, transfer: contracts.Transfer, reason: str) -> None:
        self.rejected += 1
        await self._rejected.send(
            contracts.to_wire(
                contracts.TransferRejected(
                    transfer.transfer_id,
                    transfer.from_,
                    transfer.to,
                    transfer.amount_minor,
                    reason,
                )
            ),
            envelope=Envelope(type="TransferRejected", correlation_id=transfer.transfer_id),
        )

    async def fund(self, account: str, amount_minor: int) -> None:
        """Opens an account with money in it."""
        entry = contracts.EntryPosted(
            _entry_id(), f"OPENING-{account}", account, amount_minor, "opening balance"
        )
        await self._post(entry, Envelope(type="EntryPosted", correlation_id=entry.transfer_id))

    def balance_of(self, account: str) -> int:
        return self._balances.of(account)

    def balances(self) -> dict[str, int]:
        return self._balances.snapshot()

    async def close(self) -> None:
        await self._commands.close(timeout=10)


def _entry_id() -> str:
    return f"E-{uuid.uuid4()}"
