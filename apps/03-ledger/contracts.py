"""The events a ledger is made of.

In apps/01 and apps/02 the events describe what happened *to* the system of
record. Here they **are** the system of record. There is no balances table that
events update; a balance is what you get by adding up entries, and it can be
deleted and recomputed without losing anything.

One consequence worth stating before the code: **an entry is never changed and
never deleted.** Money moved wrongly is corrected by posting the opposite entry,
and both entries stay.

Every name and field is Java's ``Ledger``, camelCase on the wire.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import timedelta
from typing import Any, TypeVar

from acemq_amqp import Topology
from acemq_amqp.patterns import StreamRetention

#: The stream every entry is appended to. A queue is emptied by being read; a
#: stream is not. Ten readers can each read all of history at their own pace.
JOURNAL = "ledger.journal"

#: An hour, because this is an example. A real ledger keeps entries for as long
#: as the law says. If retention is shorter than "forever", the projection is the
#: system of record after all — and nobody wrote that down.
RETENTION = StreamRetention(max_age=timedelta(hours=1), max_bytes=50 * 1024 * 1024)


def journal() -> Topology:
    """The journal, declared the way Java's ``declareStream`` declares it.

    Not :func:`~acemq_amqp.patterns.stream`, which writes this hour as
    ``x-max-age: 1h``. Java writes ``3600s``, the broker compares the two as
    strings, and whichever side declares second is refused with
    ``PRECONDITION_FAILED - inequivalent arg 'x-max-age'``. Spelled out here so a
    Python writer can start against a journal a Java one created, and the other
    way round.
    """
    return Topology().queue(
        JOURNAL,
        args={
            "x-queue-type": "stream",
            "x-max-age": f"{int(RETENTION.max_age.total_seconds())}s",
            "x-max-length-bytes": RETENTION.max_bytes,
        },
    )


#: Where transfer commands arrive. An ordinary queue: a command is handled once.
COMMANDS = "ledger.commands"

#: Where refusals are announced, for whoever wants to be told rather than to read.
REJECTIONS = "ledger.rejections"

EXCHANGE = "ledger"

TRANSFER_REQUESTED = "ledger.transfer.requested"
ENTRY_POSTED = "ledger.entry.posted"
TRANSFER_REJECTED = "ledger.transfer.rejected"

QUEUES = (COMMANDS, REJECTIONS)


@dataclass(frozen=True)
class EntryPosted:
    """One side of one movement of money.

    Signed rather than a debit/credit flag, so a sum over a column is simply a
    sum. Whole minor units — pennies — because a ledger in ``float`` disagrees
    with itself after enough additions.
    """

    entry_id: str
    transfer_id: str
    account: str
    amount_minor: int
    description: str


@dataclass(frozen=True)
class TransferRejected:
    """A transfer that was refused, with the reason."""

    transfer_id: str
    from_: str
    to: str
    amount_minor: int
    reason: str


@dataclass(frozen=True)
class Transfer:
    """Move money between two accounts. A request, not an event: it may be refused."""

    transfer_id: str
    from_: str
    to: str
    amount_minor: int
    description: str


Event = TypeVar("Event")


def _camel(name: str) -> str:
    # ``from`` is a keyword in Python and a field name in Java's record.
    head, *rest = name.rstrip("_").split("_")
    return head + "".join(part.title() for part in rest)


def to_wire(event: Any) -> dict[str, Any]:
    """An event as the JSON object Java's record would have produced."""
    return {_camel(name): value for name, value in asdict(event).items()}


def from_wire(kind: type[Event], payload: dict[str, Any]) -> Event:
    """An event read back from that object. A missing field raises ``KeyError``."""
    return kind(**{f.name: payload[_camel(f.name)] for f in fields(kind)})  # type: ignore[arg-type]


def topology() -> Topology:
    """Commands to a queue, rejections to a queue. The journal is declared by its writer."""
    return (
        Topology()
        .exchange(EXCHANGE, "topic")
        # Commands: an ordinary queue, because a transfer must be applied once.
        .queue(COMMANDS, quorum=False)
        .binding(COMMANDS, EXCHANGE, TRANSFER_REQUESTED)
        .queue(REJECTIONS, quorum=False)
        .binding(REJECTIONS, EXCHANGE, TRANSFER_REJECTED)
    )
