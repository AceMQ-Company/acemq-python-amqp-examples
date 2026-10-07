"""The ledger, against a real broker with a real stream — checking its own claims.

Six scenarios: the Java system test's five, with its assertions, plus a writer
restarted mid-run that must rebuild exactly the balances it had. Each scenario
starts a fresh writer, which rebuilds from everything the earlier scenarios
appended — the journal is only deleted when the run starts and ends.

A broken claim prints what was expected and what happened, and the run exits 1.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any

import contracts
from acemq_amqp import Connection, connect, dead_letter_queue, parked_queue
from ledger import LedgerModule
from projections import StatementProjection
from transfers import TransferGateway

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")


class Application:
    def __init__(self, mq: Connection) -> None:
        self.mq = mq

    async def start(self) -> Application:
        await self.mq.declare(contracts.topology())
        self.ledger = await LedgerModule.start(self.mq)
        self.transfers = await TransferGateway.start(self.mq)
        return self

    async def close(self) -> None:
        for name in ("transfers", "ledger"):
            module = getattr(self, name, None)
            if module is not None:
                await module.close()


def expect(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


async def wait_for(done: Callable[[], bool], what: str, seconds: float = 90) -> None:
    deadline = time.monotonic() + seconds
    while not done():
        if time.monotonic() > deadline:
            raise AssertionError(f"the ledger never reached: {what}")
        await asyncio.sleep(0.05)


async def double_entry(app: Application) -> None:
    """A transfer posts two entries that sum to zero."""
    ledger = app.ledger
    await ledger.fund("alice", 10_000)
    await wait_for(lambda: ledger.balance_of("alice") == 10_000, "alice funded")

    await app.transfers.request("alice", "bob", 2_500, "rent")

    await wait_for(lambda: ledger.balance_of("bob") == 2_500, "bob paid")
    expect(ledger.balance_of("alice"), 7_500, "alice")
    # Money is neither created nor destroyed by a transfer.
    expect(ledger.balance_of("alice") + ledger.balance_of("bob"), 10_000, "alice + bob")
    print("  alice 7500, bob 2500")


async def insufficient_funds(app: Application) -> None:
    """A transfer that would overdraw is refused, and the refusal is recorded."""
    ledger = app.ledger
    await ledger.fund("carol", 1_000)
    await wait_for(lambda: ledger.balance_of("carol") == 1_000, "carol funded")

    await app.transfers.request("carol", "dave", 5_000, "optimistic")

    await wait_for(lambda: bool(app.transfers.refused), "a refusal")
    reason = app.transfers.refused[0].reason
    if "insufficient funds" not in reason:
        raise AssertionError(f"the refusal does not say why: {reason!r}")
    # Nothing was posted. A ledger that half-applies a refused transfer is worse
    # than one that refuses loudly.
    expect(ledger.balance_of("carol"), 1_000, "carol")
    expect(ledger.balance_of("dave"), 0, "dave")
    print(f"  refused: {reason}")


async def a_projection_agrees_with_the_writer(app: Application) -> None:
    """A projection built from offset zero agrees with the writer."""
    ledger = app.ledger
    await ledger.fund("erin", 20_000)
    await wait_for(lambda: ledger.balance_of("erin") == 20_000, "erin funded")
    await app.transfers.request("erin", "frank", 3_000, "invoice 1")
    await app.transfers.request("erin", "frank", 4_000, "invoice 2")
    await wait_for(lambda: ledger.balance_of("frank") == 7_000, "frank paid twice")

    # A reader that has never seen a message, starting at the beginning of time.
    async with await StatementProjection.start(app.mq) as statements:
        await wait_for(lambda: statements.balance_of("frank") == 7_000, "the projection")
        expect(statements.balance_of("erin"), ledger.balance_of("erin"), "erin, projected")
        expect(statements.balance_of("frank"), ledger.balance_of("frank"), "frank, projected")
        # The detail the balance does not have: the opening balance and two debits.
        expect(len(statements.statement_of("erin")), 3, "entries against erin")
        expect(
            [entry.description for entry in statements.statement_of("frank")],
            ["invoice 1", "invoice 2"],
            "frank's statement",
        )
    print("  erin 13000, frank 7000, from the writer and from offset zero")


async def a_later_projection_sees_everything(app: Application) -> None:
    """A projection added later still gets all of history."""
    ledger = app.ledger
    await ledger.fund("grace", 5_000)
    await app.transfers.request("grace", "heidi", 1_000, "before the projection existed")
    await wait_for(lambda: ledger.balance_of("heidi") == 1_000, "heidi paid")

    # Started after the entries were written. On a queue there would be nothing
    # left to read; a stream is not emptied by reading.
    async with await StatementProjection.start(app.mq) as late:
        await wait_for(lambda: late.balance_of("heidi") == 1_000, "the late projection")
        expect(len(late.statement_of("heidi")), 1, "entries against heidi")
        expect(
            late.statement_of("heidi")[0].description,
            "before the projection existed",
            "heidi's entry",
        )
    print("  heidi's entry, read by a projection started after it was written")


async def readers_do_not_compete(app: Application) -> None:
    """Two readers of the same stream do not compete for entries."""
    ledger = app.ledger
    await ledger.fund("ivan", 8_000)
    await app.transfers.request("ivan", "judy", 2_000, "shared")
    await wait_for(lambda: ledger.balance_of("judy") == 2_000, "judy paid")

    async with (
        await StatementProjection.start(app.mq) as first,
        await StatementProjection.start(app.mq) as second,
    ):
        await wait_for(
            lambda: first.balance_of("judy") == 2_000 and second.balance_of("judy") == 2_000,
            "both projections",
        )
        # Both saw the same entry. On a queue exactly one of them would have.
        expect(len(first.statement_of("judy")), 1, "judy, first reader")
        expect(len(second.statement_of("judy")), 1, "judy, second reader")
    print("  judy's entry, seen by both readers")


async def a_restarted_writer_rebuilds_what_it_had(app: Application) -> None:
    """Not in the Java test. The double-counting bug its README describes, ruled out."""
    ledger = app.ledger
    await ledger.fund("kim", 3_000)
    await app.transfers.request("kim", "lee", 1_200, "before the restart")
    await wait_for(lambda: ledger.balance_of("lee") == 1_200, "lee paid")
    before = ledger.balances()

    # The writer goes away and comes back with nothing but the journal.
    await ledger.close()
    app.ledger = ledger = await LedgerModule.start(app.mq)
    expect(ledger.balances(), before, "every balance, rebuilt from the journal")

    # And carries on from there without counting anything twice.
    await app.transfers.request("lee", "kim", 200, "after the restart")
    await wait_for(lambda: ledger.balance_of("kim") == 2_000, "kim repaid")
    await asyncio.sleep(0.5)
    expect(ledger.balance_of("lee"), 1_000, "lee")

    # Every account in the journal, against an independent reader of it. Money in
    # equals the opening balances, and transfers moved it without making any.
    async with await StatementProjection.start(app.mq) as everything:
        await wait_for(lambda: everything.entries == ledger.replayed + 2, "the whole journal")
        for account, balance in ledger.balances().items():
            expect(everything.balance_of(account), balance, f"{account}, projected")
        opened = sum(
            entry.amount_minor
            for account in ledger.balances()
            for entry in everything.statement_of(account)
            if entry.transfer_id.startswith("OPENING-")
        )
        expect(sum(ledger.balances().values()), opened, "money in the ledger")
    print(f"  {ledger.replayed} entries replayed, {len(before)} balances identical")


SCENARIOS: list[Callable[[Application], Awaitable[None]]] = [
    double_entry,
    insufficient_funds,
    a_projection_agrees_with_the_writer,
    a_later_projection_sees_everything,
    readers_do_not_compete,
    a_restarted_writer_rebuilds_what_it_had,
]


async def tidy(mq: Connection) -> None:
    """Deletes the journal and the queues, so every run starts from an empty ledger.

    A second run against a journal the first one left would find alice already
    holding 7,500 and fail on the first claim — which is the ledger being right.
    """
    for queue in (contracts.JOURNAL, *contracts.QUEUES):
        for name in (queue, dead_letter_queue(queue), parked_queue(queue)):
            await mq.delete_queue(name)


async def main() -> int:
    failed = []
    async with await connect(URL) as mq:
        await tidy(mq)
        try:
            for scenario in SCENARIOS:
                name = scenario.__name__.replace("_", " ")
                print(name)
                app = Application(mq)
                try:
                    await app.start()
                    await asyncio.wait_for(scenario(app), timeout=180)
                except (AssertionError, asyncio.TimeoutError) as failure:
                    print(f"  FAILED: {failure or 'took longer than 180s'}")
                    failed.append(name)
                finally:
                    await app.close()
        finally:
            await tidy(mq)

    if failed:
        print(f"{len(failed)} of {len(SCENARIOS)} failed: {', '.join(failed)}")
        return 1
    print(f"all {len(SCENARIOS)} held")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
