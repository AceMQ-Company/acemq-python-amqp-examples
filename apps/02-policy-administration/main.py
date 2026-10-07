"""The whole monolith, six modules, one connection, one database — checking its own claims.

Which is what it is in production too: that is the point of a monolith. The
boundaries are not processes but imports and a broker, and this file is the only
one that imports more than one module.

Six scenarios, each against a freshly started application: the Java system
test's five, with its assertions, plus a lookup that never comes back. A broken
claim prints what was expected and what happened, and the run exits 1.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path
from typing import Any

import contracts
import underwriting as underwriting_module
from acemq_amqp import Connection, Envelope, Topology, connect, dead_letter_queue, parked_queue
from billing import BillingModule
from claims import ClaimsModule, ClaimUndecidedError
from documents import DocumentModule
from policies import PolicyModule
from underwriting import UnderwritingModule

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")


class Application:
    """One process, five modules with code and the audit queue, one shared database."""

    def __init__(self, mq: Connection) -> None:
        self.mq = mq
        self._scratch = tempfile.TemporaryDirectory(prefix="policy-")

    async def start(self) -> Application:
        await self.mq.declare(contracts.topology())
        # One database, several modules — the monolith's actual advantage. The
        # outbox still has to exist, because the broker is not in its transactions.
        database = partial(sqlite3.connect, Path(self._scratch.name) / "policy.db")
        self.policies = await PolicyModule.start(self.mq, database)
        self.underwriting = await UnderwritingModule.start(self.mq)
        self.documents = DocumentModule(self.mq)
        self.billing = await BillingModule.start(self.mq, database)
        self.claims = await ClaimsModule.start(self.mq)
        return self

    async def close(self) -> None:
        for name in ("claims", "billing", "underwriting", "policies"):
            module = getattr(self, name, None)
            if module is not None:
                await module.close()
        self._scratch.cleanup()


def expect(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


async def wait_for(done: Callable[[], bool], what: str, seconds: float = 90) -> None:
    deadline = time.monotonic() + seconds
    while not done():
        if time.monotonic() > deadline:
            raise AssertionError(f"the application never reached: {what}")
        await asyncio.sleep(0.05)


async def the_happy_path(app: Application) -> None:
    """An ordinary application becomes a policy, and the premium is taken once."""
    await app.policies.submit("A. Applicant", "TERM-LIFE", 100_000, 40)

    # Submitted -> underwritten -> issued -> charged, with no module calling another.
    await wait_for(lambda: len(app.billing.charges) == 1, "one charge")
    expect(app.underwriting.accepted, 1, "underwriting accepted")
    expect(app.policies.issued, 1, "policies issued")
    expect(len(app.billing.charges), 1, "charges")

    # 100 base + 20 age loading, from the rating table in the pricing stage.
    policy_id = app.billing.charges[0]
    expect(app.policies.premium_of(policy_id), 120, "the premium recorded")
    expect(await app.policies.pending_in_outbox(), 0, "records left in the outbox")
    print(f"  {policy_id}: issued and charged 120")


async def referred_above_the_limit(app: Application) -> None:
    """An application above the automatic limit is referred, and never becomes a policy."""
    await app.policies.submit("B. Applicant", "TERM-LIFE", 750_000, 35)

    await wait_for(lambda: app.underwriting.declined == 1, "one referral")
    # Nothing downstream ran. Billing charging a referred application would be
    # the expensive version of this bug. Waited a moment, so "nothing" means it.
    await asyncio.sleep(0.5)
    expect(app.policies.issued, 0, "policies issued")
    expect(app.billing.charges, [], "charges")
    print("  referred: no policy, no charge")


async def claims_ask_rather_than_read(app: Application) -> None:
    """A claim is assessed against an answer from policies, not against a local copy."""
    await app.policies.submit("C. Applicant", "TERM-LIFE", 50_000, 30)
    await wait_for(lambda: len(app.billing.charges) == 1, "one charge")
    policy_id = app.billing.charges[0]

    await app.claims.submit(policy_id, 5_000, "windscreen")
    # A policy nobody issued. The lookup is what makes this answerable at all.
    await app.claims.submit("POL-does-not-exist", 5_000, "windscreen")

    await wait_for(lambda: app.claims.settled == 1 and app.claims.rejected == 1, "one of each")
    print(f"  {policy_id}: settled; POL-does-not-exist: rejected")


async def documents_travel_by_reference(app: Application) -> None:
    """A large document travels as a claim check, not as a message."""
    await app.policies.submit("D. Applicant", "TERM-LIFE", 60_000, 45)
    await wait_for(lambda: len(app.billing.charges) == 1, "one charge")
    policy_id = app.billing.charges[0]

    # Four megabytes, which is a small scan and a large message.
    scan = bytes(4 * 1024 * 1024)
    key = await app.documents.store(policy_id, "medical-report", scan)

    fetched = app.documents.fetch(key)
    expect(fetched is not None and len(fetched), len(scan), "bytes redeemed by the key")
    if policy_id not in key or "medical-report" not in key:
        raise AssertionError(f"the key does not name the policy and the kind: {key}")

    # Java stops at the key. The audit queue holds what actually crossed the
    # broker, so the claim can be checked rather than asserted.
    crossed = None
    deadline = time.monotonic() + 30
    while crossed is None and time.monotonic() < deadline:
        delivery = await app.mq.pull(contracts.AUDIT)
        if delivery is None:
            await asyncio.sleep(0.05)
            continue
        await delivery.ack()
        if (
            Envelope.from_headers(delivery.headers, delivery.routing_key).type
            == "DocumentStored"
        ):
            crossed = delivery.body
    if crossed is None:
        raise AssertionError("DocumentStored never reached the audit queue")
    if len(crossed) > 1024:
        raise AssertionError(f"the event carried {len(crossed)} bytes; the document went too")
    print(f"  {key}: 4 MiB stored, {len(crossed)} bytes on the wire")


async def billing_is_idempotent(app: Application) -> None:
    """Three copies of one event charge once; a genuinely different event still charges."""
    await app.policies.submit("E. Applicant", "TERM-LIFE", 80_000, 50)
    await wait_for(lambda: len(app.billing.charges) == 1, "one charge")
    policy_id = app.billing.charges[0]
    premium = app.policies.premium_of(policy_id)

    # Three copies carrying one message id: what a redelivery looks like from
    # the consumer's side.
    message_id = f"redelivery-{uuid.uuid4()}"
    issued = app.mq.publisher(contracts.EXCHANGE, contracts.POLICY_ISSUED, mandatory=True)
    for _ in range(3):
        await issued.send(
            contracts.to_wire(
                contracts.PolicyIssued(policy_id, "APP-x", "E. Applicant", "TERM-LIFE", premium)
            ),
            envelope=Envelope(type="PolicyIssued", id=message_id),
        )

    # Two, not one. The copies share an id and are one charge; they are not the
    # original issue, which had an id of its own.
    await wait_for(lambda: len(app.billing.charges) == 2, "two charges")
    await asyncio.sleep(2)
    expect(len(app.billing.charges), 2, "charges after three copies")
    print(f"  {policy_id}: three copies, {len(app.billing.charges) - 1} extra charge")


async def a_lookup_that_never_answers_decides_nothing(app: Application) -> None:
    """Not in the Java test. The timeout path its README describes, exercised."""
    await app.policies.submit("F. Applicant", "TERM-LIFE", 40_000, 30)
    await wait_for(lambda: len(app.billing.charges) == 1, "one charge")
    policy_id = app.billing.charges[0]

    await app.policies.stop_answering()
    try:
        await app.claims.submit(policy_id, 1_000, "while policies is away")
    except ClaimUndecidedError as undecided:
        print(f"  {undecided}")
    else:
        raise AssertionError("a claim was decided with no answer from policies")
    expect((app.claims.settled, app.claims.rejected), (0, 0), "claims settled and rejected")
    expect(app.claims.requester.timed_out, 1, "requests the requester counted as timed out")


SCENARIOS: list[Callable[[Application], Awaitable[None]]] = [
    the_happy_path,
    referred_above_the_limit,
    claims_ask_rather_than_read,
    documents_travel_by_reference,
    billing_is_idempotent,
    a_lookup_that_never_answers_decides_nothing,
]


def every_queue() -> list[str]:
    """This application's queues and every queue the library declares beside them."""
    declared = Topology()
    for queue in contracts.QUEUES:
        declared = declared.queue(queue, quorum=False, dead_letter=True)
    names = [*declared.queues, *underwriting_module.topology().queues]
    for step in underwriting_module.STEPS:
        queue = underwriting_module.queue_for(step)
        names += [dead_letter_queue(queue), parked_queue(queue)]
    return sorted(set(names))


async def tidy(mq: Connection) -> None:
    """Deletes this app's queues, so a run starts and ends with none of its leftovers.

    The ``policy`` and ``underwriting`` exchanges stay: the library has no call
    to delete one, and declaring them again is a no-op.
    """
    for name in every_queue():
        await mq.delete_queue(name)


async def main() -> int:
    failed = []
    # One connection for the whole application, because it is one application.
    print(underwriting_module.describe())
    async with await connect(URL) as mq:
        try:
            for scenario in SCENARIOS:
                name = scenario.__name__.replace("_", " ")
                print(name)
                await tidy(mq)
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
