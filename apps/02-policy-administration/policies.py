"""Applications and policies: the module that owns the records everything else refers to.

**The outbox is still necessary.** This is a monolith with one database, so the
usual argument for an outbox — two services, two datastores — does not apply.
It applies anyway, because the two systems that must agree are *this database*
and *the broker*, and no transaction spans both.

**Claims asks this module a question rather than reading its tables.** In one
process a direct call would obviously work, which is exactly why the discipline
matters: the moment claims calls a function here, the two modules are one.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from contextlib import closing
from datetime import timedelta

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, accept
from acemq_amqp.patterns import OutboxRelay, SqlOutboxStore, create_schema, record, serve


class PolicyModule:
    def __init__(self, mq: Connection, database: Callable[[], sqlite3.Connection]) -> None:
        self._mq = mq
        self._database = database
        self._outbox = SqlOutboxStore(database)
        self._relay = OutboxRelay(mq, self._outbox, interval=timedelta(milliseconds=200))
        self.issued = 0

    @classmethod
    async def start(
        cls, mq: Connection, database: Callable[[], sqlite3.Connection]
    ) -> PolicyModule:
        policies = cls(mq, database)
        create_schema(database, idempotency=None, registry=None)
        with closing(database()) as setup, setup:
            setup.execute(
                "CREATE TABLE IF NOT EXISTS applications (id TEXT PRIMARY KEY,"
                " applicant TEXT, product TEXT, sum_assured INTEGER, age INTEGER)"
            )
            setup.execute(
                "CREATE TABLE IF NOT EXISTS policies (id TEXT PRIMARY KEY,"
                " application_id TEXT, applicant TEXT, product TEXT, premium INTEGER)"
            )
        policies._relay.start()
        policies._accepted = await mq.consume(contracts.POLICIES, policies._issue)
        policies._lookups = await serve(mq, contracts.POLICY_LOOKUP, policies._status_of)
        return policies

    async def _write(
        self, sql: str, row: tuple, event_key: str, event: object, kind: str, correlation: str
    ) -> None:
        """One row and one outbox record, in one transaction."""
        transaction = self._database()
        try:
            transaction.execute(sql, row)
            # The outbox writes through the caller's connection: there is no
            # second commit that can fail on its own.
            await self._outbox.add(
                record(
                    self._mq,
                    contracts.EXCHANGE,
                    event_key,
                    contracts.to_wire(event),
                    envelope=Envelope(type=kind, correlation_id=correlation),
                ),
                connection=transaction,
            )
            transaction.commit()
        except BaseException:
            transaction.rollback()
            raise
        finally:
            transaction.close()

    async def submit(self, applicant: str, product: str, sum_assured: int, age: int) -> str:
        """Takes an application and announces it, in one transaction."""
        application_id = f"APP-{uuid.uuid4().hex[:8]}"
        await self._write(
            "INSERT INTO applications (id, applicant, product, sum_assured, age)"
            " VALUES (?, ?, ?, ?, ?)",
            (application_id, applicant, product, sum_assured, age),
            contracts.APPLICATION_SUBMITTED,
            contracts.ApplicationSubmitted(
                application_id, applicant, product, sum_assured, age
            ),
            "ApplicationSubmitted",
            application_id,
        )
        return application_id

    async def _issue(self, message: Message) -> Ack:
        """Underwriting said yes, so the policy exists."""
        accepted = contracts.from_wire(contracts.ApplicationAccepted, message.payload)
        policy_id = f"POL-{uuid.uuid4().hex[:8]}"
        await self._write(
            "INSERT INTO policies (id, application_id, applicant, product, premium)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                policy_id,
                accepted.application_id,
                accepted.applicant,
                accepted.product,
                accepted.annual_premium,
            ),
            contracts.POLICY_ISSUED,
            contracts.PolicyIssued(
                policy_id,
                accepted.application_id,
                accepted.applicant,
                accepted.product,
                accepted.annual_premium,
            ),
            "PolicyIssued",
            accepted.application_id,
        )
        self.issued += 1
        return accept()

    def _status_of(self, message: Message) -> dict:
        """Answers the question claims asks, without claims touching this module's tables."""
        query = contracts.from_wire(contracts.PolicyQuery, message.payload)
        return contracts.to_wire(self._lookup(query.policy_id))

    def _lookup(self, policy_id: str) -> contracts.PolicyStatus:
        with closing(self._database()) as connection:
            row = connection.execute(
                "SELECT premium FROM policies WHERE id = ?", (policy_id,)
            ).fetchone()
        return contracts.PolicyStatus(policy_id, row is not None, row[0] if row else 0)

    def premium_of(self, policy_id: str) -> int | None:
        status = self._lookup(policy_id)
        return status.annual_premium if status.in_force else None

    async def pending_in_outbox(self) -> int:
        return await self._outbox.count()

    async def stop_answering(self) -> None:
        """Takes the lookup responder away, as a busy or redeploying module would."""
        await self._lookups.close()

    async def close(self) -> None:
        if not self._lookups.closed:
            await self._lookups.close()
        await self._accepted.close(timeout=10)
        await self._relay.close()
