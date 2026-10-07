"""Deciding whether to accept an application, and at what price.

The one part of this application that is genuinely a *sequence*: check the
applicant against the register, price the risk, then decide. Each stage can fail
and be slow for its own reasons, so each gets a queue — a slow stage shows up as
a deep queue you can point at.

Java builds this with ``mq.pipeline("underwriting", ...)``. Python has no
pipeline object; the same thing is a declared route — :func:`route_of` — and a
:func:`follow_slip` consumer per stage. The topology is Java's to the letter: a
direct exchange named after the pipeline, one quorum queue per step called
``underwriting.<step>``, bound on the step's name. The route travels on the
message in Java's step-names form, so a Java stage could take any of these
three places.
"""

from __future__ import annotations

from datetime import timedelta

import contracts
from acemq_amqp import Ack, Connection, Envelope, Message, Topology, accept, exponential_retry
from acemq_amqp.patterns import follow_slip, route_of, start

PIPELINE = "underwriting"

#: Name, then what it does. The name is the routing key and the queue suffix, so
#: it stays short and stable; the description is what the start-up line prints.
STEPS = {
    "register": "look the applicant up on the shared industry register",
    "price": "apply the rating table for the product and the applicant's age",
    "decide": "accept, or refer anything a rule should not be deciding",
}

#: Applications above this are a human's decision, not a rule's.
REFERRAL_THRESHOLD = 500_000

#: The register is somebody else's service and is periodically unavailable,
#: which is a wait rather than a decline.
REGISTER_RETRY = exponential_retry(4, timedelta(milliseconds=200), timedelta(seconds=5))


def describe() -> str:
    """The line somebody unfamiliar with this system reads to find out what the stages do."""
    return f"pipeline {PIPELINE}: " + " | ".join(f"{n} ({what})" for n, what in STEPS.items())


def queue_for(step: str) -> str:
    return f"{PIPELINE}.{step}"


def topology() -> Topology:
    """What Java's ``Pipeline.declareTopology`` declares, and the register's retry rungs."""
    declared = Topology().exchange(PIPELINE, "direct")
    for step in STEPS:
        declared = declared.queue(
            queue_for(step), retry=REGISTER_RETRY if step == "register" else None
        ).binding(queue_for(step), PIPELINE, step)
    return declared


class UnderwritingModule:
    def __init__(self, mq: Connection) -> None:
        self._mq = mq
        self._accepted = mq.publisher(
            contracts.EXCHANGE, contracts.APPLICATION_ACCEPTED, mandatory=True
        )
        self._declined = mq.publisher(
            contracts.EXCHANGE, contracts.APPLICATION_DECLINED, mandatory=True
        )
        self.accepted = 0
        self.declined = 0

    @classmethod
    async def start(cls, mq: Connection) -> UnderwritingModule:
        underwriting = cls(mq)
        await mq.declare(topology())
        stages = {
            "register": underwriting._check_register,
            "price": underwriting._price,
            "decide": underwriting._decide,
        }
        underwriting._stages = [
            await mq.consume(
                queue_for(name),
                follow_slip(mq, stage, pipeline=PIPELINE),
                retry=REGISTER_RETRY if name == "register" else None,
            )
            for name, stage in stages.items()
        ]
        # Fed from the module's own queue rather than bound to the exchange: the
        # pipeline owns its stages' queues, and what enters it is this module's
        # decision.
        underwriting._submissions = await mq.consume(
            contracts.UNDERWRITING, underwriting._enter
        )
        return underwriting

    async def _enter(self, message: Message) -> Ack:
        await start(
            self._mq,
            route_of(PIPELINE, *STEPS),
            message.payload,
            envelope=Envelope(type=PIPELINE, correlation_id=message.envelope.correlation_id),
        )
        return accept()

    def _check_register(self, message: Message) -> dict:
        application = contracts.from_wire(contracts.ApplicationSubmitted, message.payload)
        # A real one calls an industry service; it is the stage most likely to be
        # slow, and it has its own queue to prove it. Java's ``Checked`` record.
        return {
            "application": message.payload,
            "knownToRegister": "known" in application.applicant.lower(),
        }

    def _price(self, message: Message) -> dict:
        checked = message.payload
        application = contracts.from_wire(
            contracts.ApplicationSubmitted, checked["application"]
        )
        # A rating table, compressed to one line. Older applicants and larger sums cost more.
        base = application.sum_assured // 1000
        age_loading = max(0, application.age_of_applicant - 30) * 2
        register_loading = base // 2 if checked["knownToRegister"] else 0
        # Java's ``Priced`` record.
        return {
            "application": checked["application"],
            "annualPremium": base + age_loading + register_loading,
            "refer": application.sum_assured > REFERRAL_THRESHOLD,
        }

    async def _decide(self, message: Message) -> None:
        priced = message.payload
        application = contracts.from_wire(contracts.ApplicationSubmitted, priced["application"])
        envelope = Envelope(
            type="UnderwritingDecision", correlation_id=application.application_id
        )
        if priced["refer"]:
            # Declined rather than parked: "a human must look at this" is a real
            # outcome of underwriting, not a failure of it.
            await self._declined.send(
                contracts.to_wire(
                    contracts.ApplicationDeclined(
                        application.application_id,
                        application.applicant,
                        f"sum assured of {application.sum_assured}"
                        " is above the automatic limit",
                    )
                ),
                envelope=envelope,
            )
            self.declined += 1
            return
        await self._accepted.send(
            contracts.to_wire(
                contracts.ApplicationAccepted(
                    application.application_id,
                    application.applicant,
                    application.product,
                    application.sum_assured,
                    priced["annualPremium"],
                )
            ),
            envelope=envelope,
        )
        self.accepted += 1

    async def close(self) -> None:
        await self._submissions.close(timeout=10)
        for stage in self._stages:
            await stage.close(timeout=10)
