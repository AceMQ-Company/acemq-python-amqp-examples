"""What every module in this monolith agrees on, and nothing else.

The same role ``contracts.py`` plays in apps/01, and the reason is sharper here:
these modules run in one process, so nothing but discipline stops one importing
another. This file is the only thing they share. Every module imports it and
none of its siblings, which is what makes the monolith *modular* rather than
merely large: a module that only ever received events can be lifted into its own
process by changing where it connects.

Every name and every field is the one Java's ``Policies`` uses, byte for byte.
The payloads are camelCase on the wire because that is what a Java record
serialises to.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any, TypeVar

from acemq_amqp import Topology

#: One topic exchange for the whole application.
EXCHANGE = "policy"

# ---- routing keys -------------------------------------------------------------

APPLICATION_SUBMITTED = "policy.application.submitted"
APPLICATION_ACCEPTED = "policy.application.accepted"
APPLICATION_DECLINED = "policy.application.declined"
POLICY_ISSUED = "policy.policy.issued"
DOCUMENT_STORED = "policy.document.stored"
PREMIUM_CHARGED = "policy.premium.charged"
CLAIM_SUBMITTED = "policy.claim.submitted"
CLAIM_SETTLED = "policy.claim.settled"
CLAIM_REJECTED = "policy.claim.rejected"

# ---- queues -------------------------------------------------------------------
#
# A queue per module, named for the module. That these queues are served by
# tasks in one process is an operational detail, not an architectural one.

UNDERWRITING = "policy.underwriting"
POLICIES = "policy.policies"
BILLING = "policy.billing"
CLAIMS = "policy.claims"

#: Everything, for the audit trail. Not decoration: without it claims and
#: documents publish events nothing is bound to. Java's publisher refuses those
#: by default; Python's drops them unless it is asked to be ``mandatory``, which
#: every publisher in this application is.
AUDIT = "policy.audit"

#: Where claims asks policies whether a policy is in force. Request/reply.
POLICY_LOOKUP = "policy.lookup"

QUEUES = (UNDERWRITING, POLICIES, BILLING, CLAIMS, AUDIT, POLICY_LOOKUP)


# ---- events -------------------------------------------------------------------


@dataclass(frozen=True)
class ApplicationSubmitted:
    """A broker submitted an application. Published by policies, from its outbox."""

    application_id: str
    applicant: str
    product: str
    sum_assured: int
    age_of_applicant: int


@dataclass(frozen=True)
class ApplicationAccepted:
    """Underwriting reached a decision and priced it."""

    application_id: str
    applicant: str
    product: str
    sum_assured: int
    annual_premium: int


@dataclass(frozen=True)
class ApplicationDeclined:
    """Underwriting refused it, with a reason a human can act on."""

    application_id: str
    applicant: str
    reason: str


@dataclass(frozen=True)
class PolicyIssued:
    """A policy exists. Published by policies once underwriting accepted."""

    policy_id: str
    application_id: str
    applicant: str
    product: str
    annual_premium: int


@dataclass(frozen=True)
class DocumentStored:
    """A document belongs to a policy.

    The document itself is **not** here: this is a claim check, the key it was
    stored under and how big it is.
    """

    policy_id: str
    document_key: str
    kind: str
    bytes: int


@dataclass(frozen=True)
class PremiumCharged:
    """The first premium was taken. Published by billing."""

    policy_id: str
    applicant: str
    amount: int


@dataclass(frozen=True)
class ClaimSettled:
    """The claim was assessed."""

    claim_id: str
    policy_id: str
    paid: int


@dataclass(frozen=True)
class ClaimRejected:
    """It was not, and why."""

    claim_id: str
    policy_id: str
    reason: str


@dataclass(frozen=True)
class PolicyQuery:
    """What claims asks."""

    policy_id: str


@dataclass(frozen=True)
class PolicyStatus:
    """What policies answers. A record rather than a boolean, so it can grow a reason."""

    policy_id: str
    in_force: bool
    annual_premium: int


Event = TypeVar("Event")


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def to_wire(event: Any) -> dict[str, Any]:
    """An event as the JSON object Java's record would have produced."""
    return {_camel(name): value for name, value in asdict(event).items()}


def from_wire(kind: type[Event], payload: dict[str, Any]) -> Event:
    """An event read back from that object. A missing field raises ``KeyError``."""
    return kind(**{f.name: payload[_camel(f.name)] for f in fields(kind)})  # type: ignore[arg-type]


def topology() -> Topology:
    """The whole application's topology, as one value, applied once at start-up.

    Classic queues, because Java's ``classicQueue`` is what the other half
    declares and the broker compares the queue type like any other argument.
    """
    return (
        Topology()
        .exchange(EXCHANGE, "topic")
        # Underwriting acts on submissions.
        .queue(UNDERWRITING, quorum=False)
        .binding(UNDERWRITING, EXCHANGE, APPLICATION_SUBMITTED)
        # Policies issues once underwriting has accepted, and answers lookups.
        .queue(POLICIES, quorum=False)
        .binding(POLICIES, EXCHANGE, APPLICATION_ACCEPTED)
        # Billing charges once a policy exists, never before.
        .queue(BILLING, quorum=False)
        .binding(BILLING, EXCHANGE, POLICY_ISSUED)
        # Claims needs to know which policies exist.
        .queue(CLAIMS, quorum=False)
        .binding(CLAIMS, EXCHANGE, POLICY_ISSUED)
        # Everything, including event types not invented yet.
        .queue(AUDIT, quorum=False)
        .binding(AUDIT, EXCHANGE, "policy.#")
        # Not bound: a request is addressed to a queue, not routed to whoever
        # happens to be listening.
        .queue(POLICY_LOOKUP, quorum=False)
    )
