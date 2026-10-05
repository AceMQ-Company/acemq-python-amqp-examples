"""What every service in this system agrees on, and nothing else.

The events, the exchange, the queue each service reads, and the routing keys
that connect them. In a larger estate this is what a schema registry holds.

What is deliberately *not* here: any service's domain model, any database
access, any shared helper. A contracts module that grows those stops being a
contract and becomes a shared library, which is how five services turn back
into one deployable that happens to have five entry points.

Every name and every field below is the one Java's ``Fulfilment`` uses, byte
for byte: the payloads go over the wire in camelCase because that is what a
Java record serialises to, and a Python service and a Java service reading the
same queue have to agree on more than the idea of an order.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any, TypeVar

from acemq_amqp import Topology

#: One topic exchange. Every event in the system is published here.
EXCHANGE = "fulfilment"

# ---- routing keys -------------------------------------------------------------
#
# "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the middle is what
# lets a service subscribe to everything about orders without naming each event,
# and lets notifications subscribe to everything at all.

ORDER_PLACED = "fulfilment.order.placed"
PAYMENT_CAPTURED = "fulfilment.payment.captured"
PAYMENT_DECLINED = "fulfilment.payment.declined"
STOCK_RESERVED = "fulfilment.stock.reserved"
STOCK_UNAVAILABLE = "fulfilment.stock.unavailable"
ORDER_SHIPPED = "fulfilment.order.shipped"

# ---- queues -------------------------------------------------------------------
#
# A queue per service, named after the service rather than after the event. Two
# services wanting the same event each get their own copy, and neither can
# starve the other.

PAYMENTS = "fulfilment.payments"
INVENTORY = "fulfilment.inventory"
SHIPPING = "fulfilment.shipping"
NOTIFICATIONS = "fulfilment.notifications"

QUEUES = (PAYMENTS, INVENTORY, SHIPPING, NOTIFICATIONS)


# ---- events -------------------------------------------------------------------
#
# Frozen, so an event cannot be half-built or changed on its way through. Each
# carries the order id, because that is the only identifier every service
# shares.


@dataclass(frozen=True)
class OrderPlaced:
    """Someone placed an order. Published by the gateway, from its outbox."""

    order_id: str
    customer: str
    sku: str
    quantity: int
    total: float


@dataclass(frozen=True)
class PaymentCaptured:
    """The money is ours. Published by payments."""

    order_id: str
    customer: str
    sku: str
    quantity: int
    amount: float


@dataclass(frozen=True)
class PaymentDeclined:
    """It is not, and will not be. Published by payments; nothing downstream proceeds."""

    order_id: str
    customer: str
    reason: str


@dataclass(frozen=True)
class StockReserved:
    """Stock is held for this order. Published by inventory."""

    order_id: str
    customer: str
    sku: str
    quantity: int


@dataclass(frozen=True)
class StockUnavailable:
    """There is not enough. Published by inventory; the money must be given back."""

    order_id: str
    customer: str
    sku: str
    reason: str


@dataclass(frozen=True)
class OrderShipped:
    """On its way. Published by shipping."""

    order_id: str
    customer: str
    tracking: str


Event = TypeVar("Event")


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def to_wire(event: Any) -> dict[str, Any]:
    """An event as the JSON object Java's record would have produced."""
    return {_camel(name): value for name, value in asdict(event).items()}


def from_wire(kind: type[Event], payload: dict[str, Any]) -> Event:
    """An event read back from that object.

    A missing field raises ``KeyError`` rather than defaulting, so a producer
    that stopped agreeing with the contract fails loudly in the consumer — and
    goes round the retry ladder to the dead letters, where someone will see it.
    """
    return kind(**{f.name: payload[_camel(f.name)] for f in fields(kind)})  # type: ignore[arg-type]


def topology() -> Topology:
    """The whole system's topology, as one value.

    Every service applies this on start-up. Applying the same topology from five
    places is safe and is the point: no service depends on another having
    started first, and there is no deployment order to get wrong.

    Classic queues, because Java's ``classicQueue`` is what the other half of
    the system declares, and a broker compares the queue type like any other
    argument: a Python service asking for quorum here would be refused the
    moment a Java one had got there first.
    """
    return (
        Topology()
        .exchange(EXCHANGE, "topic")
        # Payments acts on new orders.
        .queue(PAYMENTS, quorum=False)
        .binding(PAYMENTS, EXCHANGE, ORDER_PLACED)
        # Inventory acts once the money is taken, not before. Reserving stock for
        # an order that cannot be paid for is how a warehouse fills with holds
        # nobody releases.
        .queue(INVENTORY, quorum=False)
        .binding(INVENTORY, EXCHANGE, PAYMENT_CAPTURED)
        # Shipping needs stock held.
        .queue(SHIPPING, quorum=False)
        .binding(SHIPPING, EXCHANGE, STOCK_RESERVED)
        # Notifications wants everything, which is what a wildcard is for.
        .queue(NOTIFICATIONS, quorum=False)
        .binding(NOTIFICATIONS, EXCHANGE, "fulfilment.#")
    )
