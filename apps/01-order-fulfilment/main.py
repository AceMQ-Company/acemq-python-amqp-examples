"""The whole system, five services, one broker — and checking its own claims.

In production these are five deployments. Here they run in one process against
one real RabbitMQ, which exercises every queue, every hop and every failure path
for the cost of a single broker, and fails if any service stops agreeing with
the contracts.

Five orders go through, each against a freshly started system: one that
succeeds, one where the warehouse is flaky, one over the payment limit, one
where stock runs out, and one the relay announces twice. Every claim is checked;
a broken one prints what was expected and what happened, and the run exits 1.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import contracts
from acemq_amqp import Envelope, connect, dead_letter_queue, parked_queue
from gateway import GatewayService
from inventory import InventoryService
from notifications import NotificationsService
from payments import PaymentsService
from shipping import ShippingService

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")


class System:
    """Five services started side by side, each with only what it owns."""

    def __init__(self) -> None:
        self._scratch = tempfile.TemporaryDirectory(prefix="fulfilment-")

    async def start(self) -> System:
        # A database per service, because services do not share one. The moment
        # two services read the same table, the deployment boundary is a fiction.
        root = Path(self._scratch.name)
        self.gateway = await GatewayService.start(URL, root / "gateway.db")
        self.payments = await PaymentsService.start(URL, root / "payments.db")
        self.inventory = (await InventoryService.start(URL)).with_stock("WIDGET", 10)
        self.shipping = await ShippingService.start(URL)
        self.notifications = await NotificationsService.start(URL)
        return self

    async def close(self) -> None:
        for name in ("notifications", "shipping", "inventory", "payments", "gateway"):
            service = getattr(self, name, None)
            if service is not None:
                await service.close()
        self._scratch.cleanup()


def expect(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected!r}, got {actual!r}")


async def wait_for(done: Callable[[], bool], what: str, seconds: float = 90) -> None:
    deadline = time.monotonic() + seconds
    while not done():
        if time.monotonic() > deadline:
            raise AssertionError(f"the system never reached: {what}")
        await asyncio.sleep(0.05)


async def an_order_travels_through_every_service(system: System) -> None:
    order = await system.gateway.place_order("ada", "WIDGET", 2, 42.00)

    await wait_for(lambda: system.shipping.shipped == 1, "one order shipped")

    # One order in at the gateway, and every service downstream acted exactly once.
    expect(system.payments.captured, 1, "payments captured")
    expect(system.inventory.reserved, 1, "inventory reserved")
    expect(system.shipping.shipped, 1, "shipping shipped")
    # Stock actually moved. Without this the reservation is a log line.
    expect(system.inventory.stock_of("WIDGET"), 8, "WIDGET left")

    # The customer's view is the whole story, assembled from events published by
    # four services that never spoke to each other. The wait is for the final
    # count rather than an intermediate one, which on a fast machine can be
    # passed through between two polls.
    await wait_for(lambda: len(system.notifications.timeline_of(order)) >= 4, "four events")
    expect(
        system.notifications.timeline_of(order),
        ["OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped"],
        "the timeline, built from the correlation id alone",
    )
    # The outbox is empty, so nothing is waiting to be published.
    expect(await system.gateway.pending_in_outbox(), 0, "records left in the outbox")
    print(f"  {order}: {' -> '.join(system.notifications.timeline_of(order))}")


async def a_flaky_warehouse_is_retried_rather_than_failed(system: System) -> None:
    system.inventory.with_flaky_warehouse(2)
    order = await system.gateway.place_order("grace", "WIDGET", 1, 10.00)

    await wait_for(lambda: system.shipping.shipped == 1, "one order shipped")

    # Two failures, then success. The order was never lost and no human was involved.
    if system.inventory.retried < 2:
        raise AssertionError(f"inventory retried {system.inventory.retried} times, wanted >= 2")
    expect(system.inventory.reserved, 1, "inventory reserved")
    await wait_for(
        lambda: "OrderShipped" in system.notifications.timeline_of(order), "OrderShipped seen"
    )
    print(f"  {order}: shipped after {system.inventory.retried} retries")


async def an_order_over_the_limit_stops_at_payments(system: System) -> None:
    order = await system.gateway.place_order("charles", "WIDGET", 1, 5_000.00)

    await wait_for(lambda: system.payments.declined == 1, "one payment declined")

    # Nothing downstream ran, which is the point of declining before reserving.
    expect(system.inventory.reserved, 0, "inventory reserved")
    expect(system.shipping.shipped, 0, "shipping shipped")
    expect(system.inventory.stock_of("WIDGET"), 10, "WIDGET left")

    await wait_for(lambda: len(system.notifications.timeline_of(order)) == 2, "two events")
    expect(
        system.notifications.timeline_of(order),
        ["OrderPlaced", "PaymentDeclined"],
        "the timeline",
    )
    print(f"  {order}: {' -> '.join(system.notifications.timeline_of(order))}")


async def there_is_not_enough_stock_and_retrying_would_not_help(system: System) -> None:
    order = await system.gateway.place_order("alan", "WIDGET", 99, 99.00)

    await wait_for(lambda: system.inventory.rejected == 1, "one reservation refused")

    # The money was taken and the stock was not there. In a real system this is
    # where a refund is triggered; it is deliberately visible rather than swallowed.
    expect(system.payments.captured, 1, "payments captured")
    expect(system.shipping.shipped, 0, "shipping shipped")
    # Refused once and not retried: running out is an outcome, not an error.
    expect(system.inventory.retried, 0, "inventory retries")

    await wait_for(lambda: len(system.notifications.timeline_of(order)) == 3, "three events")
    expect(
        system.notifications.timeline_of(order),
        ["OrderPlaced", "PaymentCaptured", "StockUnavailable"],
        "the timeline",
    )
    print(f"  {order}: {' -> '.join(system.notifications.timeline_of(order))}")


async def an_order_announced_twice_is_charged_once(system: System) -> None:
    order = await system.gateway.place_order("edsger", "WIDGET", 1, 25.00)
    await wait_for(lambda: system.shipping.shipped == 1, "one order shipped")

    # What a relay that died between publishing and marking the record published
    # does on its next sweep: the same message, the same id, a second time.
    async with await connect(URL) as mq:
        await mq.publisher(contracts.EXCHANGE, contracts.ORDER_PLACED).send(
            contracts.to_wire(contracts.OrderPlaced(order, "edsger", "WIDGET", 1, 25.00)),
            envelope=Envelope(type="OrderPlaced", id=order, correlation_id=order),
        )

    await wait_for(lambda: system.payments.duplicates_refused == 1, "the duplicate refused")
    expect(system.payments.captured, 1, "payments captured")
    expect(system.shipping.shipped, 1, "shipping shipped")
    # Notifications sees the duplicate too — it is a real message — and nothing after it.
    await wait_for(lambda: len(system.notifications.timeline_of(order)) >= 5, "five events")
    await asyncio.sleep(0.5)
    expect(
        system.notifications.timeline_of(order),
        ["OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped", "OrderPlaced"],
        "the timeline",
    )
    print(f"  {order}: announced twice, charged {system.payments.captured} time")


SCENARIOS: list[Callable[[System], Awaitable[None]]] = [
    an_order_travels_through_every_service,
    a_flaky_warehouse_is_retried_rather_than_failed,
    an_order_over_the_limit_stops_at_payments,
    there_is_not_enough_stock_and_retrying_would_not_help,
    an_order_announced_twice_is_charged_once,
]


async def tidy() -> None:
    """Deletes this app's queues, so a run starts and ends with none of its leftovers.

    The ``fulfilment`` exchange stays: the library has no call to delete one,
    and declaring it again is a no-op.
    """
    async with await connect(URL) as mq:
        for queue in contracts.QUEUES:
            for name in (queue, dead_letter_queue(queue), parked_queue(queue)):
                await mq.delete_queue(name)


async def main() -> int:
    await tidy()
    failed = []
    try:
        for scenario in SCENARIOS:
            name = scenario.__name__.replace("_", " ")
            print(name)
            system = System()
            try:
                await system.start()
                await asyncio.wait_for(scenario(system), timeout=180)
            except (AssertionError, asyncio.TimeoutError) as failure:
                print(f"  FAILED: {failure or 'took longer than 180s'}")
                failed.append(name)
            finally:
                await system.close()
    finally:
        await tidy()

    if failed:
        print(f"{len(failed)} of {len(SCENARIOS)} failed: {', '.join(failed)}")
        return 1
    print(f"all {len(SCENARIOS)} held")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
