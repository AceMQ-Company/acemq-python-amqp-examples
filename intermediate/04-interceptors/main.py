"""A tenant on every message and every handler timed, without either appearing in a handler.

Every organisation has something every message needs and no library can guess: a
tenant, a trace context, an authorisation token, a size limit. Without a seam
these get copied into every call site, and one of them is eventually forgotten —
and nobody finds out until the message that needed it is the one that went
without.

An interceptor is one function handed the message and the rest of the work. It
composes the way ASGI middleware does, so `try`/`finally` is enough to do
something on the way out, and one interceptor that opens something and closes it
is one function rather than two halves that have to agree.
"""

from __future__ import annotations

import asyncio
import contextvars
import os
import time

from acemq_amqp import (
    Ack,
    ConsumeContext,
    ConsumeNext,
    Envelope,
    Message,
    PublishContext,
    PublishNext,
    Topology,
    accept,
    connect,
)

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-interceptors.audit"

tenant: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="")

trail: list[str] = []


async def stamp_the_tenant(context: PublishContext, send: PublishNext):
    """Puts the caller's tenant on the message. Registered first, so outermost."""
    trail.append("stamp in")
    context.set_header("tenant", tenant.get())
    try:
        return await send(context)
    finally:
        trail.append("stamp out")


async def refuse_the_untenanted(context: PublishContext, send: PublishNext):
    """Refuses a publish rather than letting it through unlabelled.

    Refusing is raising, and the caller sees the exception. That is the whole
    point of intercepting rather than observing: an interceptor that could only
    watch could not have stopped this.
    """
    trail.append("check in")
    if not context.envelope.headers.get("tenant"):
        raise PermissionError(f"acemq: {context.routing_key} needs a tenant")
    try:
        return await send(context)
    finally:
        trail.append("check out")


async def time_the_handler(context: ConsumeContext, handle: ConsumeNext) -> Ack:
    started = time.monotonic()
    try:
        return await handle(context)
    finally:
        took = time.monotonic() - started
        print(f"  {context.queue} took {took * 1000:.0f}ms for tenant "
              f"{context.envelope.headers.get('tenant')}")


async def main() -> None:
    # Registered in order, first registered outermost: it sees the message
    # first on the way in and last on the way out. That matters as soon as one
    # reads what another wrote — the check below only works because the stamp
    # ran before it.
    async with await connect(
        URL,
        on_publish=[stamp_the_tenant, refuse_the_untenanted],
        on_consume=[time_the_handler],
    ) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        handled: asyncio.Queue[Message] = asyncio.Queue()

        async def audit(message: Message) -> Ack:
            # Nothing here knows a tenant exists. That is the measure of whether
            # the seam is worth having.
            await asyncio.sleep(0.02)
            await handled.put(message)
            return accept()

        consumer = await mq.consume(QUEUE, audit)
        publisher = mq.publisher(routing_key=QUEUE)

        tenant.set("acme")
        await publisher.send({"event": "user.invited"}, envelope=Envelope(type="Audit"))
        message = await asyncio.wait_for(handled.get(), timeout=10)
        print(f"the handler saw tenant={message.envelope.headers['tenant']!r}")
        print(f"the order in and out: {trail}")

        tenant.set("")
        try:
            await publisher.send({"event": "user.invited"}, envelope=Envelope(type="Audit"))
            print("an untenanted message was published, which the check should have stopped")
        except PermissionError as refused:
            print(f"refused before it reached the broker: {refused}")

        # Reserved names are refused rather than dropped. Silently discarding a
        # header somebody set is worse than saying no, and an application header
        # that could impersonate an x-acemq- one is a way to forge an envelope.
        try:
            PublishContext("", QUEUE, Envelope(), {}).set_header("x-acemq-id", "forged")
            print("a reserved header was accepted, which it should not have been")
        except ValueError as refused:
            print(f"reserved: {refused}")

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
