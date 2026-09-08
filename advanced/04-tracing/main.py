"""A consumer's span joined to the publish that caused it, minutes and processes apart.

Metrics answer *how much*. A trace answers *what happened to this message* — this
one was published by checkout, retried twice and given up on — and a counter
cannot.

The thing worth watching is the parentage. A consumer's span is a child of the
publish that caused it, taken from the message's own headers rather than from
whatever context happened to be current when the delivery arrived. Those are
different processes and often minutes apart, and joining them is the one thing a
messaging system needs from tracing that an HTTP client does not.

The context travels in `traceparent` and `tracestate` — deliberately not
`x-acemq-` prefixed, unlike every other header here, because they are the W3C
names every other piece of tracing tooling already reads.

The library depends on the OpenTelemetry *API* alone, so it exports nothing
until an application installs an SDK and configures one. This example is that
application: the exporter below is in memory, so the spans can be printed rather
than shipped somewhere.
"""

from __future__ import annotations

import asyncio
import os

from acemq_amqp import (
    Ack,
    Envelope,
    Message,
    PublishError,
    Topology,
    accept,
    connect,
    headers,
    retry,
)
from acemq_amqp.tracing import OpenTelemetryTracing
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

EXCHANGE = "py-tracing-events"
QUEUE = "py-tracing.orders"


async def main() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    async with await connect(URL) as mq:
        await mq.declare(
            Topology().exchange(EXCHANGE, "topic").queue(QUEUE, dead_letter=True)
        )

        # One line, and every publish and every delivery on this connection is
        # traced. Nothing in a handler changes.
        OpenTelemetryTracing(tracer_provider=provider).install(mq)

        seen: list[Message] = []

        async def place(message: Message) -> Ack:
            seen.append(message)
            if message.payload["order"] == "ORD-BAD":
                # No retry policy on this connection, so this one delivery is
                # the whole of its life: it is dead-lettered rather than tried
                # again. The span still reads `retried`, because that is what
                # the handler decided; where the message went afterwards is the
                # dead-letter queue's story.
                return retry(RuntimeError("the catalogue rejected the line item"))
            return accept()

        consumer = await mq.consume(QUEUE, place)

        publisher = mq.publisher(routing_key=QUEUE)
        await publisher.send({"order": "ORD-1"}, envelope=Envelope(type="OrderPlaced"))
        await publisher.send({"order": "ORD-BAD"}, envelope=Envelope(type="OrderPlaced"))

        # A publish nothing is bound to take, asked for with mandatory=True so
        # the broker returns it instead of dropping it silently. `unroutable`,
        # `failed` and `dead_lettered` set the span status to ERROR; the others,
        # `retried` included, do not — a retry is the system working, and a wall
        # of red traces that turned out fine is how people learn to ignore the
        # colour.
        try:
            await mq.publisher(EXCHANGE, "nobody.listens", mandatory=True).send(
                {"order": "ORD-LOST"}, envelope=Envelope(type="OrderPlaced")
            )
        except PublishError as unroutable:
            print(f"unroutable: {unroutable}")

        while len(seen) < 2:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        print(f"the delivered message carried {headers.TRACEPARENT}: "
              f"{headers.TRACEPARENT in seen[0].envelope.headers}")

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    spans = exporter.get_finished_spans()
    by_id = {span.context.span_id: span for span in spans}
    print(f"{len(spans)} spans:")
    for span in spans:
        parent = span.parent.span_id if span.parent else None
        print(
            f"  {span.name} ({span.kind.name}) status={span.status.status_code.name}"
            f" outcome={span.attributes.get('messaging.acemq.outcome')!r}"
            f" parent={by_id[parent].name if parent in by_id else parent}"
        )

    traces = {span.context.trace_id for span in spans}
    print(
        f"{len(spans)} spans across {len(traces)} traces — "
        "a publish and the consume it caused share one"
    )


if __name__ == "__main__":
    asyncio.run(main())
