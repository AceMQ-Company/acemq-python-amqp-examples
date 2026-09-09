"""Two services on two versions of one schema, talking to each other anyway.

The old service has not been redeployed and writes orders without a currency.
The new one writes them with one. Neither knows what the other is running, and
both read every message correctly — because each message carries the identifier
of the schema it was *written* with, and the registry turns that identifier back
into a schema the reader can resolve onto its own.

That resolution is the whole product. A field the writer added and the reader
does not know is skipped rather than shifting every field after it; a field the
writer never wrote is filled in from the reader's default rather than missing.
"""

from __future__ import annotations

import asyncio
import os

from acemq_amqp import Ack, Envelope, FatalError, Message, Topology, accept, connect
from acemq_amqp.codecs.avro import AVRO_REGISTERED_CONTENT_TYPE, AvroCodec
from acemq_amqp.patterns import InMemorySchemaRegistry, fingerprint

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

EXCHANGE = "py-schema-orders"
OLD_QUEUE = "py-schema-old-reader"
NEW_QUEUE = "py-schema-new-reader"

#: Groups the versions of one message type. Conventionally the type itself.
SUBJECT = "py.order.placed"

V1 = """
{ "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "total",    "type": "double" }
  ] }
"""

# A field added, with a default. The default is not decoration: without one this
# is not a backwards-compatible change, and a v2 reader meeting a v1 message
# would have nothing to put in the field.
V2 = """
{ "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "total",    "type": "double" },
    { "name": "currency", "type": "string", "default": "EUR" }
  ] }
"""


def schema_id_on(body: bytes) -> int:
    """The identifier framed onto a registered message.

    Confluent's framing, which every AceMQ library writes: one zero byte, then
    four bytes of schema identifier, big-endian, then the Avro body.
    """
    return int.from_bytes(body[1:5], "big")


async def main() -> None:
    # Not a registry in the sense that matters: nothing is shared between
    # processes, so a consumer cannot look up a schema a producer registered
    # somewhere else, which is the entire point of having one. It is here to
    # show the shape. `SqlSchemaRegistry` is the one that outlives the process,
    # and the wire framing is Confluent's, so theirs works too.
    registry = InMemorySchemaRegistry()

    # Two services, each holding the schema it was written against. Each writes
    # with it and resolves every message it reads onto it.
    old_service = await AvroCodec.from_registry(registry, SUBJECT, V1)
    new_service = await AvroCodec.from_registry(registry, SUBJECT, V2)
    print(f"v1 registered as id {old_service.schema_id}")
    print(f"v2 registered as id {new_service.schema_id}")

    # Registering the same definition again is not a new version. Without that,
    # a service that registers its schemas on every start adds a version per
    # restart and the version number stops meaning anything.
    again = await registry.register(SUBJECT, "avro", V2)
    versions = await registry.versions(SUBJECT)
    print(f"versions of {SUBJECT}: {[str(v) for v in versions]}")

    # What a message written by the *other* service arrives framed with. A codec
    # is taught the writer schemas it will meet; asked to decode an identifier it
    # has never seen, it refuses rather than guessing — see below.
    for definition in versions:
        await old_service.learn_from(registry, definition.id)
        await new_service.learn_from(registry, definition.id)

    async with await connect(URL) as mq:
        topology = (
            Topology()
            .exchange(EXCHANGE, "topic")
            .queue(OLD_QUEUE, dead_letter=True)
            .binding(OLD_QUEUE, EXCHANGE, "order.placed")
            .queue(NEW_QUEUE, dead_letter=True)
            .binding(NEW_QUEUE, EXCHANGE, "order.placed")
        )
        await mq.declare(topology)

        read: dict[str, list[tuple[str, dict, int]]] = {"old": [], "new": []}

        def reader(which: str):
            async def handle(message: Message) -> Ack:
                read[which].append(
                    (message.content_type or "", message.payload, schema_id_on(message.body))
                )
                return accept()

            return handle

        consumers = [
            await mq.consume(OLD_QUEUE, reader("old"), codec=old_service),
            await mq.consume(NEW_QUEUE, reader("new"), codec=new_service),
        ]

        # The old service publishing, having never heard of a currency.
        await mq.publisher(EXCHANGE, "order.placed", codec=old_service).send(
            {"order_id": "A-1", "total": 42.0},
            envelope=Envelope(type="OrderPlaced"),
        )
        # And the new one, which has been redeployed.
        await mq.publisher(EXCHANGE, "order.placed", codec=new_service).send(
            {"order_id": "B-2", "total": 99.5, "currency": "GBP"},
            envelope=Envelope(type="OrderPlaced"),
        )

        deadline = asyncio.get_running_loop().time() + 15
        while (
            min(len(read["old"]), len(read["new"])) < 2
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.05)
        for consumer in consumers:
            await consumer.close()

        print()
        print(f"{'reader':<8} {'written with':<13} decoded")
        for which, version in (("old", "v1"), ("new", "v2")):
            in_order = sorted(read[which], key=lambda seen: seen[1]["order_id"])
            for _, payload, written_with in in_order:
                written = f"id {written_with}"
                print(f"{version:<8} {written:<13} {payload}")

        await mq.delete_queue(OLD_QUEUE)
        await mq.delete_queue(NEW_QUEUE)
        for queue in (OLD_QUEUE, NEW_QUEUE):
            await mq.delete_queue(f"{queue}.dlq")
            await mq.delete_queue(f"{queue}.parked")

    # ----------------------------------------------------------------------
    # And what happens to a consumer that never primed itself. `from_registry`
    # teaches a codec its own schema and no other, so this one knows id 1 and
    # meets a message written with id 2.
    unprimed = await AvroCodec.from_registry(registry, SUBJECT, V1)
    v2_bytes = new_service.encode({"order_id": "C-3", "total": 1.0, "currency": "USD"})
    refusal = ""
    try:
        unprimed.decode(v2_bytes, AVRO_REGISTERED_CONTENT_TYPE)
    except FatalError as unknown:
        refusal = str(unknown)
    print(f"\nunprimed reader: {refusal}")

    # ----------------------------------------------------------------------
    if [v.version for v in versions] != [1, 2]:
        raise SystemExit(f"the subject does not have two versions: {versions}")
    if again.id != new_service.schema_id:
        raise SystemExit(
            f"registering v2 twice made a second version: {again.id} and "
            f"{new_service.schema_id}"
        )
    if again.fingerprint != fingerprint(V2):
        raise SystemExit("the fingerprint is not a hash of the definition")

    by_order = {
        which: {payload["order_id"]: (payload, written_with)
                for _, payload, written_with in seen}
        for which, seen in read.items()
    }
    for which in ("old", "new"):
        if sorted(by_order[which]) != ["A-1", "B-2"]:
            raise SystemExit(f"the {which} reader did not see both orders: {by_order[which]}")

    # Every message is framed with the schema it was *written* with, whatever
    # the reader holds. That identifier is the only thing on the wire that makes
    # any of the rest work.
    if by_order["old"]["B-2"][1] != new_service.schema_id:
        raise SystemExit("the v2 message is not framed with the v2 schema id")
    if by_order["new"]["A-1"][1] != old_service.schema_id:
        raise SystemExit("the v1 message is not framed with the v1 schema id")

    # The direction that costs money to get wrong: a producer already on the new
    # schema, a consumer still on the old one. The field it does not know is
    # skipped, and the fields it does know are the right way round.
    old_reading_new = by_order["old"]["B-2"][0]
    if "currency" in old_reading_new:
        raise SystemExit(f"the v1 reader invented a currency: {old_reading_new}")
    if old_reading_new != {"order_id": "B-2", "total": 99.5}:
        raise SystemExit(f"the v1 reader misread a v2 message: {old_reading_new}")

    # And the other way, which is what the default in V2 is for.
    new_reading_old = by_order["new"]["A-1"][0]
    if new_reading_old != {"order_id": "A-1", "total": 42.0, "currency": "EUR"}:
        raise SystemExit(f"the reader's default was not applied: {new_reading_old}")
    if by_order["new"]["B-2"][0]["currency"] != "GBP":
        raise SystemExit("the v2 reader lost the currency the v2 writer wrote")

    if "learn_from" not in refusal or str(new_service.schema_id) not in refusal:
        raise SystemExit(
            f"an unprimed codec did not say which schema it was missing: {refusal!r}"
        )


if __name__ == "__main__":
    asyncio.run(main())
