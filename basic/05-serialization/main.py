"""Six formats on one queue, read by one consumer.

This is what a format migration actually looks like: the producers change one at
a time, the queue carries both spellings for a while, and the consumer has to
read whatever turns up. A `CompositeCodec` is that consumer — it asks each codec
whether it can read the content type it was given, in order, and the first that
says yes gets the body.

The five optional formats are the five Java and Go ship, so a message from
either is readable here. Each is an install extra of its own: a service that
speaks YAML has no reason to install a protobuf runtime.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from acemq_amqp import (
    Ack,
    Codec,
    CompositeCodec,
    Envelope,
    JsonCodec,
    Message,
    Topology,
    accept,
    connect,
)
from acemq_amqp.codecs.avro import AvroCodec
from acemq_amqp.codecs.protobuf import ProtobufCodec
from acemq_amqp.codecs.toml import TomlCodec
from acemq_amqp.codecs.xml import XmlCodec
from acemq_amqp.codecs.yaml import YamlCodec

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-formats.orders"

AVRO_SCHEMA = """
{"type": "record", "name": "OrderPlaced", "namespace": "acemq.example",
 "fields": [{"name": "order_id", "type": "string"},
            {"name": "total_cents", "type": "long"},
            {"name": "tenant", "type": "string"}]}
"""


def order_placed() -> type[Any]:
    """`message OrderPlaced { string order_id = 1; int64 total_cents = 2; ... }`.

    Built from a descriptor at run time rather than from a generated module, so
    that this example is one file and needs no protoc. A real service imports
    what protoc produced; the codec cannot tell the difference, because both are
    the same generated class.
    """
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file = descriptor_pb2.FileDescriptorProto()
    file.name = "order.proto"
    file.package = "acemq.example"
    file.syntax = "proto3"
    message = file.message_type.add()
    message.name = "OrderPlaced"
    for name, number, kind in (
        ("order_id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("total_cents", 2, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("tenant", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    ):
        field = message.field.add()
        field.name = name
        field.number = number
        field.type = kind
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    built: type[Any] = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("acemq.example.OrderPlaced")
    )
    return built


def fields(payload: Any) -> dict[str, Any]:
    """A decoded payload as a mapping, whatever the codec handed back.

    The protobuf codec returns the generated message itself, which is the point
    of protobuf: typed fields rather than a bag of strings. XML is the other end
    of that — every value comes back a string, because XML has no types.
    """
    if isinstance(payload, dict):
        return payload
    return {field.name: value for field, value in payload.ListFields()}


async def main() -> None:
    protobuf = ProtobufCodec(order_placed())
    avro = AvroCodec(AVRO_SCHEMA)

    # Order matters only where two codecs claim the same content type; none of
    # these do. JSON is first because it is what the connection writes when
    # nobody says otherwise.
    reader = CompositeCodec(JsonCodec(), YamlCodec(), TomlCodec(), XmlCodec(), avro, protobuf)
    print(f"the consumer reads: {reader.content_type} first, and {len(reader.codecs)} in total")

    async with await connect(URL, codec=reader) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        seen: asyncio.Queue[Message] = asyncio.Queue()

        async def handle(message: Message) -> Ack:
            await seen.put(message)
            return accept()

        consumer = await mq.consume(QUEUE, handle)

        order = {"order_id": "A-7", "total_cents": 4250, "tenant": "acme"}
        writers: list[tuple[str, Codec, Any]] = [
            ("json", JsonCodec(), order),
            ("yaml", YamlCodec(), order),
            ("toml", TomlCodec(), order),
            ("xml", XmlCodec("order"), order),
            ("avro", avro, order),
            ("protobuf", protobuf, protobuf.message_type(**order)),
        ]

        # A codec per publisher, which is how a real migration arrives: one
        # producer at a time changes what it writes, and none of them agree to
        # do it on the same day.
        for name, codec, payload in writers:
            await mq.publisher(routing_key=QUEUE, codec=codec).send(
                payload, envelope=Envelope(type="OrderPlaced")
            )
            print(f"published {name} as {codec.content_type}")

        for _ in writers:
            message = await asyncio.wait_for(seen.get(), timeout=10)
            decoded = fields(message.payload)
            print(
                f"read {message.content_type}: order_id={decoded['order_id']!r} "
                f"total_cents={decoded['total_cents']!r} ({len(message.body)} bytes)"
            )

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
