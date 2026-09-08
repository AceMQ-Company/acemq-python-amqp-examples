"""Message bodies the broker cannot read, and a keyring that can rotate.

A broker holds messages on disk, in its backups and in its management interface,
and everybody with access to any of those can read them. For a queue carrying
card numbers or medical records that is the whole problem, and TLS does not
touch it: TLS protects the wire, not the queue the message sits in for four
hours.

`EncryptedCodec` wraps any other codec and encrypts what it produced. AES-GCM, a
fresh nonce per message, and the key identifier in the clear in front of the
ciphertext — which is what makes rotation possible at all, because a consumer
reads which key a message needs instead of assuming the current one.
"""

from __future__ import annotations

import asyncio
import os

from acemq_amqp import Ack, Envelope, JsonCodec, Message, Topology, accept, connect
from acemq_amqp.codecs.encrypted import (
    EncryptedCodec,
    EncryptionKey,
    Keyring,
    generate_key,
    key_id_of,
)

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-encrypted.payments"

PAYMENT = {"card": "4111111111111111", "amount_cents": 4999, "holder": "A. Person"}


async def main() -> None:
    # In a real deployment these come from a secret manager, one per period.
    # Here they are generated, because a key checked into an examples repository
    # is not a key.
    january = EncryptionKey("2026-01", generate_key())
    february = EncryptionKey("2026-02", generate_key())

    keyring = Keyring(january)
    codec = EncryptedCodec(JsonCodec(), keyring)

    async with await connect(URL, codec=codec) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        # Publish one and read it back as raw bytes, the way the broker holds
        # it. Nothing here is decrypting: this is what an operator with the
        # management interface open actually sees.
        await mq.publisher(routing_key=QUEUE).send(PAYMENT, envelope=Envelope(type="Payment"))
        raw = await mq.pull(QUEUE)
        assert raw is not None
        await raw.ack()
        print(f"on the broker: {raw.content_type} {raw.body[:24].hex()}…")
        print(f"the card number is in there: {b'4111' in raw.body}")
        # Answered from the bytes alone, holding no keys at all. It is what a
        # consumer uses to decide which key to ask for.
        print(f"which key does it need: {key_id_of(raw.body)!r}")

        received: asyncio.Queue[Message] = asyncio.Queue()

        async def settle(message: Message) -> Ack:
            await received.put(message)
            return accept()

        consumer = await mq.consume(QUEUE, settle)

        publisher = mq.publisher(routing_key=QUEUE)
        await publisher.send(PAYMENT, envelope=Envelope(type="Payment"))

        # Rotation. The new key is added and made current; the old one stays on
        # the ring, because messages encrypted with it are still on the queue
        # and still have to be readable. That is the whole reason the identifier
        # travels in the clear.
        keyring.add(february)
        keyring.use("2026-02")
        await publisher.send(PAYMENT, envelope=Envelope(type="Payment"))
        print(f"the ring now holds {keyring.ids}, writing with {keyring.current.id!r}")

        for _ in range(2):
            message = await asyncio.wait_for(received.get(), timeout=10)
            print(
                f"decrypted with {key_id_of(message.body)!r}: "
                f"holder={message.payload['holder']!r}"
            )

        await consumer.close()

        # A consumer given only the new key cannot read a message written under
        # the old one, and fails saying which key it wanted rather than
        # returning something that is not the message.
        newcomer = EncryptedCodec(JsonCodec(), Keyring(february))
        try:
            newcomer.decode(raw.body, raw.content_type)
            print("a message was read without its key, which would be a serious bug")
        except Exception as refused:  # the refusal is the point
            print(f"without 2026-01: {type(refused).__name__}: {refused}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
