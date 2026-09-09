"""A payload too large for a broker, put aside, and the reference that travels instead.

A forty-megabyte message is possible and is a mistake: it sits in the broker's
memory, it is copied to every queue bound to the exchange, and it turns a
dead-letter queue into something nobody can open. What travels instead is a
claim check — the payload goes to a store, and the message carries the key.

The threshold is the part worth watching rather than the offloading. Below it a
payload travels inline, exactly as it would without this codec, because turning
a two-hundred-byte event into a store round trip *and* a broker round trip makes
the common case slower in order to fix the rare one. Three bytes at the front of
the body say which of the two a message is, so a consumer reads both without
being told which to expect — which is what lets the threshold be changed, or
this codec be introduced, on a queue that already has messages in it.
"""

from __future__ import annotations

import asyncio
import base64
import os
import shutil
from pathlib import Path

from acemq_amqp import Ack, Envelope, JsonCodec, Message, Topology, accept, connect
from acemq_amqp.patterns import (
    DEFAULT_THRESHOLD,
    ClaimCheckCodec,
    FilesystemClaimCheckStore,
    claim_key_of,
    is_claim_check,
)

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-claim-check.invoices"
PARKED = f"{QUEUE}.parked"

# Where the payloads go. Beside the example so it can be looked at while this
# runs; in a service it is the mount, the volume or the bucket every publisher
# and every consumer can reach.
PAYLOADS = Path(__file__).with_name("payloads")

# An ordinary invoice. A couple of hundred bytes, which is what nearly all of
# them are, and the case an unconditional claim check would make worse.
INVOICE = {
    "invoice": "INV-2231",
    "supplier": "Northwind Paper",
    "total_cents": 8450,
    "lines": [{"sku": "A4-80GSM", "quantity": 40}],
}

# The same invoice with the supplier's scan attached, base64 in the body the way
# an attachment usually arrives. Half a megabyte is an unremarkable scan and
# eight times the threshold.
SCANNED = {
    **INVOICE,
    "invoice": "INV-2232",
    "scan": base64.b64encode(b"%PDF-1.4\n" + bytes(384 * 1024)).decode("ascii"),
}


def held() -> list[str]:
    """The keys the store is holding, which are file names on this one."""
    return sorted(path.name for path in PAYLOADS.iterdir())


async def main() -> None:
    shutil.rmtree(PAYLOADS, ignore_errors=True)

    # The filesystem store rather than the in-memory one, and that choice is the
    # pattern rather than a detail. `InMemoryClaimCheckStore` keeps the payloads
    # in the publisher's own memory — which is where they were going to be
    # anyway — so a consumer in another process finds nothing at all. What makes
    # a claim check work is the payload outliving the process that wrote it.
    #
    # A directory is the honest middle ground: right where the filesystem is
    # shared and durable, and no better than the dictionary on a container's
    # local disk. Object storage is the usual answer in a deployment, and a
    # store in front of S3 is the same three methods.
    store = FilesystemClaimCheckStore(PAYLOADS)
    codec = ClaimCheckCodec(JsonCodec(), store)

    async with await connect(URL, codec=codec, origin="examples/07-claim-check") as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        publisher = mq.publisher(routing_key=QUEUE)
        await publisher.send(INVOICE, envelope=Envelope(type="InvoiceReceived"))
        await publisher.send(SCANNED, envelope=Envelope(type="InvoiceReceived"))

        # Pulled rather than consumed, so the bodies can be looked at as the
        # broker holds them and then decoded by hand. The broker has no idea any
        # of this happened: it holds two messages, and one of them is 39 bytes.
        wire: list[bytes] = []
        for _ in range(2):
            delivery = await mq.pull(QUEUE)
            assert delivery is not None
            await delivery.ack()
            wire.append(delivery.body)

        inline, checked = wire
        print(f"small invoice:  {len(inline):>6} bytes on the wire, "
              f"framing {inline[:3].hex(' ')}, claim check: {is_claim_check(inline)}")
        print(f"with the scan:  {len(checked):>6} bytes on the wire, "
              f"framing {checked[:3].hex(' ')}, claim check: {is_claim_check(checked)}")
        # Answered from the body alone, holding no store at all. It is the line
        # an operator wants in front of a dead-letter queue: which object does
        # this message need, and is it still there?
        print(f"key on the wire: {claim_key_of(checked)}")
        print(f"in the store:    {held()}")

        held_after_two = held()

        # Both come back through the same codec, and the consumer does not know
        # which was which — the framing told it.
        read_back = [codec.decode(body, "application/json") for body in wire]
        for invoice in read_back:
            scan = invoice.get("scan", "")
            print(f"read back:       {invoice['invoice']}, {len(scan)} bytes of scan")

        # -------------------------------------------------------------------
        # The boundary itself, with no broker in the way. The comparison is
        # strictly less than, so a payload of exactly the threshold is the first
        # one that is offloaded — the same number and the same comparison in all
        # five libraries, because a threshold that differs by language means two
        # services disagree about which messages are claim checks.
        overhead = len(JsonCodec().encode({"scan": ""}))
        below = codec.encode({"scan": "x" * (DEFAULT_THRESHOLD - 1 - overhead)})
        at = codec.encode({"scan": "x" * (DEFAULT_THRESHOLD - overhead)})
        print(f"{DEFAULT_THRESHOLD - 1} bytes encoded: "
              f"{len(below)} on the wire, claim check: {is_claim_check(below)}")
        print(f"{DEFAULT_THRESHOLD} bytes encoded: "
              f"{len(at)} on the wire, claim check: {is_claim_check(at)}")

        # -------------------------------------------------------------------
        # And what the pattern costs. The store and the queue have separate
        # lifetimes and nothing enforces a relationship between them, so a
        # payload can be removed while a message referring to it is still
        # deliverable. Here that is a directory being emptied; in a deployment it
        # is a lifecycle rule on a bucket, or a volume reclaimed with a pod.
        await publisher.send(SCANNED, envelope=Envelope(type="InvoiceReceived"))
        for path in PAYLOADS.iterdir():
            path.unlink()

        handled: list[Message] = []

        async def record(message: Message) -> Ack:
            handled.append(message)
            return accept()

        consumer = await mq.consume(QUEUE, record)

        # It is fatal rather than retryable, and the difference matters: the
        # payload is not coming back, so a message redelivered for it holds a
        # queue open until it ages out. A body that will not decode never reaches
        # the handler at all, and the consumer settles it itself — parked rather
        # than dead-lettered, because a message nothing could read is a different
        # problem from one that failed five times.
        for _ in range(150):
            if await mq.message_count(PARKED) > 0:
                break
            await asyncio.sleep(0.1)
        await consumer.close()

        parked = await mq.pull(PARKED)
        assert parked is not None
        await parked.ack()
        reason = Envelope.from_headers(parked.headers, parked.routing_key).error
        print(f"payload gone:    {reason}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(PARKED)

    shutil.rmtree(PAYLOADS, ignore_errors=True)

    # The sizes either side of the threshold are the claim, so they are checked
    # rather than printed. An example that printed whatever it got would go on
    # passing after the threshold stopped being applied.
    if is_claim_check(inline):
        raise SystemExit("a payload under the threshold was put in the store")
    if not is_claim_check(checked):
        raise SystemExit("a payload over the threshold travelled on the broker")
    if len(checked) >= len(inline):
        raise SystemExit(f"the reference is not the smaller of the two: {len(checked)} bytes")
    if len(held_after_two) != 1:
        raise SystemExit(f"expected one payload in the store, found {held_after_two}")
    if claim_key_of(checked) not in held_after_two:
        raise SystemExit("the key on the wire is not the one the store issued")
    if [invoice["invoice"] for invoice in read_back] != ["INV-2231", "INV-2232"]:
        raise SystemExit(f"an invoice did not come back: {read_back}")
    if read_back[1]["scan"] != SCANNED["scan"]:
        raise SystemExit("the stored payload did not come back byte for byte")
    if is_claim_check(below) or not is_claim_check(at):
        raise SystemExit(f"{DEFAULT_THRESHOLD} is no longer compared strictly less than")
    if handled:
        raise SystemExit("a message whose payload was gone reached the handler")
    if "is not in the store" not in reason:
        raise SystemExit(f"the parked message does not say what went wrong: {reason!r}")


if __name__ == "__main__":
    asyncio.run(main())
