"""Dead-lettered invoices put back one tenant at a time, and the rest afterwards.

Five invoices fail while a downstream system is out. The system comes back, and
the question is not "can we replay" but "can we replay *some of it*" — because
the tenant on the phone is the one that has to go first, and moving all five
means moving four somebody has not finished investigating.
"""

from __future__ import annotations

import asyncio
import json
import os

from acemq_amqp import Ack, Envelope, Message, Topology, accept, connect, retry
from acemq_amqp.patterns import replay

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-replay-invoices"
DEAD_LETTERS = f"{QUEUE}.dlq"

INVOICES = [
    {"invoice": "INV-1", "tenant": "acme"},
    {"invoice": "INV-2", "tenant": "globex"},
    {"invoice": "INV-3", "tenant": "acme"},
    {"invoice": "INV-4", "tenant": "initech"},
    {"invoice": "INV-5", "tenant": "acme"},
]


def only_acme(envelope: Envelope, body: bytes) -> bool:
    """Which dead letters to move.

    The filter is handed the envelope and the raw bytes, not a decoded payload.
    A dead-letter queue is exactly where a message nothing could decode ends up,
    so a replay that insisted on decoding would fail on the messages it is most
    needed for.
    """
    return json.loads(body)["tenant"] == "acme"


async def main() -> None:
    async with await connect(URL) as mq:
        # No retry policy, so one failure is one dead letter. That keeps the
        # example about replaying rather than about waiting.
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        ledger_is_down = True
        settled: list[str] = []

        async def bill(message: Message) -> Ack:
            if ledger_is_down:
                return retry(RuntimeError("the ledger is not answering"))
            settled.append(message.payload["invoice"])
            return accept()

        consumer = await mq.consume(QUEUE, bill)

        publisher = mq.publisher(routing_key=QUEUE)
        for invoice in INVOICES:
            await publisher.send(invoice, envelope=Envelope(type="InvoiceRaised"))

        while await mq.message_count(DEAD_LETTERS) < len(INVOICES):
            await asyncio.sleep(0.1)
        print(f"dead letters: {await mq.message_count(DEAD_LETTERS)}")

        ledger_is_down = False

        # routing_key sends them back to the queue they came from, through the
        # default exchange. `restart` defaults to True, so each message gets a
        # fresh set of attempts and its x-acemq-error cleared — it is being
        # tried again deliberately, not continuing from where it gave up.
        first = await replay(mq, DEAD_LETTERS, routing_key=QUEUE, only=only_acme)
        print(
            f"acme: moved {first.moved}, skipped {first.skipped}, "
            f"stopped because {first.reason}"
        )

        while len(settled) < 3:
            await asyncio.sleep(0.1)
        print(f"settled: {sorted(settled)}")

        # A replay with no filter takes what is left. That the two it declined
        # are still on the queue — rather than consumed and dropped — is the
        # property that makes running a filtered replay safe.
        rest = await replay(mq, DEAD_LETTERS, routing_key=QUEUE)
        print(f"the rest: moved {rest.moved}, stopped because {rest.reason}")

        while len(settled) < 5:
            await asyncio.sleep(0.1)
        print(f"settled: {sorted(settled)}")

        await consumer.close()
        print(f"dead letters left: {await mq.message_count(DEAD_LETTERS)}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(DEAD_LETTERS)
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
