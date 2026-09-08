"""The attempt counter moving, a message giving up, and a fatal error skipping the wait.

Three payments go through one queue. One fails twice and then works, one fails
for ever, and one fails in a way that will never work however often it is tried.
What you should see is three different endings from one policy.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from acemq_amqp import (
    Ack,
    Envelope,
    FatalError,
    Message,
    Topology,
    accept,
    connect,
    exponential_retry,
    headers,
    retry,
)

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-payments.charge"
DEAD_LETTERS = f"{QUEUE}.dlq"

# Short enough to watch. Every delay is under the 30-second line, so every wait
# is spent in the consumer holding the delivery.
POLICY = exponential_retry(4, timedelta(milliseconds=400), timedelta(seconds=2))

# What the same policy looks like at a production scale, printed but not run.
PRODUCTION = exponential_retry(6, timedelta(seconds=10))


async def main() -> None:
    print(f"the schedule being run: {[str(d) for d in POLICY.schedule()]}")
    print(f"waits spent in the broker: {POLICY.broker_rungs() or 'none, all under 30s'}")
    print(f"a 10s policy would need rungs: {[str(d) for d in PRODUCTION.broker_rungs()]}")

    async with await connect(URL, retry=POLICY) as mq:
        # Passing the policy rather than a list of delays is what keeps the
        # rung queues and the consumer publishing to them derived from one
        # thing. A second copy of the list drifts, and the way that drift shows
        # up is a retry addressed to a queue nobody declared.
        await mq.declare(Topology().queue(QUEUE, dead_letter=True, retry=POLICY))

        attempts: dict[str, list[int]] = {}
        finished: asyncio.Event = asyncio.Event()
        accepted = 0

        async def charge(message: Message) -> Ack:
            nonlocal accepted
            card = message.payload["card"]
            attempts.setdefault(card, []).append(message.envelope.attempt)

            if card == "expired":
                # Nothing about a fourth attempt makes an expired card work, so
                # the remaining attempts are skipped rather than spent.
                raise FatalError("the card expired in 2019")
            if card == "declined":
                return retry(RuntimeError("the issuer declined"))
            if len(attempts[card]) < 3:
                return retry(RuntimeError("the acquirer timed out"))

            accepted += 1
            if accepted == 1:
                finished.set()
            return accept()

        consumer = await mq.consume(QUEUE, charge)

        publisher = mq.publisher(routing_key=QUEUE)
        for card in ("flaky", "declined", "expired"):
            await publisher.send({"card": card}, envelope=Envelope(type="ChargeCard"))

        await asyncio.wait_for(finished.wait(), timeout=30)
        # The two failures have to run out as well, and the last of them waits
        # 0.4 + 0.8 + 1.6 seconds before it does.
        await asyncio.sleep(4)
        await consumer.close()

        for card, seen in sorted(attempts.items()):
            print(f"{card}: attempts {seen}")

        # Read the dead letters as bytes. The message that went there may be
        # exactly the one nothing could decode, and a reader that assumes JSON
        # cannot tell you that.
        print(f"dead letters: {await mq.message_count(DEAD_LETTERS)}")
        while (delivery := await mq.pull(DEAD_LETTERS)) is not None:
            envelope = Envelope.from_headers(delivery.headers, delivery.routing_key)
            await delivery.ack()
            print(f"  {headers.ERROR}: {envelope.error}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(DEAD_LETTERS)
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
