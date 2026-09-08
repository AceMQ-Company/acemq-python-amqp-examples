"""Ten concurrent questions, each getting its own answer, and a failure reaching the caller.

Request and reply over a broker is the pattern people most often write for
themselves and most often write wrongly. The two things worth watching:

* ten requests are in flight at once and each answer finds its own caller. The
  correlation is the envelope's, not a queue per request.
* the responder's tenth question raises, and the caller sees a `ResponderError`
  naming the failure — not a timeout thirty seconds later. A timeout means "no
  answer"; a failure means "the answer is no", and a caller that cannot tell
  them apart retries the one thing that will never work.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

from acemq_amqp import Message, Topology, connect
from acemq_amqp.patterns import Requester, ResponderError, serve

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-quotes.ask"


async def quote(message: Message) -> dict[str, object]:
    """The responder. Returning a value is replying; raising is failing."""
    request = message.payload
    if request["symbol"] == "UNKNOWN":
        raise ValueError("no such instrument")
    await asyncio.sleep(0.05)
    return {"symbol": request["symbol"], "price_cents": 100 * int(request["lot"])}


async def main() -> None:
    async with await connect(URL) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        # Concurrency 5, so the responder really is answering several at once
        # and the ordering below is not an artefact of a single worker.
        responder = await serve(mq, QUEUE, quote, concurrency=5)

        # The requester owns one exclusive, auto-deleting reply queue for all of
        # its questions. A queue per request is the other design, and it costs a
        # declaration and a deletion on the broker for every call.
        async with await Requester.open(
            mq, routing_key=QUEUE, timeout=timedelta(seconds=10)
        ) as caller:
            print(f"replies come back on {caller.reply_queue}")

            asked = [
                caller.ask({"symbol": f"SYM-{n}", "lot": n}) for n in range(1, 11)
            ]
            answers = await asyncio.gather(*asked)
            for answer in answers:
                print(f"{answer['symbol']} -> {answer['price_cents']}")

            try:
                await caller.ask({"symbol": "UNKNOWN", "lot": 1})
                print("the failure did not come back, which is the bug this example exists for")
            except ResponderError as failed:
                print(f"the caller was told: {failed}")

        await responder.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


if __name__ == "__main__":
    asyncio.run(main())
