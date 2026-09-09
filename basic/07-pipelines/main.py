"""An order carried through three services by an itinerary it brings with it.

Nothing here knows what comes next. Each step does its one job and hands the
message back, and the slip on the message says where it goes — which is what
makes the order of the steps a property of the message rather than something
compiled into three services that then have to be redeployed together.

The second half is the part that pays for the pattern. A run that fails at the
second step does not start again at the first: the slip records how far it got,
so putting it back means resuming, and the step that already charged somebody
does not charge them twice.
"""

from __future__ import annotations

import asyncio
import json
import os

from acemq_amqp import Envelope, FatalError, Message, Topology, connect
from acemq_amqp.patterns import RoutingSlip, follow_slip, route_of, slip_from, start

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

# The itinerary form: every step names its own destination, so nothing but the
# message is needed to follow it. These go on the default exchange, which is why
# the exchange in each step is empty and the routing key is a queue name.
SLIP_QUEUES = ["py-slip-validate", "py-slip-charge", "py-slip-ship"]

# The declared form: one direct exchange named after the pipeline, one queue per
# step called `{pipeline}.{step}`, bound on the step's own name. That naming is
# Java's and is not arranged for Python's convenience — it is what lets a Python
# step stand in a pipeline a Java service declared.
PIPELINE = "py-fulfilment"
STEPS = ["validate", "charge", "ship"]


def queue_for(step: str) -> str:
    return f"{PIPELINE}.{step}"


async def wait_for(done: asyncio.Event, what: str, within: float = 15.0) -> None:
    try:
        await asyncio.wait_for(done.wait(), timeout=within)
    except asyncio.TimeoutError:
        raise SystemExit(f"{what} never finished") from None


async def main() -> None:
    async with await connect(URL) as mq:
        # ------------------------------------------------------------------
        # One: an itinerary assembled per message.
        await mq.declare(
            Topology()
            .queue(SLIP_QUEUES[0], dead_letter=True)
            .queue(SLIP_QUEUES[1], dead_letter=True)
            .queue(SLIP_QUEUES[2], dead_letter=True)
        )

        stamped = asyncio.Event()
        visited: list[str] = []
        shipped: list[dict] = []

        def stamp(step: str):
            """One stop: it returns the payload for the next stop."""

            async def stage(message: Message) -> dict:
                visited.append(step)
                order = dict(message.payload)
                order.setdefault("stamps", [])
                order["stamps"] = [*order["stamps"], step]
                if step == "ship":
                    shipped.append(order)
                    stamped.set()
                return order

            return stage

        consumers = [
            await mq.consume(SLIP_QUEUES[at], follow_slip(mq, stamp(step)))
            for at, step in enumerate(STEPS)
        ]

        slip = (
            RoutingSlip()
            .then("", SLIP_QUEUES[0], name="validate")
            .then("", SLIP_QUEUES[1], name="charge")
            .then("", SLIP_QUEUES[2], name="ship")
        )
        print(f"itinerary: {slip}")
        await start(mq, slip, {"order": "A-1"})

        await wait_for(stamped, "the itinerary")
        for consumer in consumers:
            await consumer.close()
        print(f"visited:   {visited}")
        print(f"shipped:   {shipped[0] if shipped else None}")

        # ------------------------------------------------------------------
        # Two: a route declared in advance, and a run that resumes.
        topology = Topology().exchange(PIPELINE, "direct")
        for step in STEPS:
            topology = topology.queue(queue_for(step), dead_letter=True).binding(
                queue_for(step), PIPELINE, step
            )
        await mq.declare(topology)

        ran: list[str] = []
        delivered = asyncio.Event()
        the_card_works = False

        def fulfil(step: str):
            async def stage(message: Message) -> dict:
                ran.append(step)
                if step == "charge" and not the_card_works:
                    # Fatal, so it is dead-lettered rather than retried. The
                    # point is a run that stops half way, not one that waits.
                    raise FatalError("the card issuer is refusing everything")
                if step == "ship":
                    delivered.set()
                return message.payload

            return stage

        # `pipeline=` is the one thing the declared form does not put on the
        # wire. Without it the step names come back with nowhere to go.
        running = [
            await mq.consume(
                queue_for(step), follow_slip(mq, fulfil(step), pipeline=PIPELINE)
            )
            for step in STEPS
        ]

        route = route_of(PIPELINE, *STEPS)
        print(f"\nroute:     {route}")
        await start(mq, route, {"order": "B-2"})

        # It gets as far as charge and stops there.
        parked = f"{queue_for('charge')}.dlq"
        deadline = asyncio.get_running_loop().time() + 15
        while (
            await mq.message_count(parked) < 1
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.05)
        print(f"ran:       {ran}")

        # ------------------------------------------------------------------
        # The operator's half. The message that stopped carries the route and
        # the position it reached, which is the whole of what is needed to put
        # it back where it was rather than at the beginning.
        stopped = await mq.pull(parked)
        if stopped is None:
            raise SystemExit("nothing was dead-lettered, so there is nothing to resume")
        await stopped.ack()

        envelope = Envelope.from_headers(stopped.headers, stopped.routing_key)
        resume = slip_from(envelope, pipeline=PIPELINE)
        if resume is None:
            raise SystemExit("the dead-lettered message carries no route")
        print(f"resuming:  {resume}")
        print(f"           at position {envelope.route_position} of {envelope.route}")

        the_card_works = True
        await start(mq, resume, json.loads(stopped.body))

        await wait_for(delivered, "the resumed run")
        for consumer in running:
            await consumer.close()
        print(f"ran:       {ran}")

        # ------------------------------------------------------------------
        for queue in [*SLIP_QUEUES, *(queue_for(step) for step in STEPS)]:
            await mq.delete_queue(queue)
            await mq.delete_queue(f"{queue}.dlq")
            await mq.delete_queue(f"{queue}.parked")

    if visited != ["validate", "charge", "ship"]:
        raise SystemExit(f"the itinerary was not followed in order: {visited}")
    if not shipped or shipped[0]["stamps"] != ["validate", "charge", "ship"]:
        raise SystemExit(f"the payload did not accumulate every step: {shipped}")

    # What the resume bought. `validate` charged nothing, but `charge` did, and
    # a restart would have run both again.
    if resume.done and [step.name for step in resume.done] != ["validate"]:
        raise SystemExit(f"the slip does not say validate was done: {resume.done}")
    if resume.next is None or resume.next.name != "charge":
        raise SystemExit(f"the resumed run does not start at charge: {resume.next}")
    if envelope.route_position != 1:
        raise SystemExit(f"the route position is {envelope.route_position}, not 1")
    if ran != ["validate", "charge", "charge", "ship"]:
        raise SystemExit(f"the run did not resume where it stopped: {ran}")


if __name__ == "__main__":
    asyncio.run(main())
