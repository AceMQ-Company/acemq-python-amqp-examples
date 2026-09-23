"""A broker that has run out of memory, and a health check that answers anyway.

RabbitMQ protects itself. When it crosses its memory or disk high watermark it
raises an alarm and stops reading from every connection that publishes:
`connection.blocked` goes out, and from then on a publish, a declaration, or
anything else written to that socket simply waits. Nothing fails. Nothing
arrives either.

That state is the one a readiness probe gets wrong, because from this end it
looks exactly like a broker that has gone away — and the two want opposite
responses. A blocked broker is one to wait for; a dead one is one to fail over
from. An application that fails its own readiness check on a block is one an
orchestrator restarts into the same blocked broker, having thrown away whatever
it was holding, and doing that to every replica at once turns a broker under
memory pressure into an outage with a crash loop on top.

So a blocked connection is reported **up**, with the reason written on it.
`parts["blocked"]` carries the state, a dashboard shows it, an alert rule
matches it, and nothing is taken out of rotation for it. That is a change of
behaviour rather than a new feature: up to 0.6.0 the same connection reported
`degraded`.

The other half of the claim is the timing. `health()` normally proves the
connection by asking the broker something, and a round trip is precisely what a
blocked broker will not complete — an unbounded check would spend its whole
three-second deadline arriving at `down` for a broker that is up and talking. So
the block is read first, off the notification the broker already sent, and the
probe is skipped entirely. What would take three seconds to time out takes
microseconds to answer, and the numbers this prints are the ones the run
measured.

**This example has a broker to itself**, and it is the only one here that does
besides the TLS example. A memory alarm is broker-wide: dropping the watermark
on the shared broker would block every other example along with this one. The
watermark is put back in a `finally`, because a broker left with an alarm on it
is a broker the next run inherits — and an example that leaves the machine worse
than it found it is not one to copy.

What to watch: `blocked` goes false, true, false; the status stays `up`
throughout; and the report made while blocked carries no `round-trip`, because
there was not one.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from typing import NamedTuple

from acemq_amqp import (
    BLOCKED_DETAIL,
    HealthReport,
    HealthStatus,
    Topology,
    connect,
)

URL = os.environ.get("ACEMQ_BLOCKED_URL", "amqp://guest:guest@localhost:5673/")
# An alarm is the broker's decision about the broker's memory, so nothing this
# process can send will raise one. Provoking it means reaching the broker the
# way an operator would, which is `rabbitmqctl` inside its container.
CONTAINER = os.environ.get("ACEMQ_BLOCKED_CONTAINER", "acemq-python-examples-blocked-broker")

QUEUE = "py-blocked.orders"

# Only a fallback: the watermark in force is read off the broker before the
# alarm and put back afterwards, because writing a number down here would be
# wrong on half the brokers this repository runs against. RabbitMQ 3.13 defaults
# to 0.4 and 4.x to 0.6, and CI runs every example against both. When the broker
# will not say, the lower of the two is the safer guess — a broker restored too
# low is merely cautious, one restored too high has stopped protecting itself.
DEFAULT_WATERMARK = "0.4"


class Timed(NamedTuple):
    """A health report and how long it took to arrive at."""

    report: HealthReport
    seconds: float


async def main() -> None:
    watermark = _watermark()

    async with await connect(URL, origin="orders@example") as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))
        publisher = mq.publisher(routing_key=QUEUE)

        # One publish before the alarm, and not only to have something on the
        # queue. RabbitMQ blocks the connections that publish and leaves the
        # ones that only consume alone, so a connection that had never
        # published would sit through the entire alarm correctly reporting
        # itself unblocked — which is true, and not what this is about.
        await publisher.send({"order": "before the alarm"})

        before = await _health(mq)
        _show("before the alarm", before)

        print(f"\nsetting the memory high watermark to 0 on {CONTAINER}")
        _rabbitmqctl("set_vm_memory_high_watermark", "0")

        stalled: list[asyncio.Task[None]] = []
        try:
            # Fired rather than awaited, which is the whole shape of the
            # problem. A publish on a blocked connection does not fail, it
            # waits — so awaiting one here would wait for the alarm this loop
            # exists to observe. They are collected and settled further down,
            # once the socket is being read again.
            deadline = time.monotonic() + 30
            while not mq.blocked and time.monotonic() < deadline:
                order = {"order": f"during the alarm {len(stalled)}"}
                stalled.append(asyncio.ensure_future(publisher.send(order)))
                await asyncio.sleep(0.25)

            if not mq.blocked:
                raise SystemExit(
                    f"the broker never blocked the connection after {len(stalled)} publishes; "
                    f"check that {CONTAINER} is the broker {URL} points at"
                )

            sent = f"{len(stalled)} publish" + ("" if len(stalled) == 1 else "es")
            print(
                f"blocked after {sent}; "
                f"{mq.outstanding_publishes} still waiting for a confirm"
            )
            during = await _health(mq)
            _show("while blocked", during)
            # aio-pika reads RabbitMQ's reason and logs it — "was blocked by:
            # 'low on memory'" is its line, not this example's — but it keeps
            # no accessor for it, so the library has nothing to report. That is
            # why the detail above is the shared sentence and nothing more:
            # inventing a reason here would read exactly like one the broker
            # sent. Java, Go, Ruby and .NET all print the broker's own words.
            print(f"  the broker's reason, as this library can see it: {mq.blocked_reason}")
        finally:
            # In a `finally` and not at the end of the happy path. Every
            # assertion below is about a broker under an alarm, so every one of
            # them is a way to leave this function with the alarm still on —
            # and the next thing to use this broker would then find it blocked
            # for a reason nothing in its own output explains.
            print(f"\nputting the watermark back to {watermark}")
            _rabbitmqctl("set_vm_memory_high_watermark", watermark)

        # The alarm clears on the broker's next memory reading rather than on
        # the command returning, and `connection.unblocked` arrives after that.
        clear_by = time.monotonic() + 30
        while mq.blocked and time.monotonic() < clear_by:
            await asyncio.sleep(0.25)

        # The publishes that were waiting are confirmed now, which is the point
        # of a block not being an error: they were never lost, only paused.
        settled = await asyncio.gather(*stalled, return_exceptions=True)
        failed = [answer for answer in settled if isinstance(answer, BaseException)]
        print(f"{len(settled) - len(failed)} of {len(settled)} stalled publishes went through")
        for answer in failed[:1]:
            print(f"  and one did not: {answer}")

        after = await _health(mq)
        _show("after the alarm cleared", after)

        await publisher.send({"order": "after the alarm"})
        print(f"\non the queue: {await mq.message_count(QUEUE)}")

        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    if before.report.status is not HealthStatus.UP:
        raise SystemExit(f"the connection was not up before the alarm: {before.report.status}")
    if before.report.parts["blocked"] is not False:
        raise SystemExit(f"blocked was {before.report.parts['blocked']!r} before the alarm")
    if "round-trip" not in before.report.parts:
        raise SystemExit("the report before the alarm did not make a round trip")

    # The behaviour change itself. Reported `degraded` up to 0.6.0, and a
    # readiness endpoint keyed on `up` would have failed on a broker that was
    # doing exactly what it is supposed to do under pressure.
    if during.report.status is not HealthStatus.UP:
        raise SystemExit(
            f"a blocked connection reported {during.report.status.value}, not up"
        )
    if during.report.parts["blocked"] is not True:
        raise SystemExit(f"blocked was {during.report.parts['blocked']!r} under the alarm")
    # The exact sentence, because it is the same sentence in Java, Go, Ruby and
    # .NET: one alert rule reads a blocked broker whatever the service is
    # written in, so the wording is a contract rather than a phrasing.
    if not during.report.detail.startswith(BLOCKED_DETAIL):
        raise SystemExit(f"the blocked report does not say why: {during.report.detail!r}")
    if "round-trip" in during.report.parts:
        raise SystemExit(
            "the blocked report made a round trip, which a blocked broker cannot answer"
        )
    # Generous next to the microseconds this actually takes, and deliberately
    # so: what it is here to catch is a check that went back to probing a
    # blocked broker, and that one costs the full three-second deadline.
    if during.seconds > 0.25:
        raise SystemExit(
            f"the blocked report took {during.seconds:.3f}s, so it waited on something"
        )

    if after.report.status is not HealthStatus.UP:
        raise SystemExit(f"the connection did not recover: {after.report.status}")
    if after.report.parts["blocked"] is not False:
        raise SystemExit(
            f"blocked was {after.report.parts['blocked']!r} after the alarm cleared"
        )
    if "round-trip" not in after.report.parts:
        raise SystemExit("the report after the alarm did not make a round trip")


async def _health(mq) -> Timed:  # type: ignore[no-untyped-def]
    """One health check, timed.

    Timed here rather than inside the library because it is the caller's
    question: a readiness endpoint's budget is the probe's timeout, and what
    matters is how long the answer took to reach the endpoint.
    """
    started = time.perf_counter()
    report = await mq.health()
    return Timed(report, time.perf_counter() - started)


def _show(label: str, timed: Timed) -> None:
    report = timed.report
    print(f"{label}: {report.status.value} in {timed.seconds * 1_000_000:.0f}us")
    if report.detail:
        print(f"  {report.detail}")
    for name, value in report.parts.items():
        print(f"  {name}: {value}")


def _watermark() -> str:
    """What the broker's memory high watermark is now, to put back afterwards.

    `rabbitmqctl eval` rather than parsing `status`, because the value wanted is
    one term and `status` is two hundred lines of report around it. A broker
    that will not answer is not worth guessing at either, so the documented
    default stands in and the run goes on — the alternative is an example that
    refuses to start over the value it would have restored.
    """
    try:
        answer = _rabbitmqctl("eval", "vm_memory_monitor:get_vm_memory_high_watermark().")
    except SystemExit:
        raise
    except Exception:
        return DEFAULT_WATERMARK
    return answer or DEFAULT_WATERMARK


def _rabbitmqctl(*arguments: str) -> str:
    """One `rabbitmqctl` call, inside the broker's own container.

    `-u rabbitmq` and the `HOME` are not decoration. rabbitmqctl reaches the
    server over Erlang distribution and authenticates with the cookie under
    `HOME`; run as root it reads root's, is answered `Invalid challenge reply`,
    and fails against a broker that is working perfectly.
    """
    command = [
        "docker", "exec", "-u", "rabbitmq", "-e", "HOME=/var/lib/rabbitmq",
        CONTAINER, "rabbitmqctl", *arguments,
    ]
    try:
        done = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    except FileNotFoundError:
        raise SystemExit(
            "docker is not on PATH, and this example provokes a memory alarm with rabbitmqctl "
            "inside the broker's container"
        ) from None
    if done.returncode != 0:
        raise SystemExit(
            f"`rabbitmqctl {' '.join(arguments)}` failed in {CONTAINER}: "
            f"{(done.stderr or done.stdout).strip()}\n"
            "Start this example's own broker with `docker compose up -d`, or name another "
            "with ACEMQ_BLOCKED_CONTAINER and ACEMQ_BLOCKED_URL."
        )
    return done.stdout.strip()


if __name__ == "__main__":
    asyncio.run(main())
