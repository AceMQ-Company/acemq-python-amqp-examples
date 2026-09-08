"""`/acemq-metrics`, `/acemq-health` and `/acemq-info`, on the same paths as Java, Go and .NET.

How much is going through, how much is failing and how long a handler takes are
numbers nobody outside the process can see. Whether the connection is really up
— as opposed to a socket that is open and wedged — is a question only something
holding the connection can ask. So the library answers both, through a
three-method `Observer` and a health check, and takes no dependency on a metrics
client to do it.

It does not ship an HTTP server, because a library that opened a port would be a
library that opened a port in every process that imported it. Wiring one up is
the twenty lines below, and the paths are the ones the other three libraries
use so that one scrape configuration reads all four.

What to watch: the counters move, the handler duration has a distribution rather
than an average, and the health report distinguishes *down* from *degraded* —
a consumer whose workers have died is a connection that works perfectly and a
queue nothing is reading.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import urllib.request
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from acemq_amqp import (
    Ack,
    BrokerHealth,
    Envelope,
    Message,
    Metrics,
    Topology,
    accept,
    aggregate_health,
    connect,
    prometheus_text,
    retry,
)
from acemq_amqp.prometheus import PrometheusObserver
from prometheus_client import CollectorRegistry, generate_latest

URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")
PORT = int(os.environ.get("ACEMQ_HTTP_PORT", "9464"))

QUEUE = "py-metrics.events"


class Both:
    """An observer is three methods, so an application can have two of them.

    `Metrics` keeps the numbers in memory and renders a scrape body with nothing
    installed at all; `PrometheusObserver` writes into the registry the rest of
    the application already exports. Neither is privileged — everything the
    library reports goes through this interface.
    """

    def __init__(self, *observers: object) -> None:
        self._observers = observers

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        for observer in self._observers:
            observer.count(metric, delta, labels)  # type: ignore[attr-defined]

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        for observer in self._observers:
            observer.gauge(metric, value, labels)  # type: ignore[attr-defined]

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        for observer in self._observers:
            observer.observe(metric, seconds, labels)  # type: ignore[attr-defined]


async def main() -> None:
    metrics = Metrics()
    registry = CollectorRegistry()
    observer = Both(metrics, PrometheusObserver(registry))

    async with await connect(URL, origin="events@example", observer=observer) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        loop = asyncio.get_running_loop()
        serving = _serve(mq, registry, loop)

        handled = 0

        async def project(message: Message) -> Ack:
            nonlocal handled
            await asyncio.sleep(0.01 * (handled % 3))
            handled += 1
            if message.payload["event"] == "broken":
                return retry(RuntimeError("the projection store is not answering"))
            return accept()

        consumer = await mq.consume(QUEUE, project)

        publisher = mq.publisher(routing_key=QUEUE)
        for number in range(10):
            event = "broken" if number == 7 else f"event-{number}"
            await publisher.send({"event": event}, envelope=Envelope(type="Projected"))

        while handled < 10:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        print("counters:")
        for name, value in sorted(metrics.counts.items()):
            print(f"  {name} {value}")
        # A distribution rather than an average, because the handler that takes
        # thirty seconds once an hour is invisible in a mean.
        for name, duration in sorted(metrics.durations.items()):
            print(
                f"  {name}: {duration.count} samples, "
                f"{duration.fastest * 1000:.0f}ms fastest, "
                f"{duration.mean * 1000:.0f}ms mean, {duration.slowest * 1000:.0f}ms slowest"
            )

        print(f"health: {(await mq.health()).status.value}")
        # Combined with the application's own checks, worst wins, and they run
        # at once under a deadline — a probe that hangs is a pod that never
        # comes back.
        combined = await aggregate_health(BrokerHealth(mq), _ProjectionStore())
        parts = {name: report.status.value for name, report in combined.parts.items()}
        print(f"aggregate: {combined.status.value} {parts}")

        for path in ("/acemq-metrics", "/acemq-health", "/acemq-info"):
            # On a worker thread, because /acemq-health hands its work back to
            # this loop: a blocking request made from the loop itself waits for
            # a loop that is waiting for the request.
            body = await asyncio.to_thread(_get, f"http://127.0.0.1:{PORT}{path}")
            first = body.decode().strip().splitlines()[0]
            print(f"GET {path} -> {len(body)} bytes, first line: {first}")

        # The same numbers with nothing installed, which is what the library
        # promises: the metrics client is an extra, exactly as the broker
        # client is.
        body = prometheus_text(metrics).splitlines()
        lines = [line for line in body if not line.startswith("#")]
        print(f"prometheus_text with nothing installed: {len(lines)} sample lines")

        serving.shutdown()
        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")


def _get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=5) as answer:
        body: bytes = answer.read()
    return body


class _ProjectionStore:
    """One of the application's own checks, to show that they aggregate."""

    name = "projections"

    async def check(self):  # type: ignore[no-untyped-def]
        from acemq_amqp import HealthReport, HealthStatus

        return HealthReport(status=HealthStatus.UP, detail="12 projections up to date")


def _serve(mq, registry, loop) -> ThreadingHTTPServer:  # type: ignore[no-untyped-def]
    """The twenty lines that turn the library's answers into endpoints."""

    class Handler(BaseHTTPRequestHandler):
        # The name is http.server's, not a choice.
        def do_GET(self) -> None:
            if self.path == "/acemq-metrics":
                self._reply(200, "text/plain; version=0.0.4", generate_latest(registry))
            elif self.path == "/acemq-health":
                # The health check is a coroutine and this is a thread, so it
                # is handed to the loop rather than run here.
                report = asyncio.run_coroutine_threadsafe(mq.health(), loop).result(10)
                body = json.dumps({"status": report.status.value, "detail": report.detail})
                self._reply(200 if report.healthy else 503, "application/json", body.encode())
            elif self.path == "/acemq-info":
                body = json.dumps({"origin": mq.origin, "consumers": len(mq.consumers)})
                self._reply(200, "application/json", body.encode())
            else:
                self._reply(404, "text/plain", b"not found\n")

        def _reply(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("content-type", content_type)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


if __name__ == "__main__":
    asyncio.run(main())
