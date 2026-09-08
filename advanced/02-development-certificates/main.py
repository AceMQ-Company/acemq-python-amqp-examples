"""A TLS broker on a laptop, and the reason its certificates cannot reach production.

Every developer who has needed TLS locally has generated a self-signed
certificate and then had to turn verification off to use it. The certificate
works, so it stays; the flag that made it work stays too; and the two of them
travel together into an environment where the flag is a way to hand every
message and every password to whoever answers on the address in the URL.

`acemq_amqp.devcerts` writes an authority, a broker certificate, a client
certificate and a matching `rabbitmq.conf` — the same file names Go's
`acemq-certs` writes. Everything it produces is stamped **ACEMQ DEVELOPMENT ONLY
- DO NOT TRUST** in its subject organisation, and this library refuses any
certificate carrying it, however trust is configured. That is not a warning in a
docstring; it is the mechanism, and it is why the flag that lets these through
is a named argument a reviewer will see rather than a `verify=False`.

Before this runs, the broker has to be holding the certificates:

    python -m acemq_amqp.devcerts --directory certs --broker localhost
    chmod 644 certs/server.key
    docker compose --profile tls up -d
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

from acemq_amqp import (
    Ack,
    Envelope,
    Message,
    Security,
    SecurityError,
    Topology,
    accept,
    connect,
    devcerts,
    is_development_certificate,
)

TLS_URL = os.environ.get("ACEMQ_TLS_URL", "amqps://guest:guest@localhost:5671/")
PLAIN_URL = os.environ.get("ACEMQ_URL", "amqp://guest:guest@localhost:5672/")

QUEUE = "py-tls.audit"

# The certificates the broker was started with. Regenerating them here would
# leave the running broker holding the old ones, which is a TLS handshake
# failure that takes a while to work out.
CERTIFICATES = Path(os.environ.get("ACEMQ_CERTS", "certs"))


def show_what_the_generator_writes() -> None:
    """Generate a set into a throwaway directory, purely to look at them."""
    with tempfile.TemporaryDirectory() as scratch:
        written = devcerts.generate(scratch, broker_host="localhost", validity_days=7)
        for path in written.files:
            mode = oct(path.stat().st_mode & 0o777)[2:]
            print(f"  {path.name} {mode}")
        print(f"  valid until {written.expires:%Y-%m-%d}, stamped {written.marker!r}")
        # The private keys are 0600. Worth knowing before mounting them into a
        # container that runs as somebody else: RabbitMQ cannot read a key it
        # does not own, and reports it as a listener that failed to start.
        print(f"  the broker configuration is at {written.broker_configuration.name}")


async def main() -> None:
    print("what the generator writes:")
    show_what_the_generator_writes()

    authority = CERTIFICATES / "ca.crt"
    print(f"a development certificate: {is_development_certificate(authority.read_bytes())}")

    # The way through, for the one place it belongs. Naming an authority
    # *replaces* the system trust store rather than adding to it, which is the
    # point: a broker holding a certificate from a public authority is not your
    # broker.
    development = Security(
        certificate_authority=authority,
        allow_development_certificates=True,
    )

    async with await connect(TLS_URL, security=development) as mq:
        await mq.declare(Topology().queue(QUEUE, dead_letter=True))

        received: asyncio.Queue[Message] = asyncio.Queue()

        async def audit(message: Message) -> Ack:
            await received.put(message)
            return accept()

        consumer = await mq.consume(QUEUE, audit)
        await mq.publisher(routing_key=QUEUE).send(
            {"event": "signed in"}, envelope=Envelope(type="Audit")
        )
        message = await asyncio.wait_for(received.get(), timeout=10)
        print(f"over TLS: {message.payload}")

        await consumer.close()
        await mq.delete_queue(QUEUE)
        await mq.delete_queue(f"{QUEUE}.dlq")
        await mq.delete_queue(f"{QUEUE}.parked")

    # The same connection without the flag. This is what would happen if these
    # certificates reached an environment nobody meant them to reach.
    try:
        await connect(TLS_URL, security=Security(certificate_authority=authority))
        print("a development certificate was accepted, which would defeat the whole marker")
    except SecurityError as refused:
        print(f"without the flag: {refused}")

    # And TLS settings against a plaintext URL, which is refused rather than
    # ignored: a service handed an authority, connected in plaintext and
    # reporting success is the failure this module exists to prevent.
    try:
        await connect(PLAIN_URL, security=Security(certificate_authority=authority))
        print("TLS settings were ignored on an amqp:// URL, which is the quiet failure")
    except SecurityError as refused:
        print(f"on an amqp:// URL: {refused}")


if __name__ == "__main__":
    asyncio.run(main())
