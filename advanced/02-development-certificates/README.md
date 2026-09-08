# Development certificates and TLS

A TLS broker on a laptop, and the reason its certificates cannot reach
production.

> Needs the library's **main** branch — `acemq_amqp.devcerts` landed after
> 0.3.0, and it needs the `[crypto]` extra.

This is the one example that needs a broker of its own, because it needs a TLS
listener holding certificates this repository generated:

```bash
.venv-main/bin/python -m acemq_amqp.devcerts --directory certs --broker localhost
chmod 644 certs/server.key
docker compose --profile tls up -d
.venv-main/bin/python advanced/02-development-certificates/main.py
```

The `chmod` is worth understanding rather than copying: the generator writes
private keys `0600`, which is right for a key and wrong for a container that
runs as another user. RabbitMQ reports an unreadable key as a listener that
failed to start, which is a long way from what it is.

## What to look for

**The seven files and their modes.** An authority, a broker certificate, a
client certificate and a `rabbitmq.conf` pointing the broker at them — the same
file names Go's `acemq-certs` writes, so it is a drop-in replacement for it. The
example generates a set into a throwaway directory purely to show them; the ones
it connects with are the ones the broker was started on, because regenerating
them under a running broker is a handshake failure that takes a while to work
out.

**`a development certificate: True`.** Everything the generator writes carries
`ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its subject organisation, and
`is_development_certificate` reads it back out of the PEM.

**`without the flag: acemq: the certificate authority … is marked …`.** This is
the example. The library refuses any certificate carrying that marker, **however
trust is configured** — `without_verifying_the_broker()` included. It is not a
warning in a docstring; it is the mechanism. A generated authority's private key
sits next to its certificate and usually ends up in a repository, so a
development certificate that could reach production would be an authority
anybody who can read that repository can issue against, and the connection would
succeed.

The way through is `Security(allow_development_certificates=True)` — a named
argument a reviewer will see, and one more thing to `grep` for in a deployed
configuration. Java, Go and .NET stamp the same string and enforce it the same
way.

**`on an amqp:// URL: acemq: this Security configures TLS but the URL is
amqp://`.** TLS settings against a plaintext URL are **refused rather than
ignored**. A service that was handed a certificate authority, connected in
plaintext and reported success is the failure the whole module exists to
prevent.

## Naming an authority replaces the trust store

`Security(certificate_authority=...)` does not add to the machine's trust store,
it replaces it. That is the point: a broker holding a certificate from a public
authority is not your broker, and the several hundred authorities a machine
trusts by default are several hundred ways to be wrong. A self-signed broker
certificate works here too — put the broker's own certificate in the file and it
becomes the one authority you trust.

## What is not here

`without_verifying_the_broker()` is not used in this example and should not be
used anywhere. It encrypts the traffic and checks nothing at all about who is on
the other end, so somebody who can answer on the address in your URL receives
every message you publish and every password you log in with — over a connection
that looks encrypted in every log and every metric. The alternative is the one
line above.
