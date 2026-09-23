# AceMQ for Python — examples

[![ci](https://github.com/AceMQ-Company/acemq-python-amqp-examples/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-python-amqp-examples/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-python-amqp-examples/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-python-amqp-examples/actions/workflows/attribution-guard.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB)](#requirements)

Runnable examples for [AceMQ for Python](https://github.com/AceMQ-Company/acemq-python-amqp).
Each one is a single `main.py`: open a directory and the whole example is in
front of you, with no shared helpers to trace, and a `README.md` beside it saying
what to look for while it runs.

Every one of them talks to a real broker, and CI runs all twenty-one on every push.
That matters more in Python than in a compiled language: there is no compiler to
notice a renamed argument, so an example nobody runs is an example nobody knows
is broken.

## Running one

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
docker compose up -d
.venv/bin/python basic/01-publish-and-consume/main.py
```

Point them somewhere else with `ACEMQ_URL`:

```bash
ACEMQ_URL=amqps://guest:guest@broker:5671/ .venv/bin/python basic/01-publish-and-consume/main.py
```

Each example declares the queues it needs, deletes them on the way out, and uses
names nothing else uses. Run any of them twice: the second run should report
exactly what the first did.

One thing does stay: `intermediate/05-scheduler` leaves the six
`acemq.schedule.*` queues behind, because they are not its queues to delete.
They are the library's retry ladder, declared identically by Java, Go, .NET and
Ruby, and shared with every other service scheduling on the same broker — an
example that tidied them away on exit would be throwing out somebody else's
pending deliveries along with its own. They come back empty, so a second run
still reports what the first did.

## Where the library comes from

`requirements.txt` resolves the **released** package, `acemq-amqp==0.7.1`, from
<https://acemq.org/pypi/> — a static PEP 503 index, no account and no
credential, each link carrying the `sha256` pip verifies before installing. It
is where the documentation tells you to get the library, so it is where the
examples get it, and an example that stops working against a release is a red
build here rather than a surprise for whoever copies it.

All twenty-one resolve it. For a while six of them could not: the optional codecs,
encrypted bodies, development certificates, the saga, the scheduler and the
OpenTelemetry adapter all landed after 0.3.0 was cut, so those six installed the
library's `main` branch from a second requirements file and CI ran them under a
second interpreter. 0.5.0 carries every one of them, so the second file, the
second interpreter and the list that decided between them are gone. One
`requirements.txt`, one `.venv`, and nothing here is proving anything about code
a reader cannot install.

The extras are named in `requirements.txt` rather than dragged in: the library
core has no dependencies at all, and an examples repository that opens sockets
and reaches for every codec has to say so. `opentelemetry-sdk` is there for the
same reason — the library depends on the OpenTelemetry *API* alone and exports
nothing without an SDK, because an application's telemetry stack is the
application's decision.

## What is here

### basic

| | |
|---|---|
| [01-publish-and-consume](basic/01-publish-and-consume) | A durable queue, a confirmed publish, and a consumer that says what it did. |
| [02-retries-and-dead-letters](basic/02-retries-and-dead-letters) | The attempt counter moving, a message giving up, and a fatal error skipping the wait. |
| [03-topology-and-drift](basic/03-topology-and-drift) | Printing a topology before applying it, and a broker that refuses a service whose idea of a queue has moved on. |
| [04-replay](basic/04-replay) | Dead-lettered invoices put back one tenant at a time, and the rest afterwards. |
| [05-serialization](basic/05-serialization) | JSON, YAML, TOML, XML, Avro and protobuf on one queue, read by one consumer. |
| [06-streams](basic/06-streams) | Six readings written once and read three times, from three different places. |
| [07-pipelines](basic/07-pipelines) | An order carried through three services by an itinerary, and a failed run resumed where it stopped. |

### intermediate

| | |
|---|---|
| [01-request-reply](intermediate/01-request-reply) | Ten concurrent questions, each getting its own answer, and a responder failure reaching the caller. |
| [02-idempotent-consumer](intermediate/02-idempotent-consumer) | One payment delivered four times and charged once — and still once after a restart. |
| [03-transactional-outbox](intermediate/03-transactional-outbox) | The message and the work in one transaction, and a relay publishing what was committed. |
| [04-interceptors](intermediate/04-interceptors) | A tenant on every message and every handler timed, without either appearing in a handler. |
| [05-scheduler](intermediate/05-scheduler) | Deliver this later — and the long one does not hold up the short one. |
| [06-saga](intermediate/06-saga) | Three systems, no shared transaction, and what is left when a compensation itself fails. |
| [07-claim-check](intermediate/07-claim-check) | A half-megabyte payload put in a store, 39 bytes on the wire, and the small one still travelling inline. |
| [08-consumer-groups](intermediate/08-consumer-groups) | Four slow invoices, handled four times faster by four consumers than by one. |
| [09-schema-evolution](intermediate/09-schema-evolution) | Two services on two versions of one schema, talking to each other anyway. |

### advanced

| | |
|---|---|
| [01-encrypting-payloads](advanced/01-encrypting-payloads) | Message bodies the broker cannot read, and a keyring that can rotate. |
| [02-development-certificates](advanced/02-development-certificates) | A TLS broker on a laptop, and the reason its certificates cannot reach production. |
| [03-metrics-and-health](advanced/03-metrics-and-health) | `/acemq-metrics`, `/acemq-health` and `/acemq-info`, on the same paths as Java, Go and .NET. |
| [04-tracing](advanced/04-tracing) | A consumer's span joined to the publish that caused it, minutes and processes apart. |
| [05-blocked-broker](advanced/05-blocked-broker) | A real memory alarm, and a health check that reports `up` in microseconds rather than `down` in three seconds. |

## The two that need a broker of their own

`advanced/05-blocked-broker` provokes a genuine memory alarm with
`rabbitmqctl set_vm_memory_high_watermark 0`. An alarm is broker-wide, so on the
shared broker it would stop every other example publishing as well — it gets
`blocked-broker` on 5673 instead, which `docker compose up -d` brings up with the
rest. Nothing has to be generated first, and the example puts the watermark back
in a `finally`.

`advanced/02-development-certificates` needs a TLS listener holding certificates
this repository generated, so it is the one example that does not run against
`docker compose up -d` alone:

```bash
.venv/bin/python -m acemq_amqp.devcerts --directory certs --broker localhost
chmod 644 certs/server.key
docker compose --profile tls up -d
.venv/bin/python advanced/02-development-certificates/main.py
```

The `chmod` is not a workaround to skip past. The generator writes private keys
`0600`, which is right for a key and wrong for a container that runs as another
user, and RabbitMQ reports an unreadable key as a listener that failed to
start — a long way from what it is.

## Three things worth knowing before reading any of them

**`connect` is asynchronous, and there is a blocking one.** Everything here uses
`async with await connect(url)`, because that is what the library is. A program
that is not running an event loop uses `acemq_amqp.sync.connect`, which is a
facade over the same engine rather than a second implementation — a loop runs on
a thread of its own, handlers run on a worker thread, and the envelope rules and
retry arithmetic are the ones above rather than a copy that can drift.

**`message.envelope.attempt` is the consumer's count, not the publisher's.** The
envelope carries what the publisher wrote, and a broker redelivering the original
bytes hands back a header that reads 1 for ever. A retry is *republished* rather
than requeued, which is what makes the count advance and what makes a retry limit
mean anything. `basic/02` prints `[1, 2, 3]`.

**A durable queue is a quorum queue.** `Topology().queue(...)` declares
`x-queue-type: quorum` unless told otherwise, because Java has declared quorum
since it had deployments and two services that disagree about a queue's type
cannot both consume it. The exceptions — retry rungs, dead-letter and parked
queues, and anything exclusive or auto-deleting — are declared classic without
being asked, because RabbitMQ refuses a quorum queue that is any of those.

## Requirements

Python 3.10 or newer — the library's floor, and what CI runs — and Docker.
RabbitMQ **3.13 or 4.x**, the range the library supports; `compose.yaml` brings
up 4.x and CI runs every example against both.

## How these stay honest

CI **runs every example against a real broker**, on every push and once a week,
on the oldest Python the library supports and against both broker majors the
library supports — 3.13 and 4.x. Then it runs them all a second time, which is
what catches an example depending on its own leftovers: every one of these
deletes the queues it declared — the shared scheduler ladder above excepted —
and the way that stops being true is silent.

Both brokers rather than the newest is a deliberate cost. `basic/03` prints a
`PRECONDITION_FAILED` straight from the broker, and 3.13 and 4.x word that
differently — the sort of difference an examples repository exists to find
before a reader does.

The workflow finds examples rather than listing them, so one added without
touching CI is still run — and it fails if it finds fewer than it expects, since
a `find` that matches nothing would otherwise pass having run nothing at all. It
also fails if an example has no `README.md`, because the code says what it does
and the README is where it says what to look for.

Lint is `ruff check` with the library's own settings, from `pyproject.toml`. An
example formatted to one house style and copied into a project held to another
arrives with work attached.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
