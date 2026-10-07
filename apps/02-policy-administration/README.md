# Policy administration

One deployable, six modules, one database, and no module that imports another.

```bash
.venv/bin/python apps/02-policy-administration/main.py
```

[apps/01](../01-order-fulfilment) is five services that cannot call each other
because a network is in the way. This is the same discipline with the network
removed: the modules run in one process, share one connection and one SQLite
database, and still communicate only by publishing events. It is the Python port
of the Java examples' `apps/02-policy-administration`, with the same exchange,
queues, routing keys and payloads — close enough that the two halves can be
mixed, which has been tried (below).

**A modular monolith is not a step towards microservices.** It is a different
answer to the same question: module boundaries without distributed transactions,
and one database you can actually join across.

## The flow

```mermaid
flowchart LR
    B["broker submits"] --> P["policies<br/>applications + outbox<br/>one transaction"]
    P -->|application.submitted| U["underwriting<br/>route: register → price → decide"]
    U -->|application.accepted| P
    U -->|application.declined| A
    P -->|policy.issued| BI["billing<br/>idempotent premium"]
    P -->|policy.issued| C["claims"]
    C -.->|"asks: is it in force?"| P
    D["documents<br/>claim check"] -->|document.stored| A["audit<br/>policy.#"]
    BI -->|premium.charged| A
```

The dotted line is the only one that is not an event: claims **asks** policies a
question and waits for the answer.

| File | The pattern | Why it lives there |
|---|---|---|
| `policies.py` | Transactional outbox, and a responder | One database does *not* remove the dual write: the two systems that must agree are this database and the broker |
| `underwriting.py` | A declared route, a queue per stage | The one genuinely sequential part. A slow stage is a deep queue you can point at |
| `documents.py` | Claim check | A scanned medical report is tens of megabytes. The store gets the bytes; the message gets the key |
| `billing.py` | Shared idempotency store | The only module where handling a message twice is money |
| `claims.py` | Request/reply, with a timeout | Needs an answer *now*. Asks over the broker even though the callee is in the same process |
| `contracts.py` | Topic wildcard (`policy.audit`) | Names, event shapes, the topology. The audit queue is bound to `policy.#` and has no code |
| `main.py` | — | Starts all of it on one connection and checks what it claims |

## What to look for

`main.py` runs six scenarios, each against a freshly started application:

```
pipeline underwriting: register (look the applicant up on the shared industry register) | price (...) | decide (...)
the happy path
  POL-1ec86abb: issued and charged 120
referred above the limit
  referred: no policy, no charge
claims ask rather than read
  POL-08ef0fb5: settled; POL-does-not-exist: rejected
documents travel by reference
  doc/POL-4799a4ab/medical-report/830d9b5f: 4 MiB stored, 131 bytes on the wire
billing is idempotent
  POL-ce20519d: three copies, 1 extra charge
a lookup that never answers decides nothing
  could not establish whether POL-1fc285f9 is in force, so claim CLM-22cb47c6 was neither settled nor rejected; it must be retried
all 6 held
```

The first five are the Java system test's five cases with its assertions: the
premium is 120 (100 base plus 20 for age), a referral produces no policy and no
charge, a claim against a real policy settles and one against a made-up policy is
rejected, a 4 MiB document is redeemed by its key, and three copies of one
`PolicyIssued` charge once. Two things go further than Java:

- **The claim check is checked on the wire.** Java asserts the key names the
  policy. Here the `DocumentStored` event is pulled off the audit queue and its
  body measured: 131 bytes for a 4 MiB document.
- **The lookup timeout is exercised.** Policies' responder is closed and a claim
  submitted. It raises `ClaimUndecidedError` after five seconds, neither counter
  moves, and the requester counts one timeout. A lookup that did not answer is
  not a "no".

Any broken claim prints what was expected and what happened, and the run exits 1.

## What porting it found

**Python's publishers are not mandatory by default; Java's are.** The Java
README's best story is that `claim.settled` and `document.stored` were published
with nothing bound to them, and the library refused rather than losing them —
that is why the audit queue exists. Java's `PublishOptions.defaults()` is
mandatory. Python's `mq.publisher(...)` is not: the same mistake here succeeds
and the broker drops the message, with `result.routed` false and nobody reading
it. Every publisher in this app passes `mandatory=True` so the Java behaviour
holds. The default itself is a cross-library decision and has been reported
rather than changed.

**A handler that raised inside `idempotent()` kept its claim.** Fixed in the
library (`acemq-python-amqp` 7eea77d, not yet released). Only a handler that
*returned* a retry released the key; one that raised — the usual way a Python
handler fails — left it claimed, so the retry found it "in progress" and the
message was put back without running until the claim aged out: an hour for the
in-memory store, five minutes for the SQL one. A probe with one transient
failure ran its handler once in 15 s instead of twice in 0.16 s. Java releases
the claim on an exception. Billing here does not raise, so the app is correct on
0.7.8; a billing handler that hit a database blip would have stalled.

**There is no `Pipeline` object.** Java's `mq.pipeline("underwriting", ...)`
becomes `route_of("underwriting", "register", "price", "decide")`, a
`follow_slip` consumer per stage, and the topology Java's pipeline declares: a
direct exchange named `underwriting`, quorum queues `underwriting.<step>` bound
on the step name, and the register stage's retry rungs. The step descriptions
Java logs at start-up are printed by `main.py`. One thing has no equivalent: a
Java step that returns `null` before the last stop ends the run early and counts
it as `ended_early`. A Python routing-slip step cannot — returning `None` sends
`null` to the next stage, and returning `NOTHING` dead-letters the message
because it cannot be encoded. The library documents the gap
(`OUTCOME_ENDED_EARLY`). Underwriting ends every run at its last stage, so it
does not arise here.

## Mixing it with the Java half

Checked against RabbitMQ 4 with the Java app on acemq-java-amqp 0.7.11 and this
library at 0.7.8, sharing one vhost:

- Java policies, billing and claims with Python underwriting. Java's outbox
  `ApplicationSubmitted` went through the three Python stages, came back as
  `ApplicationAccepted`, and Java issued the policy at Python's price of 120 and
  charged it once. Java's claims settled against it and rejected a made-up
  policy, and Python's claims asked Java's responder and got both answers.
- Python policies and billing with Java underwriting and claims. Python's
  `ApplicationSubmitted` went through Java's pipeline. The accepted one was
  issued and charged at 120 in Python, the referral stopped in Java, and Java's
  claims settled against the Python-issued policy using Python's answer.
- Each side declared the topology on top of the other with no
  `PRECONDITION_FAILED`, including the pipeline's quorum queues.

## Running it beside other things

Each run deletes this app's queues — the six in `contracts.py`, the three
pipeline stages, the register stage's retry rungs, and every `.dlq` and
`.parked` — before each scenario and when it finishes. Give it a vhost of its own
on a shared broker:

```bash
ACEMQ_URL=amqp://guest:guest@localhost:5672/py-policy .venv/bin/python apps/02-policy-administration/main.py
```

The `policy` and `underwriting` exchanges are left in place: the library has no
call that deletes an exchange, and declaring them again is a no-op.

## What is deliberately not here

No HTTP, and no real register or rating service. The claim-check store is a dict
because the key has to name the policy, as Java's does; the library's
`ClaimCheckCodec` does the same job transparently for any payload over a size.

**Retention is the part to think about before you ship one.** The store and the
queue have different lifetimes. A message replayed a month later carries a key,
and if the store expired it the replay produces a message nobody can read.

## Related

- [apps/01](../01-order-fulfilment) — the same patterns across five services
- [intermediate/03](../../intermediate/03-transactional-outbox) — the outbox on its own
- [intermediate/01](../../intermediate/01-request-reply) — request/reply
- [intermediate/07](../../intermediate/07-claim-check) — the library's claim check
- [basic/07](../../basic/07-pipelines) — routing slips and declared routes
