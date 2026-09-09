# Pipelines

An order carried through three services by an itinerary it brings with it — and
a failed run put back where it stopped rather than at the beginning.

```bash
.venv/bin/python basic/07-pipelines/main.py
```

## What to look for

**`visited: ['validate', 'charge', 'ship']`.** No step knows what comes next.
Each one does its job and returns the payload; `follow_slip` reads the slip off
the message, advances it, and publishes to whatever is now at the front. The
order of the steps is a property of the message, so changing it is a deploy of
the thing that starts runs and not of the three services doing the work.

**`shipped: {'stamps': ['validate', 'charge', 'ship']}`.** What a step returns
is what the next one receives, which is the difference between a pipeline and
three consumers that happen to publish to each other.

**`ran: ['validate', 'charge']`, then `['validate', 'charge', 'charge',
'ship']`.** This is the whole example. The card issuer refuses, `charge` raises
`FatalError`, and the run stops — with a message on `py-fulfilment.charge.dlq`
that still carries the route and how far along it was. Putting it back runs
`charge` again and `validate` **not at all**. A restart would have run both, and
`validate` is the cheap half: the expensive one is a step that already moved
money, sent an email or called somebody else's API.

**`at position 1 of validate,charge,ship`.** The two halves of a resume. The
route says what the steps are; the position says which one is next. Everything
the operator needs is on the message that failed.

## Two wire forms, and why there are two

Every AceMQ library reads both.

**The JSON itinerary**, in the `acemq-routing-slip` header, assembled per
message with `RoutingSlip().then(...)`:

```json
{"steps": [{"exchange": "", "routingKey": "py-slip-charge", "name": "charge"}],
 "done":  [{"exchange": "", "routingKey": "py-slip-validate", "name": "validate", "completedAt": "…"}]}
```

Each step names its own destination, so a consumer can follow it knowing nothing
in advance. That is what makes it the right form for a route assembled per
message — a refund that skips a step, an order that needs an extra approval.

**The declared route**, built by `route_of(pipeline, *steps)`, which is what
Java's `Pipeline` writes. Three short reserved headers — `x-acemq-route`,
`x-acemq-route-position`, `x-acemq-route-id` — carrying the step *names* and
nothing else. The exchange is the pipeline's name, the queue behind a step is
`{pipeline}.{step}`, and the consumer resolves the names against that. It is
smaller on the wire, it reads in a management console without decoding anything,
and one run can be followed across every hop by its `route-id` — at the price of
a route that has to be declared on both ends and is the same for every message.

`slip_from` reads either, and `follow_slip` writes back the one that arrived
unless told otherwise. That default is what lets a Python step sit in the middle
of a pipeline a Java service declared: answering a declared route with a JSON
slip would hand the next Java step a message with no route on it, and the run
would stop half way with nothing anywhere saying why.

`pipeline=` is passed to `follow_slip` for the declared form only, because the
pipeline's own name is the one thing those headers do not carry.

## The rule a step has to keep

`follow_slip` accepts the incoming message **only once the next one is out**, so
a failure to publish retries the step that just succeeded. Every step that
changes anything therefore has to be idempotent — which is the same requirement
[`intermediate/02-idempotent-consumer`](../../intermediate/02-idempotent-consumer)
exists to meet, and it is not optional here.

Raising `FatalError` stops the run where it is and skips the retries, which is
what this example does at `charge`: a card issuer refusing everything is not
going to refuse differently in four seconds. An ordinary exception is retried by
the connection's policy first, and only then dead-lettered.
