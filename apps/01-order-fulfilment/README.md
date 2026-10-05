# Order fulfilment

Five services, one broker, no shared database, and no service that knows another
exists.

```bash
.venv/bin/python apps/01-order-fulfilment/main.py
```

Everything under `basic`, `intermediate` and `advanced` demonstrates one idea at
a time. This is what they look like when they have to coexist: an outbox at the
edge, idempotency where double-charging is real harm, a retry ladder where a
downstream is flaky, and one correlation id that turns five services into one
story. It is the Python port of the Java examples' `apps/01-order-fulfilment`,
with the same exchange, queues, routing keys and payloads — close enough that the
two halves can be mixed, which has been tried (below).

## The flow

```mermaid
flowchart LR
    C["customer"] --> G["gateway<br/>orders + outbox<br/>one transaction"]
    G -->|order.placed| P["payments<br/>idempotent charge"]
    P -->|payment.captured| I["inventory<br/>retry ladder"]
    P -->|payment.declined| N
    I -->|stock.reserved| S["shipping"]
    I -->|stock.unavailable| N
    S -->|order.shipped| N["notifications<br/>fulfilment.#"]
```

Each service owns one decision and publishes what happened. None of them calls
another.

| File | The pattern | Why it lives there |
|---|---|---|
| `gateway.py` | Transactional outbox | The edge is where the dual-write problem lives: save the order *and* announce it, or a crash loses one of them |
| `payments.py` | Shared idempotency store | The only service where handling a message twice is real money. Claims before charging, confirms after publishing |
| `inventory.py` | Retry ladder | Tells "the warehouse timed out" (retry) from "there are three left and they want ten" (publish `StockUnavailable`, never retry) |
| `shipping.py` | Nothing clever | The point: it reacts to one event, does one thing, publishes one event |
| `notifications.py` | Topic wildcard | Bound to `fulfilment.#`. Added without touching a single publisher |
| `contracts.py` | — | The only thing they share: names, event shapes, the topology |
| `main.py` | — | Starts all five and checks what they claim |

## What to look for

`main.py` puts five orders through, each against a freshly started system, and
prints one line per order:

```
an order travels through every service
  ord-7dd30b96: OrderPlaced -> PaymentCaptured -> StockReserved -> OrderShipped
a flaky warehouse is retried rather than failed
  ord-632c23a5: shipped after 2 retries
an order over the limit stops at payments
  ord-602f63ac: OrderPlaced -> PaymentDeclined
there is not enough stock and retrying would not help
  ord-34a47364: OrderPlaced -> PaymentCaptured -> StockUnavailable
an order announced twice is charged once
  ord-b697889f: announced twice, charged 1 time
all 5 held
```

The first four are the Java system test's four cases with its assertions: every
service acting exactly once, stock moving from 10 to 8, the outbox empty, nothing
downstream of a declined payment, and the money taken when stock runs out. The
fifth is new here. It republishes an `OrderPlaced` with the id the relay already
used, which is what a relay that died between publishing and marking the record
published does on its next sweep. Payments refuses it and shipping does not see a
second order. Notifications does see it, because it is a real message.

Any broken claim prints what was expected and what happened, and the run exits 1.

**The timeline is built from the correlation id alone.** Every service copies it
forward:

```python
envelope=Envelope(type="PaymentCaptured", correlation_id=message.envelope.correlation_id)
```

Drop that argument in any service and the order disappears from the timeline.
Your traces and your log correlation lose it the same way in production.

**The retry count comes from the library.** `inventory.retried` reads
`acemq.messages.retried.total` from the connection's `Metrics`. A count the
handler kept for itself would include the last failure, which was dead-lettered
rather than retried. The Python `Consumer` has no `retried()` the way Java's
`MessageConsumer` does, so the observer is where the number lives.

**Notifications does not decode.** It reads six event types from one queue, so it
consumes with `TextCodec()` and reads only the envelope. Under the default JSON
codec this would also work: Python decodes JSON into a `dict` whatever its shape.
Java's JSON codec needs a target type, so there a fan-in consumer has to ask for
text.

## Mixing it with the Java half

The payloads are camelCase JSON (`orderId`, not `order_id`), because that is how
a Java record serialises. `contracts.to_wire` and `from_wire` translate at the
edge. The queues are classic, because Java declares `classicQueue`, and a broker
compares the queue type like any other argument.

This was checked against RabbitMQ 4 with Java's 0.7 services and this library's
0.7, sharing one vhost:

- Java gateway and shipping with Python payments, inventory and notifications.
  The Java outbox's `OrderPlaced` was charged in Python, Python's `StockReserved`
  was shipped by Java, and Python rebuilt both timelines.
- Python gateway and shipping with Java payments, inventory and notifications.
  All three outcomes (shipped, declined, out of stock) appeared in Java's
  timelines.
- Java declared the topology on an empty vhost first, and Python declared it again
  on top with no `PRECONDITION_FAILED`.

## Running it beside other things

Each run deletes the twelve queues it uses (four, plus each one's `.dlq` and
`.parked`) before it starts and again when it finishes. Anything else consuming
`fulfilment.*` on the same vhost would have its queues pulled out from under it,
so give the app a vhost of its own when the broker is shared:

```bash
ACEMQ_URL=amqp://guest:guest@localhost:5672/py-fulfilment .venv/bin/python apps/01-order-fulfilment/main.py
```

The `fulfilment` exchange is left in place. The library has no call that deletes
an exchange, and declaring it again is a no-op.

## Design decisions worth arguing with

**A database per service.** The gateway and payments each get their own SQLite
file. Once two services read the same table, the deployment boundary is fiction.

**Every service applies the whole topology on start-up.** Applying it five times
is safe, and it means there is no deployment order to get wrong.

**Payments runs before inventory.** Reserving stock for an order that cannot be
paid for is how a warehouse fills with holds nobody releases.

**Money is taken before stock is confirmed available.** When stock runs out the
customer has already been charged, and the run checks exactly that. A real system
triggers a refund here. The example leaves the problem visible rather than
pretending it does not exist.

**A failed publish after a claim leaves the claim held.** If publishing
`PaymentCaptured` fails, the retry finds the order claimed and refuses it until
the two-minute lease runs out. Releasing the claim would risk a second charge
instead. Java makes the same trade.

## What is deliberately not here

No HTTP. The gateway exposes `place_order(...)` as a coroutine, because a web
framework would triple the code and show nothing about messaging. No
compensation either: the refund path is named and not implemented.

## Related

- [intermediate/03](../../intermediate/03-transactional-outbox) — the outbox on its own
- [intermediate/02](../../intermediate/02-idempotent-consumer) — the idempotency store
- [basic/02](../../basic/02-retries-and-dead-letters) — the retry ladder
- [advanced/04](../../advanced/04-tracing) — the trace this correlation id enables
