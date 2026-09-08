# Saga

Three systems, no shared transaction, and what happens when the third one says
no.

> Needs the library's **main** branch — the saga landed after 0.3.0.

```bash
.venv-main/bin/python intermediate/06-saga/main.py
```

## What to look for

**`the broker saw: ['stock.reserved', 'payment.taken', 'payment.refunded',
'stock.released']`.** The compensations run backwards, newest first, because the
later steps are the ones built on the earlier ones. Refunding before releasing
stock would be undoing them in the order they were done, which is the order in
which they depend on each other.

**`complete=False compensated=True failed_at='book courier'`.** A saga reports
rather than raises. Reserving stock, taking a payment and booking a courier are
three services with three databases; there is no transaction across them, so
"roll it back" is not something a database can be asked to do. It has to be done
by running the opposite of each step that succeeded.

**`unresolved=('reserve stock',)`.** The third run is the one that matters. A
compensation *itself* fails — the warehouse will not release a picked
reservation — and the saga does not raise. Something is now half-undone and
needs a person, and an exception thrown into a message handler is a poor way to
tell anyone that: it gets retried, then dead-lettered, and the fact that a
payment was refunded but the stock was not released ends up in a queue nobody
reads. `unresolved` is the row somebody has to look at, and it is a value the
calling code can act on.

A step with no compensation is allowed and means what it says: booking a courier
here is the last step, and if it fails there is nothing of it to undo.

## Steps may be coroutines

Every step here publishes, so every step is `async`. The library takes either —
a step that only touches memory does not have to pretend to be asynchronous —
and awaits what needs awaiting.

## What a saga is not

It is not a distributed transaction and it does not pretend to be one. Between a
step succeeding and its compensation running, the world has seen the step: a
customer whose card was charged and then refunded got two emails from their
bank. A saga makes the *end state* correct, not the middle, and choosing it
means deciding that is acceptable for this workflow.
