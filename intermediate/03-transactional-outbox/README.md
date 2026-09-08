# Transactional outbox

A message written in the same transaction as the work, and a relay publishing it
after.

```bash
.venv/bin/python intermediate/03-transactional-outbox/main.py
```

## What to look for

**`orders committed: ['ORD-1']` and `messages in the outbox: 1`.** Two orders
were started and one was rolled back. The rolled-back one left *nothing* behind
— not "nothing published", but nothing in the outbox at all, because it never
was. The order and the message about it were one write.

**`the relay published 1`, and then `received: ['ORD-1']`.** Publishing is a
separate concern from the transaction, and it happens whenever the relay gets
round to it. `sweep()` is one pass, called here so the example does not wait for
the timer; `start()` is the timer, for a service.

## The rule that makes it work

`add` writes on the connection **you** hand it. It does not commit it, does not
roll it back and does not close it. That is the guarantee rather than an
oversight: roll your transaction back and the message is not in the outbox.

With no transaction to join, `add` raises rather than opening one. A fresh
connection with autocommit on would leave a message queued for work that never
happened — the exact fault the pattern was adopted to prevent.

## Why not just publish?

Publish before the commit and a rollback leaves a message about an order that
does not exist. Publish after and a crash in between leaves an order nobody was
told about. Neither is rare enough to ignore, and no amount of ordering the two
calls fixes it, because they are two systems and there is no transaction across
them.

## What the relay must not do

Publish the stored bytes through the connection's codec, which encodes them a
second time: a typed consumer then cannot read what the relay published, and
nothing looks wrong from the relay's side. That was a real bug in the .NET
library, found by the .NET version of this example and fixed in 0.1.8. `record`
encodes once, and the relay publishes what was stored.
