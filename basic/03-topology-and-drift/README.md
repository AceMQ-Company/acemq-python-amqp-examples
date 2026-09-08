# Topology, and a broker that disagrees

Declaring a topology, printing it before applying it, and catching drift.

```bash
.venv/bin/python basic/03-topology-and-drift/main.py
```

## What to look for

**The plan, printed before anything is declared.** `Topology.plan()` is a dry
run: what would be declared, in the order it would be declared, without touching
the broker. It is a list of thirteen actions for a topology the source describes
in four lines, and the interesting rows are the ones nobody wrote — the
`acemq.dlx` and `acemq.retry` exchanges, and one `py-shipping.labels.retry.*`
queue per delay above the thirty-second line. A six-attempt policy asking for
three queues is much better read here than discovered on a broker.

**`refused: ChannelPreconditionFailed … received 'classic' but current is
'quorum'`.** This is the half worth the example. A queue's arguments are fixed
when it is created and AMQP has no way to alter them, so a service whose idea of
a queue has moved on cannot declare it — and a service that cannot declare a
queue cannot consume from it either. The library does not paper over this: a
service and its broker disagreeing is a thing to be told about at start-up, not
at the first message.

The drift runs on a **second connection**, because that is what it really is: a
second service starting up with a different idea of the same queue. It is also
the tidy way to do it — a refused declaration closes the channel it happened on,
and a robust client will try to restore that channel with the declaration that
killed it.

## What to do when it happens

There is no way to alter a queue in place. Either the new declaration is wrong
and the code goes back, or the queue is drained and deleted before the new one
is declared. Choosing between those is a decision; the library's job is to make
sure somebody makes it.
