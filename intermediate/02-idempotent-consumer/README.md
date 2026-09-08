# Idempotent consumer

One logical payment delivered four times and charged once — and still once after
a restart.

```bash
.venv/bin/python intermediate/02-idempotent-consumer/main.py
```

## What to look for

**`delivered 4, charged 1`.** At-least-once delivery is the guarantee a broker
gives you, so a duplicate is not a fault to be prevented; it is a normal
Tuesday. The retry that arrived after a handler succeeded but before its
acknowledgement got out looks exactly like a fresh message, because it is one.

**A duplicate is accepted, not rejected.** The work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right.

**The key is `m.payload["payment"]`, not the envelope id.** Four separate
publishes are four different messages as far as the broker is concerned; what
must not happen twice is the charge, and the charge is identified by what is in
the body. Where the natural key *is* the envelope id, leave `key` off.

**`after a restart, PAY-42 is still known: True`.** This is the half most
write-ups leave out. An in-memory set deduplicates until the pod restarts, and a
pod restarts most often right after the incident that produced the duplicates.
The store here is SQLite on disk, and the second `SqlIdempotencyStore` is a
different object over the same file.

## The store is a seam, on purpose

`SqlIdempotencyStore` hands out a **lease** rather than a fact: `first_time`
claims the key, and `confirm` — called for you when the handler accepts — turns
the claim into a record. A consumer that dies holding a message therefore does
not block its redelivery for ever; the claim times out.

Nothing in `acemq_amqp.patterns.sql` imports a database driver. It is written
against the DB-API 2.0 protocols, so `sqlite3` from the standard library works
with nothing installed and psycopg works if you have it — `paramstyle="format"`
for the latter.

`create_schema` is called here because this is an example. A deployment gets
those tables from a migration; `schema_ddl(dialect=...)` prints the statements
to put in one.

## What this is not

It is a guard against duplicates, not a promise of exactly-once. Between the
handler finishing and the acknowledgement reaching the broker there is still a
gap where a crash leaves a message that will be delivered again. Only a store
written in the same transaction as the work closes it — which is why
`IdempotencyStore` is an interface rather than a class, and why the next example
exists.
