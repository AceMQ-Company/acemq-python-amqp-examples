# Claim check

A payload too large for a broker, put aside, and the reference that travels
instead.

```bash
.venv/bin/python intermediate/07-claim-check/main.py
```

## What to look for

**Two messages, `126` bytes and `39`.** The small invoice travels inline and
nothing is stored. The one with the scan attached — half a megabyte of base64,
which is an unremarkable scan — goes to the store, and what reaches the broker
is a UUID. The two lines are printed next to each other because the difference
between them is the entire pattern, and being told about it is not the same as
seeing 39.

**`framing ac 01 00` against `ac 01 01`.** Three bytes at the front of the body
say which of the two a message is. That is why a consumer handles both without
being told which to expect, and why this codec can be introduced on a queue that
already has messages in it: a body it did not write goes to the delegate
untouched. The same three bytes in all five libraries, so a document a Java
service put aside is readable here.

**`65535 bytes encoded: 65538 on the wire` and `65536 bytes encoded: 39`.** The
threshold is `DEFAULT_THRESHOLD`, 64 KiB, and it is compared **strictly less
than** — a payload of exactly 65536 is the first one offloaded. The example
constructs both and prints what happened to each rather than asserting the
number in a comment, because that comparison is the one thing here that cannot
be changed in one library alone. Two services that disagreed about it would
disagree about which messages are claim checks.

The inline body is 65538: three bytes of framing on top of the payload. That
overhead is why the threshold is not zero. Offloading a two-hundred-byte event
turns one broker round trip into a store round trip *and* a broker round trip,
which makes the common case slower in order to fix the rare one.

**`payload gone: … the claim check '…' is not in the store`.** The failure the
pattern introduces, and the reason it is worth showing. The store and the queue
have separate lifetimes and nothing enforces a relationship between them, so a
payload can be removed while a message referring to it is still deliverable.
Here the directory is emptied; in a deployment it is a lifecycle rule on a
bucket, or a volume reclaimed along with a pod.

It is **fatal, not retryable**. The payload is not coming back, so a message
redelivered for it only holds a queue open until it ages out. A body that will
not decode never reaches the handler — the example checks that its handler never
ran — and the consumer settles the message itself, onto
`py-claim-check.invoices.parked` with the reason attached. Parked rather than dead-lettered: a message nothing could read
is a different problem from one that failed five times.

## Which store

`FilesystemClaimCheckStore`, writing into a `payloads/` directory beside the
example, which the run deletes on its way out.

The alternative in the library is `InMemoryClaimCheckStore`, and it is the wrong
one here for the reason that is the point of the pattern: it holds the payloads
in the publisher's own memory, which is where they were going to be anyway, so a
consumer in another process gets "the claim check is not in the store" for every
message. It is genuinely useful in a test, where the publisher and the consumer
are the same process and the thing being proved is the framing.

A directory is the honest middle ground. It is right where the filesystem is
shared and durable — an NFS mount, a persistent volume — and it is the in-memory
store with extra steps on a container's local disk, where the consumer is on
another host and finds nothing. Object storage is the usual answer in a
deployment, and a store in front of S3 or Azure Blob Storage is the same three
methods: `put`, `get`, `delete`.

Writes are atomic — the payload goes to a temporary file and is moved into
place. Messaging is exactly the arrangement that makes a consumer fast enough to
read the key before the writer finished normal rather than unlikely, and without
that it would get a truncated payload and a parse error somewhere unhelpful.

## Retention is the part that goes wrong

Nothing deletes a stored payload for you, and that is deliberate. Deleting on
read breaks the second consumer of the same message; deleting on acknowledgement
breaks a replay. So when a payload may be removed is a retention decision, and
retention decisions belong to whoever owns the data — `delete` is on the store
for them to call.

What the decision has to clear is every retention that could bring a message
back: queue TTLs, dead-letter queues, and however long somebody might sit on a
message before replaying it by hand. When in doubt, longer. The line above is
what the alternative looks like.

## The key is in the body, not a header

`claim_key_of(body)` answers "which object does this message need" from the
bytes alone, holding no store at all. It is the line worth having in front of a
dead-letter queue, where the question is whether the payload is still there.

A consumer decides what a message is from the three bytes at the front of the
body and never from a header, because a header can be dropped by a shovel or a
plugin and the body cannot. `x-acemq-claim` is reserved for an application that
wants to say where a payload went in a form an operator can read; nothing here
depends on it.

The content type stays the delegate's — `application/json`. A claim-checked
message is still a document; it is a document that is somewhere else.
