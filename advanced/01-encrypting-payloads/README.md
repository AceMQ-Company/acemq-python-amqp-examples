# Encrypting payloads

Message bodies the broker cannot read, and a keyring that can rotate.

> `codecs.encrypted` needs the `[crypto]` extra, which `requirements.txt`
> installs.

```bash
.venv/bin/python advanced/01-encrypting-payloads/main.py
```

## What to look for

**`the card number is in there: False`.** The message is pulled back off the
queue as raw bytes and printed — this is what an operator with the management
interface open actually sees. A broker holds messages on disk, in its backups
and in its management interface, and everybody with access to any of those can
read them. TLS does not touch that: TLS protects the wire, not the queue the
message sits in for four hours.

**`which key does it need: '2026-01'`, answered from the bytes alone.**
`key_id_of(body)` holds no keys and decrypts nothing. The identifier travels in
the clear in front of the ciphertext, which is what makes rotation possible at
all — a consumer reads which key a message needs instead of assuming the current
one.

The header is bound in as **associated data**, so an identifier altered in
flight makes the message fail to open rather than open as something else.

**The rotation.** The new key is added and made current; the old one stays on
the ring, because messages encrypted with it are still on the queue and still
have to be readable. Both are read by the same consumer in the same run.

**`without 2026-01: FatalError: … which is not on this keyring`.** A consumer
given only the new key cannot read a message written under the old one, and says
which key it wanted. Fatal rather than retryable, because a missing key is not
something a fourth delivery fixes.

## What the failures do not say

No failure message, log line or exception ever contains the plaintext or the
key. A wrong key and a tampered body fail identically — GCM authenticates before
it returns anything, and nothing here adds a check that would tell them apart.

## Interoperability

**It interoperates with the Java library and with nothing else.** Java, Go and
.NET currently write three different framings under one content type; a body
from Go or .NET is refused here, visibly, rather than misread. That is the state
of play rather than a design, and the table to converge on is in the library's
`docs/serialization.md`.

## Keys

The keys in this example are generated at start-up, because a key checked into
an examples repository is not a key. A deployment gets them from a secret
manager, one per rotation period, and keeps the old ones for as long as messages
written under them can still be on a queue.
