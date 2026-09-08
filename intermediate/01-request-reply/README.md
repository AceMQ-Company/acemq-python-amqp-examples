# Request and reply

Ten concurrent questions, each getting its own answer, and a failure reaching
the caller.

```bash
.venv/bin/python intermediate/01-request-reply/main.py
```

## What to look for

**Ten requests in flight, ten answers, each to its own caller.** The
correlation is the envelope's, so one reply queue serves every question the
process asks. The other design — a queue per request — costs a declaration and a
deletion on the broker for every call, and a broker with ten thousand
short-lived queues is a broker with a problem.

**`replies come back on acemq-reply-…`.** One exclusive, auto-deleting queue,
generated per requester and released when its channel closes. It is declared
classic rather than quorum without being asked, because RabbitMQ refuses a
quorum queue that is exclusive or auto-deleting.

**`the caller was told: acemq: the responder could not answer …: ValueError: no
such instrument`.** This is the point of the example. A responder that raises
sends the failure back, and the caller sees a `ResponderError` naming it — not a
`RequestTimeoutError` thirty seconds later. A timeout means "no answer"; a
failure means "the answer is no", and a caller that cannot tell them apart
retries the one thing that will never work.

**The failure is also dead-lettered.** The line above the output —
`set aside … the handler rejected it: ValueError` — is the responder's side of
the same event. The caller was told, *and* the message that could not be
answered is where an operator can find it. Both, because a reply that vanished
into a caller's exception handler is not an audit trail.

## Timeouts

`Requester.open(..., timeout=...)` is the default for every question, and `ask`
takes its own for the one call that is allowed to be slower. There is always a
timeout: a request over a broker with none is a coroutine that waits for a
process that may not exist any more.
