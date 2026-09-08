# Retries and dead letters

Three payments through one queue, and three different endings from one policy.

```bash
.venv/bin/python basic/02-retries-and-dead-letters/main.py
```

Takes about five seconds: the last failure spends 0.4 + 0.8 + 1.6 seconds
waiting before it gives up.

## What to look for

**`flaky: attempts [1, 2, 3]`.** The attempt counter really moves. A retry is
*republished* rather than requeued, so the count lives on the message rather
than in the memory of the process that has been failing — which is what makes it
survive a consumer restart, and what makes a retry limit mean anything.

The counter to read is `message.envelope.attempt` on the delivery, and it is the
consumer's count. A broker redelivering the original bytes would hand back
whatever the publisher wrote, for ever.

**`expired: attempts [1]`.** `FatalError` skips the attempts that are left,
because nothing about a fourth attempt makes an expired card work. The
dead-letter reason says `the handler reported an unprocessable message`, which
is a different sentence from `exhausted 4 attempts` on purpose.

**The two `x-acemq-error` lines at the end**, read off the dead-letter queue. The
reason names the limit that was hit and the last failure: `exhausted 4 attempts:
RuntimeError: the issuer declined`. That is the line somebody reads at eight in
the morning, so it says what happened rather than that something did.

**`waits spent in the broker: none, all under 30s`.** A short wait is spent in
the consumer, holding the delivery and one prefetch slot. A long one is spent in
the broker, on a `{queue}.retry.{delay}` queue whose `x-message-ttl` is the wait.
The example's own policy is entirely below the 30 second line; the third line of
output shows what a ten-second production policy would need instead — three rung
queues, one per delay.

One queue per delay rather than one queue for all of them, because RabbitMQ
expires messages only from the **head** of a queue: mixed per-message expirations
in one queue let a ten-minute wait at the front hold back every thirty-second
wait behind it.

## The reader is bytes, deliberately

The dead letters are read with `mq.pull` and `Envelope.from_headers`, without
decoding the body. The message that went to a dead-letter queue may be exactly
the one nothing could decode, and a reader that assumes JSON cannot tell you
that.
