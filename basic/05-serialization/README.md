# Serialization

Six formats on one queue, read by one consumer.

> Needs the library's **main** branch — the optional codecs landed after 0.3.0.
> `.venv-main/bin/python`, or see the repository README.

```bash
.venv-main/bin/python basic/05-serialization/main.py
```

## What to look for

**One `CompositeCodec` reading six content types.** This is what a format
migration actually looks like: the producers change one at a time, the queue
carries both spellings for a while, and the consumer has to read whatever turns
up. The composite asks each codec whether it can read the content type it was
given, in order, and the first that says yes gets the body. A message with *no*
content type rules nothing out, so every codec is a candidate and the first that
can actually read the bytes wins.

**A codec per publisher.** `mq.publisher(..., codec=YamlCodec())` is one
producer changing what it writes without changing what anything reads. The
connection's codec is only the default.

**`total_cents='4250'` from XML and `4250` from everything else.** XML has no
types; every value comes back a string. That is not a defect in the codec, it is
XML, and it is the sort of thing worth finding in an example rather than in a
comparison that silently fails.

**Eleven bytes of Avro against fifty-eight of JSON.** The schema is not in the
message, which is the whole argument for a binary format and also the whole
problem with one: a consumer needs the schema from somewhere else.

**The protobuf message is built from a descriptor at run time**, so this example
is one file and needs no `protoc`. A real service imports what protoc produced;
the codec cannot tell the difference, because both are the same generated class.

## Extras

One install extra each — `[yaml]`, `[toml]`, `[protobuf]`, `[avro]`, `[crypto]` —
rather than one for all of them, because a service that speaks YAML has no
reason to install a protobuf runtime. XML has no extra at all: it is written
against the standard library.

These are the same five formats the Java and Go libraries ship, with the same
content types and the same wider set of accepted spellings — `text/yaml` and
`application/x-yaml` both predate the registered `application/yaml`, and refusing
them would park a message that was perfectly readable.
