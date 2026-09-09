# Schema evolution and the registry

Two services on two versions of one schema, talking to each other anyway.

```bash
.venv/bin/python intermediate/09-schema-evolution/main.py
```

## What to look for

```
reader   written with  decoded
v1       id 1          {'order_id': 'A-1', 'total': 42.0}
v1       id 2          {'order_id': 'B-2', 'total': 99.5}
v2       id 1          {'order_id': 'A-1', 'total': 42.0, 'currency': 'EUR'}
v2       id 2          {'order_id': 'B-2', 'total': 99.5, 'currency': 'GBP'}
```

**Row two is the one that matters.** A producer already on the new schema, a
consumer still on the old one. The `currency` field the writer added is *skipped*
— not misread, and not left shifting every field after it, which is what happens
to a decoder that assumes the writer used the schema the reader holds. Nobody had
to redeploy the old consumer to make the new producer safe to ship, and that is
the only reason to run a registry.

**Row three is the easier half.** A v2 reader meeting a v1 message fills
`currency` in from the reader's default. That default is not decoration: without
it, adding the field is not a backwards-compatible change and Avro has nothing to
put there.

**`written with id 1` / `id 2` is read off the wire.** Every registered message
carries the identifier of the schema it was *written* with, framed on the front:
one zero byte, four bytes of identifier, big-endian, then the Avro body. That is
Confluent's framing, and Java, Go and .NET write the same five bytes. The
identifier is the only thing that makes any of the rest work — Avro resolves a
*writer* schema onto a *reader* schema, and without it a reader has no idea what
the writer used.

**`versions of py.order.placed: [… v1 …, … v2 …]`, and registering v2 twice does
not make a v3.** A schema is identified by a SHA-256 of its exact bytes, so the
same definition registered again comes back with the same id. Without that, a
service that registers its schemas on every start adds a version per restart and
the version number stops meaning anything. Two definitions differing only in
whitespace *are* two schemas, which is strict on purpose: normalising first would
need a parser per format, and a registry that treated two schemas as one because
it mis-parsed them would be worse than one that is merely fussy.

**`unprimed reader: … this codec has not been taught …`.** A codec built by
`from_registry` knows its own schema and no other. Meeting an identifier it has
never seen it refuses, by name, rather than guessing — and the message says
exactly which call fixes it. Priming from `registry.versions(subject)` at
start-up, as this example does, is the usual answer; `learn_from` on demand is
the other.

## Registered mode is a different message

```python
codec = await AvroCodec.from_registry(registry, subject, schema)
```

That codec writes `application/vnd.acemq.avro`. A codec built as
`AvroCodec(schema)` has a fixed schema, writes `avro/binary`, and the two are not
interchangeable: the five bytes of framing are invisible in the body, so a
fixed-schema codec handed a registered message would read the identifier as the
first field and hand back a record of silent nonsense. Each mode therefore
accepts only its own content type, and says so when refused.

`basic/05-serialization` shows the fixed-schema half, alongside every other codec.

## The registry here is not a registry

`InMemorySchemaRegistry` shares nothing between processes, so a consumer cannot
look up a schema a producer registered somewhere else — which is the entire point
of having one. It is here to show the shape. `SqlSchemaRegistry` outlives the
process; Confluent's works too, because the wire framing is theirs.

Which header would carry a schema identifier is deliberately *not* something
AceMQ has invented. The identifier lives in the body's framing, where Confluent
put it, so a Python producer and a Java consumer agree without either of them
having to know about the other.
