# ProtoCatalogFormat Explorer

A self-contained HTML page that visualises real `ProtoCatalogFormat` byte
sequences — header, checkpoint, transactions — with a Wireshark-style
three-pane layout (hex / decoded tree / field detail). Bytes are emitted
directly by `ProtoCodec` and `InlineDeltaCodec` so the wire format shown is
always exactly what the implementation produces.

## Open the explorer

Double-click `index.html` or open it in any modern browser via `file://`.
No server needed; everything (fixtures, schema, decoder, UI) is embedded.

## Bundled scenarios

| # | Name | Teaches |
|---|------|---------|
| 1 | `empty-bootstrap` | header, length framing, oneof discriminator |
| 2 | `namespaces-and-pointer-tables` | repeated fields, parent_id chaining, post-CAS state |
| 3 | `txn-multi-action-with-late-bind` | 5-byte negative-int varint quirk, `parent_version=-1` |
| 4 | `create-table-inline-with-real-metadata` | oneof bytes variant, JSON-in-protobuf nesting |
| 5 | `update-table-inline-delta` | deepest message nesting, fixed64, sint64, manifest entry |
| 6 | `sealed-toggle` | the always-written `sealed` byte and in-place mutation |

Click any byte in the hex pane or any node in the tree pane to populate the
detail panel with the field's name, proto field number, wire type, scalar
type, schema declaration, and the decoded value. Inline `bytes` fields that
carry JSON (`metadata`, `schema_json`, etc.) get a "Show JSON" expander.

## Regenerating the fixtures

The fixtures are byte-accurate — they come from `ProtoCodec` directly via the
`FormatExplorerFixtures` JUnit class, which also parses `catalog.proto` for
the schema descriptor and asserts that field numbers in the `.proto` agree
with the `private static final int` constants in `ProtoCodec.java` /
`InlineDeltaCodec.java`. Schema/code drift surfaces as a build failure.

```bash
# From the fileio-catalog/ project root:
mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true
python3 docs/format-explorer/splice-fixtures.py
```

The first command writes `target/format-explorer/fixtures.json`. The second
splices it into `index.html` between the `<!-- fixtures:start -->` /
`<!-- fixtures:end -->` sentinels (in the `<script id="fixtures-data">`
block).

The test is gated by the `explorer.regenerate` system property and is
silently skipped during the regular `mvn test` run.

## Files

- `index.html` — the visualizer (HTML + inline CSS + inline JS + embedded fixtures)
- `splice-fixtures.py` — copies `target/format-explorer/fixtures.json` into `index.html`
- `../../src/test/java/org/apache/iceberg/io/FormatExplorerFixtures.java` — fixture generator + proto parser + drift check

## Notes on the wire format

The visualizer surfaces two encoding quirks that are worth knowing about:

- **5-byte negative-int varints.** `ProtoCodec.writeRawVarint(int)` operates
  on a 32-bit Java `int` and uses unsigned right-shift (`>>>= 7`). A
  negative `int32` such as the `-1` late-bind sentinel terminates after 5
  bytes. Canonical proto3 sign-extends a negative `int32` to 64 bits and
  writes 10 bytes. Most `protoc`-generated readers (treating the field as
  `int32`) accept the 5-byte form, but the encoded bytes are not strictly
  conformant. Scenarios 3 and 6 highlight this as a quirk callout.
- **Always-written `Transaction.sealed` byte.** The `sealed` bool field is
  written even when its proto3 default value would normally omit it.
  This guarantees a fixed byte offset for `ProtoCodec.sealTransaction()` /
  `unsealTransaction()` to flip in place. Click the "Toggle sealed bit"
  button on scenario 3 or 6 to see exactly which byte changes.
