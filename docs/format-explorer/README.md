# ProtoCatalogFormat Explorer

A self-contained HTML page that visualises real `ProtoCatalogFormat` byte
sequences — header, checkpoint, transactions — with a Wireshark-style
three-pane layout (hex / decoded tree / field detail). Bytes are emitted
directly by `ProtoCodec` and `InlineDeltaCodec` so the wire format shown is
always exactly what the implementation produces.

## Open the explorer

Double-click `index.html` or open it in any modern browser via `file://`.
No server needed; everything (fixtures, schema, decoder, UI) is embedded.

## Loading an arbitrary catalog file

Click the **"📂 Load catalog file…"** chip next to the scenario dropdown,
or drag-and-drop a file anywhere on the page. The proto walk, hex pane,
and tree pane all run on the loaded bytes the same as on a bundled
scenario — useful for triaging a corrupted or unfamiliar catalog file.

For inline-TM bytes fields (`InlineTable.metadata`,
`CreateTableInline.metadata`, `UpdateTableInline.full_metadata`), the page
decodes the codec-encoded payload **in the browser** using
`DecompressionStream('gzip')` (no external dependency):

- **`CODEC_JSON_GZIP`** — gunzip + UTF-8 decode + pretty-print as
  TableMetadata JSON.
- **`CODEC_STRUCTURAL`** — gunzip + walk the wrapper one level deep
  (`format_version`, `stripped_json`, `snap_block_len`/`snap_block`,
  `mdlog_block_len`/`mdlog_block`). The recovered JSON shows everything
  *except* snapshots and metadata-log; their columnar bytes are surfaced
  in the "Show compact form" table for inspection. Snapshot reconstruction
  isn't ported to JS — for full snapshot decode use the Java codec
  (`StructuralInlineMetadataCodec.decodeFull`).

The dynamic decode is asynchronous: clicking on an inline-TM bytes node
during the brief window before it completes shows a placeholder; click
another node and back to retry, or just wait for the auto-rerender.

## Bundled scenarios

| # | Name | Teaches |
|---|------|---------|
| 1 | `empty-bootstrap` | header, length framing, oneof discriminator |
| 2 | `namespaces-and-pointer-tables` | repeated fields, parent_id chaining, post-CAS state |
| 3 | `txn-multi-action-with-late-bind` | 5-byte negative-int varint quirk, `parent_version=-1` |
| 4 | `create-table-inline-gzip` | inline-TM `metadata_codec=CODEC_JSON_GZIP`; gzip-wrapped JSON |
| 5 | `create-table-inline-structural` | inline-TM `metadata_codec=CODEC_STRUCTURAL`; columnar layout (varint format_version, stripped JSON, snap_block, mdlog_block) |
| 6 | `update-table-inline-delta` | deepest message nesting, fixed64, sint64, manifest entry |
| 7 | `pointer-multi-table-conflict-retry` | atomic multi-table commits, version-based conflict detection |
| 8 | `inline-multi-table-conflict-retry` | inline-mode delta commits, conflict-retry on inline tables |

Click any byte in the hex pane or any node in the tree pane to populate the
detail panel with the field's name, proto field number, wire type, scalar
type, schema declaration, and the decoded value.

`bytes` fields that carry gzip-wrapped JSON (`AddSchema.schema_json`,
`AddPartitionSpec.spec_json`) get the same two-expander treatment as
inline-TM bytes:

- **Show recovered JSON** — gunzipped + pretty-printed.
- **Show compact form** — a single labelled segment for the gzip envelope
  (`gzip-wrapped JSON (decompresses to N bytes of Schema JSON)`), with
  byte count and hex preview. Bundled scenarios don't currently include
  AddSchema / AddPartitionSpec deltas, so the decode fires only on loaded
  files; the async decode pass runs the same `DecompressionStream('gzip')`
  path as inline-TM.

Inline-TM `metadata` / `full_metadata` fields (codec-encoded) get the
multi-segment compact form driven by Java-side pre-decoding:

- **Show recovered JSON** — the decoded `TableMetadata` JSON, pretty-printed.
- **Show compact form** — a labelled table walking the (decompressed) wire
  layout: `format_version`, `stripped_json`, `snap_block`, `mdlog_block`,
  etc., each with its byte count and a hex preview.

`AddSortOrder` no longer carries an `order_json` bytes field — it now
encodes a `repeated SortField fields = 2` (see catalog.proto). Each
`SortField` is a regular nested submessage; click into the tree to see
`source_id`, `kind` (TransformKind enum), `transform_param`, `direction`
(SortDirection enum), and `null_order` (NullOrderKind enum) with their
named enum values.

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

The visualizer surfaces one encoding quirk worth knowing about:

- **5-byte negative-int varints.** `ProtoCodec.writeRawVarint(int)` operates
  on a 32-bit Java `int` and uses unsigned right-shift (`>>>= 7`). A
  negative `int32` such as the `-1` late-bind sentinel terminates after 5
  bytes. Canonical proto3 sign-extends a negative `int32` to 64 bits and
  writes 10 bytes. Most `protoc`-generated readers (treating the field as
  `int32`) accept the 5-byte form, but the encoded bytes are not strictly
  conformant. Scenario 3 highlights this as a quirk callout.
