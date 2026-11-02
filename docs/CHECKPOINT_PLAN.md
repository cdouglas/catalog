# Checkpoints in pages: plan and review

Status: plan, 2026-11-02. R0.1 and R0.2 are done (uncommitted); R0.3, R1
and R2 are not started. The open decisions are at the end.

The catalog's checkpoint is one byte array: the log's envelope (log id,
committed set) around the catalog's state (namespaces, tables, inline table
metadata, inline manifest lists and their pool, conflict-encoding state). It is
written whole by every compaction, and read and decoded whole by every reader
that cannot fold onto a replica it holds. Several errata come back to that:

| Erratum | In short | What it costs |
|---|---|---|
| D14 | commit knobs fixed, not per store | `max.append.count` and the race backoff are constants measured on one store |
| D15 | a read reads everything | every read GETs the whole object |
| D16 | replay rewrites committed set | every checkpoint carries the whole committed set |
| D18 | ML lists stored whole | each inline-ML snapshot's manifest list is stored in full |
| D19 | whole-object compaction trigger | a checkpoint past `max.append.size` compacts on every commit |
| D20 | four reads per commit | fixed in part: a commit now reads once, for `apply`'s refresh |

Measured: an inline-ML catalog of 10 tables x 7,200 snapshots is a 24 MB object,
and every commit to it compacted (catalog-bench E3, 2026-11-02); the ingest
captures' checkpoints hold a committed set of 19.8 MB beside 12-14 MB of state,
and 237.6 MB of a 238.6 MB checkpoint when the set is prefilled to its steady
state (fileio-catalog lab, `compactions`). Paging the state alone does not bound
the object: the committed set has to be paged too (COMMITTED_PAGING.md).

## Review

Two independent design reviews (2026-11-02, read-only, on main 00c4c5d plus
D20's working tree) asked whether the code is factored for a checkpoint of many
objects, possibly a hierarchy, and what to refactor first. Line numbers are as
of that tree.

### The log layer (intentlog-core, the providers)

Where a checkpoint is assumed to be one array, one write, decoded whole:

- The SPI: `LogView.checkpoint()` is `Optional<byte[]>` (LogView.java:35);
  `SharedLog.replace` writes a checkpoint and records "in one write"
  (SharedLog.java:79-86); `putCheckpoint(tail, byte[])` (:95); `LogTail.bytes()`
  counts checkpoint and records together (LogTail.java:31).
- The envelope: `LogEnvelope.encodeCheckpoint` holds the committed set and the
  state inline (LogEnvelope.java:135-148, log.proto:41-56), and decoding copies
  both out (:180, :183).
- Replay decodes the envelope, the set and the state before the first record
  (Replay.java:145-149); `CommittedSet.of` and `bytes()` are whole-set
  (CommittedSet.java:74, :112).
- The state machine: `decodeCheckpoint(byte[])`, `encodeCheckpoint(S)`
  (StateMachine.java:54, :57); S4 is a byte round trip, and the default
  `toBuilder` is one (:64).
- The runtime: `checkpoint(replica)` encodes everything (LogRuntime.java:158),
  from three hand-built paths: create (:697), a fused replace (:497, :546, with
  `records()` :727) and `compact()` (:140). The encode at :546 sits inside the
  throttle-resend supplier, so a throttled replace encodes again on each resend.
  The fold compares whole checkpoints and record prefixes with `Arrays.equals`
  (:339-365), which makes every replica keep its whole view (Replica.java:37).
- The providers: `AtomicFileLog` frames the checkpoint inside the object, GETs
  it whole and copies it out; `InMemorySharedLog` keeps one array per position
  and checks I11 with `Arrays.equals`.

What is sound and should stay: replay as a pure fold (a page-out record would
slot in beside the GC record, Replay.java:181); opaque receipts and tails;
`Capabilities`; P1-P6; `compact()`'s steps; CommittedSet's runs. D20's change is
in the right layer (provider only; the SPI and P3 unchanged; shared arrays make
the fold's comparison an identity check), resting on the append condition naming
the exact version, which the fork's contract test checks
(`appendPinnedToAVersionReplacedAtTheSameLengthFails`); ATOMIC_FILE_LOG.md now
states it as a provider requirement.

`CompactionPolicy.due(records, logBytes, recordBytes)` is stateless and sees the
whole object (CompactionPolicy.java:29). A tail-only bound (D19, whole-object
compaction trigger) needs the checkpoint's size apart from the records'. A
per-store, cost-based policy (D14, commit knobs fixed) needs what only the
runtime can observe: latencies per operation (append, CAS, GET per MiB), lost
races, encode and decode times, replay time per record, reads per commit, the
store's parts used so far. Suggested shape: `due(input)` with store statistics
the runtime keeps and an `observe` callback; the hard caps (full, oversized, no
append) stay outside the policy. A store without append replaces on every
commit (`!caps.append()`, LogRuntime.java:524) and re-encodes the whole
checkpoint each time; a replace that carries the old checkpoint and its records
with the new record after them (an emulated append) would follow the policy,
but the TLA+ model's `CheckpointIsReplay` (CatalogCommit.tla:586) assumes a CAS
checkpoints the version it replaces.

### The catalog's state (CatalogState, ProtoCodec)

- Encoding is whole-state, every time (`ProtoCodec.encodeCheckpointState`,
  ProtoCodec.java:288-353), every map copied into a TreeMap to sort it. Inline
  table metadata is cached per `InlineTable` version (InlineTable.java:67-90);
  an inline-ML table's pool and lists are not, and are encoded again on every
  compaction (:553-604): about 97% of the state's encode work in the ingest
  captures (16 inline-ML tables, ~1,700 lists; encode 90-100 ms, decode 50-80
  ms).
- Decoding is eager except for inline metadata: namespaces, rows, properties,
  every pool entry, every list and every conflict state are decoded up front
  (ProtoCodec.java:367-427). Inline metadata stays bytes until a record touches
  the table, and then the whole table is decoded (InlineTable.java:91-99).
- Pool indices are positions in the pool sorted by path (ProtoCodec.java:570-
  601), so one new manifest can renumber every list: the blocker for D18 (ML
  lists stored whole).
- A table is spread over six id-keyed maps (CatalogState.java:57-82), and on the
  wire its row (id, version, namespace, name, conflict state) is mixed with its
  content (catalog.proto `InlineTable`). Every fold is O(tables): `toBuilder`
  copies every top-level map (CatalogState.java:115-133) and `build` rebuilds
  the lookups and copies them again (:87-99, :1009).
- ProtoCodec is 2,654 lines mixing the wire format with replay's action
  semantics (verify and apply, :1700-2600).

Natural page boundaries: per table (no pool is shared between tables,
CatalogState.java:65-82); within a table, blocks by sequence-number range,
which are a function of the state (I11), stable under expiry (sealed snapshots
are immutable, I3a) and need no page-out record (v1 tables stay one block). What
crosses a block boundary: the pool, a list built from its parent's, inherited
parent extras, parent indices, and the snapshot log (in the metadata's JSON
head). In the root: namespace versions, name lookups, id counters, the encoding,
and each table's row and bounded conflict state.

What replay needs: for a touched table, its version, conflict state, refs,
schemas, specs and sort orders, last sequence number and next row id, recent
snapshots, the parent's list and pool lookups by path, and for manifest-window
its window (up to 1,000 snapshots); the set of every snapshot id (duplicate,
ref-target and parent checks); and old history for expiry and the builder path.
Everything of an untouched table can stay encoded until a load.

What is sound and should stay: the `InlineTable` bytes/fields duality;
`PersistentMap`, `Chain`, `Window`; `MetadataView` as the seam for a partly
loaded table; copy-on-write encoding state; whole-state accessors used only by
the encoder (one seam to replace); the key-sorted I11 discipline;
`CatalogMutation` reading only what it names.

### Where the hierarchy goes

Both reviews put it in the same place:

- The runtime owns the root. The log's checkpoint becomes a small root naming
  the committed set's pages and the state's pages by content digest. The SPI
  keeps a root as bytes, so P1-P6 hold for roots unchanged and I11 becomes
  "roots are byte-equal".
- Providers store pages: a put is durable and immutable; a get returns those
  bytes or fails.
- The state machine partitions, as a function of the state alone (a format
  constant, or moved by a record): a partition a writer configures lets two
  compactors write different roots at one position, which breaks I11.
- Pages are written before the root that names them, outside the commit's
  `write()` (a failed page put is "not sent", not an ambiguous write).
- Garbage collection is the hard part: content-addressed pages are reused, so
  a sweep can race a writer that puts the same page again before its CAS. A
  one-way scheme (sealed blocks that never reopen, a floor that only rises, as
  COMMITTED_PAGING.md has for the committed set) avoids a clock; a delete grace
  period longer than any commit is a clock assumption. A reader that finds a
  page missing fails its read; it never takes the page for empty.
- The TLA+ models learn pages: put, get, delete, roots naming them, and bug
  switches for a root before its pages, a sweep of a page an in-flight root
  names, a missing page read as empty, and a configuration-dependent partition.

Once the checkpoint is a small root, the object is a root and records, so a
read of it is small: most of what D15 (a read reads everything) and the rest of
D20 (four reads per commit) cost goes with it.

## Plan

### R0: no format change

1. D19 (whole-object compaction trigger) stopgap: the policy sees the
   checkpoint's size and the records' apart, and `max.append.size` bounds the
   records after the checkpoint. A checkpoint past it no longer compacts on
   every commit. Switch: `fileio.catalog.max.append.size.basis` = `tail`
   (default) or `object` (before). Done: `CompactionPolicy.Input` (records,
   checkpoint bytes, log bytes, record bytes), `Replica.checkpointBytes`;
   `TestProtoCommitKnobs`.
2. Memoize the checkpoint's per-table encoding: an inline table whose row,
   metadata, pool, lists and conflict state are the objects a decode made, or
   the last encode of the lineage encoded, writes the bytes it wrote then
   (decoding records each table's bytes as it decodes them). Under
   `fileio.catalog.replay.check` each reuse is checked against a fresh encode.
   Switch: `fileio.catalog.checkpoint.memoize` = `true` (default) or `false`.
   Done: `CheckpointMemo`, one per catalog's state machine, keyed weakly by
   each table's row (by identity), so that every line of states folded from
   one decode keeps its tables' encodings, the runtime check's whole replays
   beside a fold included, and a line no state continues drops them;
   `TestCheckpointMemo`. It holds the encoded bytes of the inline tables of
   each live line, about the inline part of a checkpoint per line.
3. One checkpoint step from a replica, memoized per replica, for create, replace
   and `compact()`: where every paging change will land, and no encode per
   throttled resend.

### R1: refactors, no format change

1. One immutable value per table, its row apart from its content; persistent
   top-level maps, so a fold is O(changed tables).
2. Fold on an identity (root digest, record count, last record) instead of the
   whole view; `refresh`'s fold keeps it (it drops the view today,
   LogRuntime.java:114).
3. Split ProtoCodec into the wire codec and the actions' semantics.

### R2: format work

1. Stable references within a table: lists as deltas of their parent's, with a
   whole list where a block starts, and a pool per block (D18, ML lists stored
   whole).
2. The root and pages: envelope v2, `SharedLog` pages, the committed set paged
   (D16, replay rewrites committed set; COMMITTED_PAGING.md), state pages by
   table and block (D18, D19), page GC, and the TLA+ models extended.
3. A cost-based, per-store policy fed by the runtime's store statistics (D14,
   commit knobs fixed).

## Ablation switches

For the paper's ablations, R0's changes each have a switch to the behaviour
before them (D20's fix, four reads per commit, has none yet):

| Property | Default | Before | Change |
|---|---|---|---|
| `fileio.catalog.max.append.size.basis` | `tail` | `object` | R0.1, D19 (whole-object compaction trigger) |
| `fileio.catalog.checkpoint.memoize` | `true` | `false` | R0.2 |

## Decisions open

1. Does replay ever read a page? Recommended: no. Records then carry what
   replay needs of sealed history, expiry applies natively (tombstones,
   rewritten at compaction) instead of through Iceberg's builder, and a window
   reaching into a sealed block conflicts (stable under S2, since blocks only
   become sealed). Loads and compaction read pages; replay does not.
2. Page GC: one-way sealing with a floor, or a grace-period sweep.
3. Whether an inline-ML pool is a pure dedup dictionary (entries for one path
   never differ, SPEC_ML.md:147-151), which would let blocks copy entries and
   let encode drop unreferenced ones; `CatalogState` counts same-path
   replacements (:700-707), so this needs checking.

Decided (2026-11-02): R0 now, with a switch for each change.
