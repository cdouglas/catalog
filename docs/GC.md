# Committed-set GC: implementation plan

## Context

`Checkpoint.committed_transactions` is the per-catalog dedup set: every UUIDv7
of every committed transaction across the catalog's lifetime, used so a
writer can read a later checkpoint and confirm its commit landed.

The set is currently unbounded. Even with the v7 compression already in
place (commits `ea41e18` … `a19051a`), it scales linearly with total commit
history — fine for short-lived test catalogs, eventually dominant for
long-lived deployments.

The retention policy was designed in `docs/SPEC.md` ("Committed-set
retention (GC)") and tracked as **errata D11**. This document is the
implementation plan that turns the design into code. Errata **D10**
(single-pass `committedTransactionsBytes` rewrite) is folded in here since
the GC pass and the merge pass are the same loop.

## Two oracles for "did my commit land?"

The catalog's dedup set is one of two complementary ways a writer can
confirm a commit:

1. **Dedup oracle (this work).** Fast O(log N) scan of the compressed set
   for the writer's UUIDv7. Bounded by retention — UNKNOWN past the
   window.

2. **Snapshot oracle.** For data commits, the writer remembers which
   tables it wrote to and the snapshot ID it installed in each. To
   confirm post-retention, it loads any one of those tables and looks
   for the snapshot ID. Multi-table atomicity guarantees the rest:
   **finding any installed snapshot proves all of them landed**, since
   either the whole transaction was applied or none was. For
   namespace/table/property mutations there's no snapshot, but the
   writer can check the entity directly (does the namespace exist?
   does the property hold the new value?).

The catalog only implements the dedup oracle. The snapshot-oracle
fallback is purely a writer-side pattern, not catalog plumbing — there
is no "given a UUID, find the tables it touched" reverse index in the
catalog and there shouldn't be (the writer already knows what it
committed). This pattern is documented so retention can be set
aggressively without losing absolute confirmability for data writers.

## Implementation phases

### Phase A — wire-format expansion

Add `highest_dropped_timestamp_ms` to `CommittedTransactions` (field 3,
fixed64). Empty / absent ↔ value 0 ↔ "no GC has happened yet."

Files:
- `src/main/proto/catalog.proto` — add the field with comment.
- `src/main/java/org/apache/iceberg/io/ProtoCodec.java`:
  - New constant: `COMMITTED_TXNS_HIGHEST_DROPPED_TIMESTAMP_MS = 3`.
  - `encodeCommittedTransactions` writes the field when non-zero.
  - `decodeCommittedTransactions` reads it; round-trip helpers expose it.
  - `committedSetContains` ignores the field (bool API unchanged); a new
    `committedSetStatus(byte[], UUID) → TxnDedupStatus` uses it.
- `src/test/java/org/apache/iceberg/io/FormatExplorerFixtures.java` — drift
  check picks up the new field automatically via the `messageToPrefix`
  map already pointing `CommittedTransactions` → `COMMITTED_TXNS`.

### Phase B — single-pass `committedTransactionsBytes` rewrite

Replace `Builder.materializeCommittedBytes()` with a streaming merge that
both:
- absorbs the in-flight additions (`addedCommittedTransactions`), and
- drops inherited entries whose UUIDv7 timestamp is `<= dropBeforeTimestampMs`,
  updating `highest_dropped_timestamp_ms` for each drop.

New helper (place in `ProtoCodec`):
```
byte[] rewriteCommittedBytes(byte[] inheritedBytes,
                             Collection<UUID> addUnconditionally,
                             long dropBeforeTimestampMs)
```

Algorithm:
1. Sort `addUnconditionally` descending into a UUID array (small, bounded
   by an in-flight Mut — typically a few entries).
2. Stream-decode `inheritedBytes`: read `max_timestamp_ms`, then walk
   `packed_entries` one (delta-varint, 10 random bytes) tuple at a time
   while tracking the cumulative timestamp.
3. Two-pointer merge against the additions array. At each step emit the
   larger UUID (descending). Inherited entries with `timestamp_ms <=
   dropBeforeTimestampMs` are *not* emitted; instead update
   `newHighestDropped = max(prevHighestDropped, timestamp_ms)`. Added
   entries are emitted unconditionally even if their timestamp is below
   the watermark — per the design constraint that log transactions are
   never dropped.
4. The first emitted UUID's timestamp becomes the new `max_timestamp_ms`.
5. Emit the new wire bytes: `max_timestamp_ms` + `packed_entries` (now
   the merged stream) + `highest_dropped_timestamp_ms`.

Builder API:
- `materializeCommittedBytes(long dropBeforeTimestampMs)` — replaces the
  no-arg version. Builders constructed from a decoded checkpoint pass
  `dropBeforeTimestampMs = 0` when no GC pass is desired (read-only
  rebuild, no compaction); the compaction path passes `now - retentionMs`.
- `ProtoCatalogFile constructor` calls `builder.materializeCommittedBytes(0)`.

### Phase C — compaction integration

Compaction is the only place GC fires (per the SPEC: append-mode commits
don't rewrite the checkpoint). The Explore confirmed the entry point:
`ProtoCatalogFormat.Mut.buildFullFile()` at `ProtoCatalogFormat.java:711–729`,
which calls `ProtoCodec.encodeCheckpoint(original, mut, idManager)` at
line 719. Inside `encodeCheckpoint`, the `original.committedTransactionsBytes()`
pass-through (currently `ProtoCodec.java:285–288`) becomes the GC site.

Two integration knobs:

1. The format passes its `committedRetentionMs` to `encodeCheckpoint`. New
   signature: `encodeCheckpoint(original, mut, idManager, dropBeforeTimestampMs)`.
   `dropBeforeTimestampMs = System.currentTimeMillis() - committedRetentionMs`,
   computed at the call site in `tryCAS`/`buildFullFile`.

2. Inside `encodeCheckpoint`, replace the current pass-through:
   ```
   byte[] committedBytes = original.committedTransactionsBytes();
   if (committedBytes.length > 0) writeLengthDelimited(...);
   ```
   with a call to `rewriteCommittedBytes(originalBytes, mut.additions(),
   dropBeforeTimestampMs)`. The Mut exposes its accumulated
   committed-txn additions (this requires a small `Mut` API change —
   today the additions live on `Builder`, accessed via `format.from(current)`;
   for compaction the relevant additions are the just-applied transactions
   in the log being compacted).

   Actually clearer: the `Mut.commit` flow already calls
   `Builder.build()` to produce the post-commit `ProtoCatalogFile`. The
   GC happens inside that `build()` via `materializeCommittedBytes(watermark)`.
   So we plumb `watermark` into the Mut → Builder path:
   - `Mut` learns `committedRetentionMs` from `ProtoCatalogFormat` at
     construction.
   - `Mut` computes `watermark = now - retentionMs` at the moment of
     `build()`.
   - `Mut` calls `builder.setDropBeforeTimestampMs(watermark)` before
     `builder.build()`, OR we add an overload `builder.build(watermark)`.

   The cleaner shape: `Builder.build(long dropBeforeTimestampMs)`. The
   no-arg `build()` calls `build(0)` (no GC, used by tests and decode).

### Phase D — configuration knob

Mirror the existing `maxAppendCount` / `maxAppendSize` pattern in
`ProtoCatalogFormat.java`:

```java
public static final String COMMITTED_RETENTION_MS =
    "fileio.catalog.committed.retention.ms";
public static final long DEFAULT_COMMITTED_RETENTION_MS = 6L * 60 * 60 * 1000;
```

Constructor variants:
- `ProtoCatalogFormat()` — default retention.
- `ProtoCatalogFormat(Map<String, String> properties)` — parse
  `COMMITTED_RETENTION_MS` (long, ms) via the existing `parseLongProperty`.
- `ProtoCatalogFormat(int maxAppendCount, long maxAppendSize, long
  committedRetentionMs)` — direct.

Validation: `committedRetentionMs >= 0`. `0` means "drop everything but
the just-added entries" (degenerate but legal); `Long.MAX_VALUE`
disables GC.

### Phase E — query API: presumed-abort with throw on unknown

Keep `containsTransaction(UUID) → boolean`. Semantics change from
"two states (in/out)" to "two states with an escape hatch":

- `true` — UUID found in the compressed set. Definitively committed.
- `false` — UUID not found AND `timestamp(uuid) > highest_dropped_timestamp_ms`.
  **Presumed abort**: no record exists, and we know we *would* still
  have it if it had committed (it's newer than the GC horizon). Caller
  treats this as "did not commit."
- *throws* `DedupHorizonExceededException` (new, unchecked) — UUID not
  found AND `timestamp(uuid) <= highest_dropped_timestamp_ms`. The GC
  horizon has overtaken the queried UUID; we can no longer distinguish
  "was committed and aged out" from "never committed." Caller must fall
  back to a different oracle (the snapshot-oracle pattern documented in
  §"Two oracles") or treat as a hard error.

The exception carries the queried UUID and the catalog's
`highest_dropped_timestamp_ms` for diagnostics:
```java
public class DedupHorizonExceededException extends RuntimeException {
  public final UUID queryId;
  public final long horizonMs;
  // ...
}
```

`ProtoCatalogFile`:
- `boolean containsTransaction(UUID u)` — implementation:
  1. Scan `committedTransactionsBytes` for `u` (existing
     `committedSetContains` logic, single pass — also reads
     `highest_dropped_timestamp_ms` along the way).
  2. If found → return `true`.
  3. Else if `timestampMs(u) > highestDropped` → return `false`.
  4. Else → throw `DedupHorizonExceededException`.
- `long highestDroppedTimestampMs()` — accessor for callers building
  bespoke logic (e.g., the JMH benchmark, or pre-flight checks).

`Builder.containsTransaction(UUID u)`:
- Check `addedCommittedTransactions` first → `true` if present.
- Else delegate to the scan-on-bytes path with the same throw semantic.

**Replay-path safety.** `readInternal` calls `builder.containsTransaction(txn.id())`
for every appended txn during log replay (idempotency check). The just-appended
txn's UUID was minted at append time — minutes ago at most — and the watermark
is hours back, so the throw branch is unreachable in normal operation. If a
malformed/skewed writer manages to append a UUID whose timestamp is below the
watermark, the exception surfaces the bug rather than silently mis-deduping.

## Tests

New test class `TestCommittedSetGc` (or extend `TestProtoCommitKnobs` with
a nested class — the MockIO infrastructure is already there).

Round-trip and merge:
- `rewriteCommittedBytes` with empty inputs round-trips to empty.
- `rewriteCommittedBytes` with no additions and no drops returns
  byte-identical to input (pass-through).
- `rewriteCommittedBytes` with additions only (watermark = 0) merges
  in sorted descending order; result decodes to the union.
- `rewriteCommittedBytes` with watermark drops only entries whose
  timestamp ≤ watermark; emitted entries match expected; updated
  `highest_dropped_timestamp_ms` is `max(inheritedDropped, max
  timestamp of dropped entries)`.

Constraint tests:
- **Log entries with timestamps below the watermark are preserved.**
  Construct an inherited set [T-12h] (stale), additions {T-10h}
  (stale-from-clock-skew), watermark = T-6h. Result: inherited entry
  T-12h is dropped, addition T-10h survives, max_timestamp_ms = T-10h,
  highest_dropped = T-12h. **highest_dropped > max_timestamp_ms** —
  exercises the rare-but-legal edge case.
- Monotonicity: across two consecutive compactions, `highest_dropped`
  only increases.

Query semantics:
- `containsTransaction` returns `true` for a UUID in the set.
- Returns `false` for a UUID with `timestampMs > highest_dropped` not
  in the set (presumed abort).
- Throws `DedupHorizonExceededException` for a UUID with
  `timestampMs <= highest_dropped` not in the set; exception carries
  the queried UUID and the horizon.

End-to-end after-compaction lookup with retention:
- Extension of existing `writerLearnsCommitOutcomeAfterCompaction`
  (`TestProtoCommitKnobs.java`).
- Default 6h retention: `containsTransaction(txn_X) == true` after a
  CAS-rewrite that includes `txn_X`. Same as the existing test.
- Aggressive retention (`committedRetentionMs = 0` + a small clock
  advance / sleep): every CAS commit GCs all prior entries. Writer A
  commits txn_X; later compaction's watermark moves past txn_X's
  timestamp; fresh read of `containsTransaction(txn_X)` throws
  `DedupHorizonExceededException`. Verify the exception's
  `queryId` and `horizonMs` fields.
- Negative case for a definitely-uncommitted UUID with timestamp above
  the horizon: `containsTransaction(neverCommitted) == false` (no
  throw).

Format-explorer:
- Regenerate fixtures; the post-CAS scenario will now show the
  `highest_dropped_timestamp_ms` field. The existing post-decode hook
  reading `packed_entries` is unaffected; add a small additional line
  in the detail panel surfacing `highest_dropped_timestamp_ms` when
  non-zero.

## Files to modify

| File | Change |
|---|---|
| `src/main/proto/catalog.proto` | Add `fixed64 highest_dropped_timestamp_ms = 3` to `CommittedTransactions`. |
| `src/main/java/org/apache/iceberg/io/ProtoCodec.java` | New constant; encode/decode the new field; add `rewriteCommittedBytes` (single-pass merge); change `committedSetContains` to throw on horizon-exceeded misses (or expose a helper that returns the parsed `(found, horizon)` so the caller can decide). |
| `src/main/java/org/apache/iceberg/io/ProtoCatalogFile.java` | `Builder.build(long)` overload; `Builder.materializeCommittedBytes(long)` taking watermark; `containsTransaction` throws `DedupHorizonExceededException` on UNKNOWN; `highestDroppedTimestampMs()` accessor. |
| `src/main/java/org/apache/iceberg/io/ProtoCatalogFormat.java` | New constants `COMMITTED_RETENTION_MS` / `DEFAULT_COMMITTED_RETENTION_MS`; constructor & properties parse; thread `committedRetentionMs` to `Mut`; `Mut` computes watermark and passes to `Builder.build`. |
| `src/main/java/org/apache/iceberg/io/DedupHorizonExceededException.java` *(new)* | Unchecked exception with `queryId` and `horizonMs` fields. |
| `src/main/java/org/apache/iceberg/io/CatalogFile.java` | Update `containsTransaction` Javadoc to document the throw semantic. |
| `src/test/java/org/apache/iceberg/io/TestCommittedSetGc.java` *(new)* | Round-trip, merge, watermark, tri-state, edge-case tests. |
| `src/test/java/org/apache/iceberg/io/TestProtoCommitKnobs.java` | Extend `writerLearnsCommitOutcomeAfterCompaction` with retention=0 / retention=default variants. |
| `docs/SPEC.md` | Mark "Committed-set retention (GC)" status from "designed" → "implemented"; remove the TODO link to errata D11. |
| `docs/errata.md` | Strike D10 and D11 (or annotate as resolved). |
| `docs/format-explorer/index.html` | Optional: surface `highest_dropped_timestamp_ms` in the CommittedTransactions detail panel. |
| `src/test/java/org/apache/iceberg/io/FormatExplorerFixtures.java` | Regenerate fixtures (drift check carries through automatically). |

## Verification

- `mvn clean test` — all unit tests pass; new tests exercise GC paths.
- Existing JMH `CommittedTxnsBench` still runs; sizes/lookup latencies
  unchanged for the no-GC path. Optional: extend benchmark with a
  `dropBeforeTimestampMs` axis to characterize merge cost vs add cost.
- `mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true` +
  `python3 docs/format-explorer/splice-fixtures.py` — fixtures
  regenerate cleanly; explorer renders the new field.
- Open `docs/format-explorer/index.html`; confirm the post-CAS scenario
  shows `highest_dropped_timestamp_ms` (when non-zero) and the
  reconstructed UUIDs survive the GC pass.
- Manual check: build the catalog file with retention=0, commit two
  transactions, compact, verify only the most recent UUID survives in
  the dedup set and `highest_dropped_timestamp_ms` is set.

## Out of scope for this work

- Snapshot-oracle helper in the catalog. The writer-side pattern is
  documented in §"Two oracles" but no catalog API is added — the
  reverse index would duplicate state the writer already has.
- Retention tuning per workload. The 6h default is documented; users
  can override via the property, no per-table or per-namespace knob.
- Migration of pre-GC catalog files. None exist; this is a research
  prototype.
