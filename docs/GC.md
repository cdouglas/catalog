# Committed-set GC

## Context

`LogCheckpoint.committed` (log.proto) is the per-catalog dedup set: the
UUIDv7 of every committed transaction GC has not dropped, compressed
(delta-varint timestamps, newest first). Replay uses it to apply a
record once, so a retried append whose first attempt landed changes
nothing, and a writer uses it to learn whether its commit landed.

Without a bound the set grows with the catalog's whole commit history.
GC (b1af354) bounds it to about retention × commit rate: a CAS write
drops the ids older than the retention window, and records the newest
time it dropped as the set's horizon. The wire format and the policy
are in [SPEC.md](SPEC.md) §"Committed-set retention (GC)".

## Two oracles for "did my commit land?"

The catalog's dedup set is one of two complementary ways a writer can
confirm a commit:

1. **Dedup oracle (the catalog's).** A scan of the compressed set for
   the writer's UUIDv7. Bounded by retention: unknown past the horizon.

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

## Tri-state lookup

`ProtoCatalogFile.containsTransaction(UUID)` (and the same method on
its `Builder`, which also checks the ids replay has added) answers:

- `true` — the id is in the set: the transaction committed.
- `false` — the id is not in the set, and its v7 timestamp is above
  the horizon (`highest_dropped_timestamp_ms`): presumed abort. The set
  would still hold it had it committed.
- throws `DedupHorizonExceededException` — the id is not in the set,
  and its timestamp is at or below the horizon: the set cannot tell an
  aged-out commit from one that never landed. `queryId()` and
  `horizonMs()` give the id and the horizon.

A non-v7 id is never in the set and never throws. `highestDroppedTimestampMs()`
returns the horizon (0 before any GC).

## Where GC runs

Only a CAS write rewrites the checkpoint, so only a CAS write (a
compaction, or any commit in CAS-only mode) GCs; an append never does.
`ProtoCatalogFormat.Mut.buildFullFile` computes the watermark,
`now - fileio.catalog.committed.retention.ms` (0 if the retention is at
least `now`, so `Long.MAX_VALUE` disables GC), and passes it to
`ProtoCodec.encodeCheckpoint`. That calls
`CommittedSetCodec.rewrite` (intentlog-core) on the base state's set: one pass
over the compressed entries that drops every id at or below the
watermark and raises the horizon to the newest one dropped. The horizon
never decreases, and it can exceed the newest id kept.

`ProtoCatalogFile.Builder.build(long)` runs the same merge on a
builder's set, keeping the ids replay added; the commit path does not
use it (it builds with `build()`, no GC), and `TestCommittedSetGc`
covers it.

The retention defaults to 6 hours, longer than any commit's retries.
A retention shorter than a commit takes is legal but lossy. With a
retention of 0 (every CAS drops every id older than its own), a racer's
CAS can drop a committed id newer than an in-flight writer's
transaction id, or in the same millisecond; the horizon then covers the
writer's id, replay rejects its record, and the writer fails
(`CommitFailedException`) or, if an earlier write of it may have
landed, cannot learn its outcome (`CommitStateUnknownException`). The
catalog stays correct and readable; the writer learns less.

## Ids at or below the horizon (Bug 64)

A record whose id is at or below the horizon is reachable: a writer
whose clock is behind another's by more than the retention, one that
stalls that long between minting its id and appending, or, with a
retention of 0, a racer's CAS (above), or a writer's own CAS after a
conflict, whose checkpoint dropped the newer id of the commit it rebased
on (rule 2 now prevents the last). Replay used to ask
`containsTransaction` about every record, so one such record threw on
every read of the catalog, and nothing could compact it away, since
compacting reads first. Four rules keep the catalog readable and what
a commit reports true:

1. **Replay rejects it.** `ProtoCatalogFormat.replay` asks
   `Builder.mayApply`: a record whose id the set holds is a retry, and
   one at or below the horizon may be a retry of a commit whose id aged
   out, so replay skips both.
2. **A CAS write's watermark stays below its own transaction's time.**
   `buildFullFile` clamps it to one less than the time of the id it
   commits, so its own GC never raises the horizon to the record that
   follows the checkpoint.
3. **Writers mint ids after the horizon they read.**
   `ProtoCodec.Transaction.from` mints with
   `UuidV7.newUuidV7After(highestDroppedTimestampMs())`, so a writer
   whose clock is behind still commits.
4. **The commit loop maps it to an outcome.** `ProtoCatalogFormat.Mut.landed`
   catches `DedupHorizonExceededException` on any read the commit loop
   makes: the commit fails (`CommitFailedException`), since replay
   rejects its record from then on, unless a write of it may have
   landed before its id aged out, when it is unknown
   (`CommitStateUnknownException`). The exception does not escape a
   commit.

`TestCatalogRegressions.CommittedSetHorizon` covers each case (a CAS
after a conflict with retention 0, a record at the horizon, a writer
behind the horizon).

## Tests

- `TestCommittedSetGc` (intentlog-core): `CommittedSetCodec.rewrite`
  (pass-through, merge, drops at the watermark, horizon above the newest
  id kept, monotonic horizon), the tri-state lookup, and
  `Builder.build(long)`; the catalog's `TestCommittedSetGc` checks that
  `ProtoCatalogFile` delegates to it.
- `TestCommittedTransactions` (intentlog-core): round trip and defensive
  decode of the compressed set; the catalog's, through the checkpoint.
- `TestProtoCommitKnobs`: the retention property, and end to end, a
  lookup of a committed id that throws after another writer's CAS with
  retention 0.
- `TestCatalogRegressions.CommittedSetHorizon`: Bug 64.
- The fuzzers run writers with retention 0: `TestCatalogFaultProperties`
  (`gcWriters`, one bit per writer) and `TestCatalogRaceProperties`
  (`gc`), whose oracle accepts a failed or unknown operation only if
  the race ends as a serial run with it or without it.
- The format explorer's committed-set GC scenario shows the horizon;
  regenerate its fixtures with
  `mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true`.
- `CommittedTxnsBench` (JMH, in intentlog-core): set size and lookup cost.

## Out of scope

- Snapshot-oracle helper in the catalog. The writer-side pattern is
  documented in §"Two oracles" but no catalog API is added — the
  reverse index would duplicate state the writer already has.
- Retention tuning per workload. The 6h default is documented; users
  can override via the property, no per-table or per-namespace knob.
