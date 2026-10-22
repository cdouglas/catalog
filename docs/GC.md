# Committed-set GC

## Context

`LogCheckpoint.committed` (log.proto) is the per-catalog dedup set: the
UUIDv7 of every committed transaction GC has not dropped, compressed
(delta-varint timestamps, newest first). Replay uses it to apply a
record once, so a retried append whose first attempt landed changes
nothing, and a writer uses it to learn whether its commit landed.

Without a bound the set grows with the catalog's whole commit history.
GC (b1af354) bounds it to about retention × commit rate: each compaction
writes a GC record that drops the ids older than the retention window,
and records the newest time it dropped as the set's horizon. The set
belongs to the intention log, not to the catalog: its wire format, GC and
lookup are the same for any state machine on any provider, and are
specified in [LOG.md](LOG.md) §"The committed set and GC".

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

`CommittedSet.contains(UUID)` (intentlog-core; a read's set is
`Replica.committed()`, and the same method on its `Builder` also checks
the ids replay has added) answers:

- `true` — the id is in the set: the transaction committed.
- `false` — the id is not in the set, and its v7 timestamp is above
  the horizon (`highest_dropped_timestamp_ms`): presumed abort. The set
  would still hold it had it committed.
- throws `DedupHorizonExceededException` — the id is not in the set,
  and its timestamp is at or below the horizon: the set cannot tell an
  aged-out commit from one that never landed. `queryId()` and
  `horizonMs()` give the id and the horizon.

A non-v7 id is never in the set and never throws. `horizon()` returns
the horizon (0 before any GC).

## Where GC runs

GC is a record in the log: `LogRecord.advance_horizon`, an
`AdvanceHorizon{watermark_ms}` (log.proto). Replay applies one wherever
it lands (`CommittedSet.Builder.advanceHorizon` in intentlog-core, from
`Replay`): it drops every
id the set holds there, the checkpoint's and those replay added, at or
below the watermark, and raises the horizon to the newest one dropped.
The horizon moves nowhere else; it never decreases, and it can exceed
the newest id kept. A checkpoint holds exactly the set the replay before
it reached, so it is a replay of the log, whatever clock wrote it (I11).
That matters for a log whose checkpoints are stored apart from its
records: readers starting from different checkpoints of the same log
apply the same records, which a horizon filtered into a checkpoint by
its writer's clock would not guarantee.

Two writes carry a GC record; an append of an intention never does.
The watermark is `now - fileio.catalog.committed.retention.ms` (0 if the
retention is at least `now`, so `Long.MAX_VALUE` disables GC and writes
no record):

- On the catalog file, a CAS write (a compaction, or any commit in
  CAS-only mode) writes it between its checkpoint and its own record,
  capped below its own id (rule 2 below).
- On a log with checkpoints apart, `LogRuntime.compact()` appends it,
  reads through it, and checkpoints there; a writer whose commit landed
  runs it when the log is due, and so may a background compactor.

A GC record carries no id, is not deduped (replaying one again changes
nothing), and does not count toward `max.append.count`.

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
conflict, whose GC dropped the newer id of the commit it rebased on
(rule 2 now prevents the last). Replay used to ask
`containsTransaction` about every record, so one such record threw on
every read of the catalog, and nothing could compact it away, since
compacting reads first. Four rules keep the catalog readable and what
a commit reports true:

1. **Replay rejects it.** `Replay.replay` asks
   `CommittedSet.Builder.mayApply`: a record whose id the set holds is a retry, and
   one at or below the horizon may be a retry of a commit whose id aged
   out, so replay skips both.
2. **A CAS write's watermark stays below its own transaction's time.**
   `LogRuntime` clamps its GC record's watermark to one less than the
   time of the id it commits, so its own GC never raises the horizon to
   the record that follows it.
3. **Writers mint ids after the horizon they read.**
   `LogRuntime.commit` mints with
   `UuidV7.newUuidV7After(base.committed().horizon())`, so a writer
   whose clock is behind still commits.
4. **The commit loop maps it to an outcome.** `LogRuntime`'s `landed`
   catches `DedupHorizonExceededException` on any read the commit loop
   makes: the commit is rejected (`BEYOND_HORIZON`; the catalog throws
   `CommitFailedException`), since replay rejects its record from then
   on, unless a write of it may have landed before its id aged out, when
   it is unknown (`CommitStateUnknownException`). The exception does not
   escape a commit.

`TestCatalogRegressions.CommittedSetHorizon` covers each case (a CAS
after a conflict with retention 0, a record at the horizon, a writer
behind the horizon).

## GC on a shared log

On one object, a version's only GC record precedes every record appended
to it, so a writer that reads its record back finds the committed set and
replay in agreement. On a log with checkpoints apart, compactors append
GC records whenever they run, and three more cases follow
([LOG.md](LOG.md) §"The commit loop", found by `SharedLogCommit.tla`):

- **A GC record lands between a writer's record and its read back**, and
  drops the id of a record that applied. The committed set then cannot
  say, so a writer judges its record first by what replay did with it
  where it landed, and asks the set only if replay judged none.
- **A throttled send lands beside its retry**, a compactor checkpoints
  between the two copies, and a GC record drops the id. A read through
  the acknowledged copy starts from that checkpoint, where replay skips
  the copy as committed and the set cannot say. The writer counts the
  throttled send as a write that may have landed, so the commit is
  unknown, not failed.
- **A read back answers from a checkpoint that already holds the
  record**, whose id GC has dropped. The provider must return a view whose
  records include it (P3); the runtime treats one that does not as an
  ambiguous write and reads again.

A compactor's GC record has no commit of its own to stay below, so its
watermark can pass the id of a record still in flight, as a short
retention does on one object: replay rejects that record, and its writer
is rejected or unknown, never told it committed.

## Tests

- `TestCommittedSetGc` (intentlog-core): `CommittedSetCodec.rewrite`
  (pass-through, merge, drops at the watermark, horizon above the newest
  id kept, monotonic horizon), the tri-state lookup, and
  `Builder.advanceHorizon` (a GC record's replay: added ids dropped too,
  idempotent).
- `CheckpointRoundTrip`: every CAS write's checkpoint is the replay of
  the version it replaced, committed set and horizon included (I11),
  in the local suites and both fuzzers; on a shared log, every stored
  checkpoint is the replay of the log through it.
- `TestSharedLogRuntime` (intentlog-memory): the three shared-log cases
  above, each a schedule that fails with its rule removed, and copies of
  records whose originals GC compacted out or passed.
- The protocol fuzzers (`TestMemoryCommitProtocol`,
  `TestAtomicFileCommitProtocol`) and `TestMultiWriterProperties` run
  compactors with retention 0, inside commits too, and append copies of
  records that ever landed: none may apply, whether its original is a
  record, in a checkpoint, or past the horizon.
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
