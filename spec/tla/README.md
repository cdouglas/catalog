# TLA+ models of the commit protocol

Two models of the commit loop (`LogRuntime.commit`, formerly
`ProtoCatalogFormat.Mut.commit`) over one replicated state machine (`RSM.tla`):

- `CatalogCommit.tla`: one catalog object in storage with conditional writes
  (`AtomicFileLog`). `CatalogOracles.tla` states the jqwik fuzzers' oracles as
  predicates over it. Its purpose is to check the oracles: that what the
  property tests assert is enough to imply the protocol's properties, and that
  each historical protocol bug breaks a property and is caught by an oracle.
  The sections from [The file model](#the-file-model) to
  [The oracles](#the-oracles) are about it.
- `SharedLogCommit.tla`: a shared log with checkpoints apart from its records
  (Kafka, SQS, a ledger, `InMemorySharedLog`). It was written before the Java it
  validates, to say what that Java must do; see
  [The shared-log model](#the-shared-log-model).

Both models also page the committed set as `docs/COMMITTED_PAGING.md` designs it:
the floor, PageOut records, page puts and sweeps. That part was written before
its Java (stage 2 of the design's order of work); see
[Paging the committed set](#paging-the-committed-set).

The model reproduces the commit loop, not Iceberg. Table metadata (inline TM and
ML, deltas, row lineage, DVs), byte encoding, SDK error mapping and performance
are out of scope.

| File | What it holds |
|---|---|
| `RSM.tla` | The state machine and replay (`Replay`; `CommittedSet` with its blocks, floor and pages; `landed`'s verdict and page lookups), and the serial execution the properties compare with |
| `CatalogCommit.tla` | The protocol: storage, the commit loop, faults, GC, the bug switches, the properties |
| `CatalogOracles.tla` | The three fuzzers' oracles, their tightened versions, and `Adequate` |
| `MC.tla` | Scenarios (what each writer commits) and the coverage witnesses |
| `SharedLogCommit.tla` | The shared-log protocol: the log, checkpoints and trims, writers, compactors, faults, the bug switches, the properties |
| `SharedLogOracles.tla` | Oracles a fuzzer of the shared log can assert, and `Adequate` |
| `MCShared.tla` | Its scenarios and coverage witnesses |
| `check.py` | The configurations of both models (`log-*` for the shared log), and what TLC must report for each |

## Running

```bash
mkdir -p spec/tla/tools
curl -L -o spec/tla/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

spec/tla/check.py               # every configuration; each must give its expected verdict
spec/tla/check.py -k bug        # the configurations whose names contain "bug"
spec/tla/check.py -k log-       # the shared-log model's
spec/tla/check.py -n page,log-page  # exactly these configurations
spec/tla/check.py --witnesses   # every branch and interleaving listed below is reached
spec/tla/check.py --matrix      # the oracle matrices of both models (about 20 minutes)
spec/tla/check.py --matrix --tight  # the tightened oracles on the same cells
```

`check.py` writes each configuration to `states/cfg/<name>.cfg` and TLC's output
to `states/out/<name>.txt` (both ignored by git). To rerun one by hand, from
`spec/tla`:

```bash
java -cp tools/tla2tools.jar tlc2.TLC -workers 2 -config states/cfg/mix.cfg MC
java -cp tools/tla2tools.jar tlc2.TLC -workers 2 -config states/cfg/log-mix.cfg MCShared
```

`TLC_WORKERS` (default 2) and `TLC_HEAP` (default 2g) size the runs. Checked with
TLC 2026.10.13.164326 (rev 210df3a), the version that jar reported. The whole
table takes about 40 minutes. Most configurations take seconds; `mix`,
`mix-lenient`, `skew`, `log-mix` and `log-missingfail` take 3 to 6 minutes
each, and `log-three` 8 to 11.

## The file model

**Storage.** One object: `[gen, chk, log, segs]`. `chk` is the checkpoint: the
state, the committed set `C` and its GC horizon `H`. `log` holds the records
appended since, and `segs` the writes since the object was created. A record
is a transaction or a GC record, `[wm]` (log.proto's `AdvanceHorizon`): replay
drops the committed ids at or below `wm` and raises `H` to the newest it drops,
and nothing else moves `H`. A CAS writes the checkpoint of the state it
replayed, unfiltered, then a GC record at its watermark, then its own record
(`CasPayload`). The append count (`cnt`) counts transactions only. Every write
gets a fresh generation, and every conditional write names the generation it
expects. An append past `SegLimit` writes is refused.

**State** (`RSM.tla`). Each entity (namespace or table, alike) has a version. A
transaction reads some entities at the versions its snapshot had, writes some of
them, and may create an entity under one it writes. Replay applies a record if
`mayApply` admits its id (not in `C`, above `H`) and its reads still hold
(`Intention.verify`). The checkpoint carries a ghost history: the records
replay applied, in order. The properties are stated over it.

A commit judges its own record as `LogRuntime.landed` does: by what replay did
with it first (applied or rejected), and only if replay did not judge it, by the
committed set. On one object a GC record precedes every record of its version,
so the two agree: judging by the committed set alone reached the same states in
every configuration.

**Writers.** Each writer runs its operations in turn. Each writer action is
one storage request, so other writers interleave between any two. Faults are
nondeterministic, within a budget (`MaxFaults`):

| Fault | What happens |
|---|---|
| `failBefore` | an IOException before the write lands |
| `lostReply` | the write lands; its reply is lost |
| `landedConflict` | the write lands; a transport retry of it fails its precondition |
| `throttled` | throttled; the write did not land |
| `landedThrottled` | the write landed, then was throttled |
| `inflight` | the request times out but stays in flight, and may land later (`LandInflight`) |
| `readFail` | a read fails |

Clocks tick (`Tick`), each writer has a skew, and a CAS writes a GC record at
its watermark.

**Schedules.** `Sched = "free"` allows any interleaving. The other modes give the
schedules of one fuzzer each: operations run one at a time, and another may run
to completion inside one.

- `"mw"` (TestMultiWriterProperties): one operation, before any storage request, on a snapshot taken earlier.
- `"fault"` (TestCatalogFaultProperties): one operation, before the first write.
- `"race"` (TestCatalogRaceProperties): writer 1 is A, and each racer runs fresh, just before A's k-th write.

Operations (`retry`) are restaged on a fresh read after `CommitFailedException`,
as FileIOCatalog and Iceberg do. Catalog transactions are not.

### Actions, code, and the jqwik checks that cover them

Line numbers are of `LogRuntime.java` at 7e056b3 unless stated. `TestLogRuntime`
(intentlog-core) drives each branch on a scripted log; each test names its action.

| Action | Code | Covered by |
|---|---|---|
| `Begin`, `Stage` | `CatalogMutation.intention()` (`CatalogMutation.java:398`); id minted after the read's horizon :324 | all three fuzzers |
| `Start` | `commit` :202, `loop` :348 | all |
| `Write` (CAS) | `log.replace` :402, `records` :561 (GC watermark :562), `write` :582; `AtomicFileLog.write` (`AtomicFileLog.java:167`) | all |
| `Write` (APPEND) | `log.append` :426, `write` :582 | all |
| `Write` with `SegOK` false | `catch LogFullException` :427 (Bug 66) | MW, fault (segment limits drawn) |
| `Write` with faults | `write` :582; the store's signals mapped by `AtomicFileLog.write` | MW, fault (`InMemoryAtomicFileIO.Fault`) |
| `ReadBack` | `write`'s `readThrough` :613, `validate` :720; `AtomicFileLog` answers it from the write when the write says what the object holds (a replace; an append from a tail whose bytes it read or wrote), with no request: the same view, since every write gives the object a version of its own (errata D20) | all; strict versions: MW, fault |
| `Rebase` | `catch LogConflictException` :403 and the read after it :411 | all |
| `Refresh` | `catch LogConflictException` :434, `probe().continues` :440 | all |
| `Shrink` | the read when the log was replaced :444 | MW |
| `FreshForCAS` | `mustReplace` with `stale` :385 (Bugs 12, 26) | MW |
| `Resolve` | `catch AmbiguousWrite` :458 | MW, fault |
| `Exhaust`, `FinalRead` | after the loop :491, :494 | MW (faults) |
| `Unreadable` | `catch UnreadableLog` :480 | MW, fault (read faults) |
| `Judge` | `landed` :694; `CommittedSet.Builder.contains` (`CommittedSet.java:142`) | MW, fault, race (gc) |
| `Restage` | `LogCatalog.commitOnAFreshRead` (`LogCatalog.java:281`); Iceberg's table commit retry | fault, race |
| `Replay`, `Fold` | `Replay.replayOnto` (`Replay.java:127`), `mayApply` (`CommittedSet.java:151`), `CatalogIntention.verify` (`ProtoCodec.java:1560`) | all (every read), `CheckpointRoundTrip` |
| `LandInflight` | none: a timed-out request that lands later | none |
| `Tick`, `Skew` | the runtime's clock (`RuntimeConfig.clockMs`) | gc runs (wall clock); no fuzzer skews clocks |

Not modelled: creating the catalog file (`commit()`'s case 0), the size of
records (`MaxSize` counts records, and a record larger than one append stores
goes in a replace, `oversized` in `mustReplace`: Bug 71, covered by
`TestLogRuntime` and `TestCatalogRegressions`), and backoff timing.

### Properties

| Property | Statement |
|---|---|
| `Readable` | every version of the catalog replays |
| `ExactlyOnce` | no transaction applies twice |
| `Serializable` | the catalog is the serial execution of the transactions applied, each one's reads (as its snapshot had them) holding where it applies, creates binding fresh ids |
| `UniqueIds` | no id names two entities |
| `HonestCommitted` | a commit reported committed applied, and stays applied, through compaction and GC |
| `HonestFailed` | a commit reported failed, or any error but unknown, is not applied, now or later (a late landing included) |
| `UnknownExplained` | an unknown outcome follows a fault, or a read that put the id at or below the horizon |
| `SoloCommits` | a commit no other write raced, on storage that did not fail, commits |
| `StampsCount` | S3: each entity's version counts the records that changed it, so it only grows |
| `HistGrows` (action) | the history only grows, one transaction at a time |
| `CasAppliesItsRecord` (action) | a CAS applies its own record when the record holds on the checkpoint it wrote |
| `CheckpointIsReplay` (action) | a CAS's checkpoint is the replay of the version it replaces: a function of the log, not of its writer's clock (I11) |

## Results

Every configuration passes. The state counts in this file's tables are from
`check.py`'s full run at 4c685a3 (2026-11-11: 138 configurations, about six
hours on the dev box):

| Config | What it adds | States |
|---|---|---|
| `base` | two writers contending; append, then compaction by count | 61 |
| `cas-only`, `gcs`, `size` | max.append.count 0; no append support; compaction by size | 55, 55, 61 |
| `two-each`, `long` | several transactions per writer: compaction racing appends, the shrink path | 953, 542 |
| `creates` | creates in different namespaces | 61 |
| `faults`, `faults-strict`, `faults2` | every fault, late landings included; strict versions; two faults | 558; 639; 104,218 |
| `gc0`, `skew` | retention 0; a writer's clock behind by more than the retention | 21,777; 603,529 |
| `seg` | a segment cap below max.append.count | 77 |
| `mix`, `mix-lenient` | everything at once | 349,273; 584,711 |
| `long-faults`, `three` | faults with the shrink path; three writers | 53,032; 43,118 |
| `sched-*` | each fuzzer's schedules | 23 to 6,882 |
| `concerns`, `concerns-faults` | conflict encodings: a table's concerns as entities (below); with every fault | 1,995; 11,071 |
| `blind-appends` | two blind appends, with every fault | 577 |
| `validated` | three validated commits on partitions (below) | 2,079 |

**Conflict encodings.** A conflict encoding (fileio-catalog's
`ConflictEncoding`) versions a table by its concerns. In the model the
concerns are entities: `CEnts` holds `v`, the table's version, which every
change of the table writes, and two concerns, `props` and `main`. A property
change reads `props` and an append reads `main` (concern-vector's witnesses),
so either applies after the other; a change an encoding cannot see into
reads `v`, the Lamport witness. A blind append (blind-append) reads nothing:
`T({}, {"main", "v"})`, which `Verify` admits anywhere and only the record's
id keeps from applying twice. A validated commit (partition-vector,
manifest-window) reads the partitions its checks cover and writes those it
changes: with `PEnts`, commits on `p1` and `p2` apply in either order, and
two on `p1` conflict, whichever lands second (`validated`). That the stamps'
judgments are sound rests on
S3, that a version counts the changes, which `StampsCount` states: the
switch `basever` sets an entity's version to one past its version where the
record was staged (`wv`), as a table version set to the record's base plus
one would after an encoding admitted the record on a later version.

**Calibration.** Each bug switch puts back a bug the fuzzers or reviews found
(Bug N in the commit subjects). Each one breaks a property:

| Bug | Switch | Caught by |
|---|---|---|
| 10 | writers bind new ids from their snapshot | `Serializable` (the old model's MC_ids_bug) |
| 12 | a CAS checkpoints the writer's snapshot, not the version it replaces | `HistGrows` |
| 16 | a record omits the preconditions of what it only reads | `Serializable` |
| 17 | an ambiguous write is reported as a failure | `HonestFailed` |
| 18 | a precondition failure is taken as proof that the write did not land | `HonestFailed` |
| 25 | a write throttled until the retries run out is reported as a failure | `HonestFailed` |
| 26 | the CAS after a lost append reads the version `current` was bound to | `UnknownExplained` |
| 64a | replay asks `containsTransaction`, which throws at the horizon | `Readable` |
| 64b | the CAS watermark is not capped below its own record | `CasAppliesItsRecord` |
| 64c | ids are minted without regard to the horizon | `CasAppliesItsRecord` |
| 64d | `DedupHorizonExceededException` escapes the commit | `HonestFailed` |
| 66 | `SegmentLimitException` is taken as ambiguous | `UnknownExplained` |
| `ckgc` | not a bug: the design before GC records, a CAS whose checkpoint is filtered at its writer's watermark | `CheckpointIsReplay` |
| `basever` | not a bug: an entity's version set from the one its record was staged on (a conflict encoding's table version set to the record's base + 1) | `StampsCount` |

**Coverage.** `check.py --witnesses` checks, for each `Not_` predicate in
`MC.tla`, that some passing configuration violates it, so that configuration
reaches the branch:

- the shrink path, the CAS after a fresh read, a CAS rebase, a stale append retry;
- throttle retries, a full segment, the final read after the last attempt;
- a record twice in the log (so exactly-once is not vacuous); a GC record in the log;
- a failed commit whose record landed, a horizon, unknown outcomes with and without faults;
- the log past max.append.count;
- preemption on each fuzzer's schedules, two racers, restages, A failing its race;
- a timed-out write that can land after its commit reported;
- a writer's clock behind the horizon;
- a writer resolving a lost reply after GC dropped its id.

**Interleavings the fuzzers do not produce.** No counterexample was found in any of
these, so no new `TestCatalogRegressions` test:

- the free schedules (several writes interleaved, three writers);
- late landings;
- clock skew past the retention;
- lost replies resolved after GC;
- a full segment racing a CAS (`mix`).

## The oracles

`check.py --matrix` runs, for each fuzzer's oracle on that fuzzer's schedules
and each bug switch, three checks:

- does the bug break a property on those schedules (it is reachable);
- does the oracle report it (`OracleHolds`);
- does some run pass the oracle with a property broken (`Adequate` fails).

Each fuzzer is run only with what it draws. The race property injects no
faults, reads leniently and draws no segment cap. `race1` is the race with
one racer: with two, in this two-entity model, A always conflicts.

| Bug | MW | fault | race / race1 |
|---|---|---|---|
| 10 | caught | caught | caught |
| 12, 16 | caught | caught | caught (race1) |
| 17, 18, 25, 64d | caught | caught (64d: the raw exception is "unexpected") | not drawn (no faults) |
| 26 | caught | unreachable (preemption only before the first write) | not drawn (lenient reads) |
| 64a | caught | caught | caught |
| 66 | caught | **missed** | not drawn (no segment cap) |
| 64b | **missed** | **missed** | **missed** |
| 64c | **missed** (needs clock skew) | **missed** (needs clock skew) | unreachable |

On the correct model, every oracle passes on every schedule. `Adequate` holds
everywhere except the missed cells.

### What the model found

1. **The commit protocol holds** on every configuration, including the
   interleavings and faults no fuzzer produces.
2. **TestCatalogFaultProperties accepts an unknown outcome without a fault.**
   Its `resolve` accepts an unknown outcome if the catalog is with or without
   the operation. Bug 66 (an unknown outcome on a full segment) is reachable on
   its schedules and passes it; the multi-writer test catches it with its rule
   that an outcome is unknown only after an injected fault. (The Java test did
   fail on Bug 66, but only because its own setup's writes fill a segment limit
   of 3; with a limit of 7 it passes with the bug in place.)
3. **No oracle asserts that an uncontested commit succeeds.** Bugs that cause only
   spurious failures pass all three oracles. Bug 64b is one: with retention 0,
   a CAS whose own watermark reached its record rejected it, and a writer
   committing twice in the same millisecond hit it alone. Bug 64 was found by
   review, not by a fuzzer. The multi-writer test counts such rejections in its
   statistics ("spurious") but does not fail on them.
4. **The race property's gc relaxation is too broad.** With gc, A may fail or be
   unknown, and the race passes if it ends as a serial run with or without A. That
   masks Bug 64b. It would also accept A reported as `CommitFailedException` when
   A took effect. No race schedule produces that: A always reads back its own
   write before another writer runs. The relaxation is still wider than it
   needs to be.
5. **The relaxations are otherwise sound.**
   - The log running past max.append.count breaks nothing.
   - Every unknown outcome without a fault follows a horizon crossing, so the
     multi-writer test's "unknown is fine with gc" admits no real violation.
   - "Serial with or without A" admits none on the correct model.

### Oracle changes

The tightened oracles in `CatalogOracles.tla` (`check.py --matrix --tight`)
pass on the correct model. All but one part are now in the Java tests. Each
was calibrated by putting its bug back in the code:

1. `CheckpointRoundTrip`: the record that follows a checkpoint, the compacting
   commit's own, applies if it holds on the checkpoint (`CasAppliesItsRecord`).
   Every fuzzer runs it on every write. With Bug 64b put back, all three
   fuzzers now fail on it at their default tries.
2. TestMultiWriterProperties: a commit with no commit since its snapshot, no
   preemption and no fault must be acknowledged (`OracleMW2`). Bug 64b put
   back, with change 1 off: it fails on it. On the correct code it found **Bug
   67**: a transaction that read or updated a table and then dropped it could
   never commit, because replay rejects a record that drops a table it also
   reads or updates. The model could not find that, since its transactions
   have no such structure.
3. TestCatalogFaultProperties: an outcome is unknown only after an injected
   fault, or with a gc writer. With Bug 66 put back and a segment limit its
   setup survives, it fails on it; without the rule, it passed. The model's
   `OracleFault2` also asks that an operation nothing raced commits. That part
   is not in the Java test: unlike the model's, the fuzzer's operations can be
   rejected for what they do (a create of a table that exists, a stale
   handle's validation), and its model has no way to tell those from a
   spurious rejection.
4. TestCatalogRaceProperties: with gc, A may lose its outcome only if a racer
   ran before one of its writes; and if A took effect, its outcome must be
   unknown (`RacePred2`). This closes the hole in finding 4, which no race
   schedule reaches, so there is no bug to calibrate it with. In the Java test
   the first racer always runs before A's first write, so the first condition
   holds whenever A writes. Change 1 is what catches 64b there.

## The shared-log model

`SharedLogCommit.tla` models the same commit loop on a log that cannot replace
itself: `LogRuntime`'s append branch over a `SharedLog` whose checkpoints are
stored apart from its records (Kafka, SQS FIFO with an archiver, a ledger,
`InMemorySharedLog`). It was written before the Java it validates (the
unconditional path, `compact()`, `InMemorySharedLog`), to state what that code
must do; the Java follows it, and the Java fuzzers were calibrated against its
bug switches ([Calibration in Java](#calibration-in-java)). The state machine
and replay are `RSM.tla`'s.

**The log.** One total order of records (P1). `log` holds all of it, trimmed
records included, as a ghost; a reader sees only what follows the checkpoint it
starts from. A checkpoint is `[pos, st, C, H]`: the replay of the log through
`log[pos]`. The first, at 0, is the empty state. A reader may start from any
usable checkpoint (one at or past the trim point). With `Lag`, its read may end
anywhere after that checkpoint, as a lagging replica's does.

**Writers.** Each stages a transaction on a read, appends its record
unconditionally (`Send`), and reads through it (`ReadBack`). A write that fails
or loses its reply is resolved by a fresh read (`Resolve`), as on the file:
committed if the record is there; otherwise sent again if the transaction holds,
else failed. The faults are the file model's, less `landedConflict` (there is no
condition to fail), plus `notSent` (`LogNotSentException`: nothing landed).

**Compactors.** Within budgets, in any order and interleaved with the writers:

- `AppendGc` appends a GC record at a compactor's clock less the retention;
- `PutCheckpoint(p)` stores the replay through any position `p`, read from a
  usable checkpoint before it;
- `Trim(p)` trims through a position that a stored checkpoint covers (P5).

`LogRuntime.compact()` (a GC record, a read through it, a checkpoint there, a
trim) is one order of these.

### Actions and code

Line numbers are of `LogRuntime.java` at 7e056b3. `TestSharedLogRuntime`
(intentlog-memory) drives each schedule the model found, with its switch in
brackets.

| Action | Code | Covered by |
|---|---|---|
| `Begin` | `commit` :202; the id minted after the read's horizon :324; the commit's floor :328 | protocol, MW |
| `Send` | `log.append(record, null)` :426, through `write` :582; a throttled send kept as `duplicate` :609 (`dup`) | protocol, MW |
| `ReadBack` | `readThrough` :613; a view without the record is ambiguous :617 (`p3`) | protocol, MW |
| `Resolve` | `catch AmbiguousWrite` :458; `read` reads again until it reaches the floor :639 (`lagread`) | protocol, MW |
| `Exhaust` | after the loop :491 | protocol |
| `Judge` | `landed` :694: replay's outcome first, then the committed set (`setonly`) | protocol, MW |
| `AppendGc`, `PutCheckpoint`, `Trim` | `compact` :89; `InMemorySharedLog.putCheckpoint` and `trim` | protocol (also inside commits, whole or GC record only), MW (between operations) |
| `LandInflight` | a `DELAYED` append, landed by `InMemorySharedLog.deliverDelayed` | protocol, MW |
| lagging reads (`Lag`), reads from older checkpoints | `InMemorySharedLog`'s read shapes (`ReadShape`) | protocol (stale reads) |

"protocol" is `TestMemoryCommitProtocol` (the testkit's `CommitProtocol` on
`InMemorySharedLog`); "MW" is `TestMultiWriterProperties` on the shared log.
Not modelled: creating the log (its first checkpoint, `create` :528),
`refresh`, a record larger than the log stores, which `loop` rejects before
sending it (`TOO_LARGE`, Bug 71; `TestSharedLogRuntime`), and reads outside a
commit, such as a catalog's right after its own, which `read(floor)` keeps
from going back (Bug 72; `TestSharedLogRuntime`, `TestCatalogRegressions`).

### Properties

The file model's, restated over the whole log, with these differences:

| Property | Statement |
|---|---|
| `Readable` | some stored checkpoint covers the trimmed prefix |
| `Deterministic` | every reader, from whichever usable checkpoint, replays the log to the same state, committed set and horizon |
| `CheckpointIsReplay` | a checkpoint at `p` is the replay of the log through `p` (I11) |
| `ExactlyOnce` | no commit applies twice, whatever ids it took |
| `SoloCommits` | as on the file; checkpoints and trims are no race, a lagging read is |

`CasAppliesItsRecord` has no counterpart (there is no fused write).

### Results

Every configuration of the correct protocol passes:

| Config | What it adds | States |
|---|---|---|
| `log-base`, `log-creates`, `log-blind` | two writers, one transaction each: contending; creating; each writing what the other reads (write skew, and a copy of a record that would apply twice but for its id) | 40 each |
| `log-two-each` | two transactions each | 617 |
| `log-faults`, `log-faults2` | every fault, late landings included; two faults within two attempts | 546; 4,058 |
| `log-ckpt` | two checkpoints and two trims: reads from a later checkpoint, a read back past a trim | 446 |
| `log-gc` | a compactor's GC record at retention 0, checkpoints, a trim | 27,741 |
| `log-lag` | the same, with lagging reads | 45,020 |
| `log-skew` | retention 1, a writer's clock behind by 2 and a compactor's ahead by 2 | 124,021 |
| `log-gc-faults`, `log-blind-faults` | compaction and every fault | 286,486; 300,888 |
| `log-mix` | compaction, every fault, lagging reads | 540,348 |
| `log-three` | three writers, with compaction | 1,338,353 |
| `log-concerns`, `log-concerns-faults` | a table's concerns as entities; with every fault | 890; 18,562 |
| `log-blind-appends` | two blind appends, with compaction and every fault | 301,950 |
| `log-validated` | three validated commits on partitions | 890 |

**Coverage.** `check.py --witnesses` reaches, in the passing configurations:

- a read from a later checkpoint, two usable checkpoints (so `Deterministic`
  is not vacuous), a trim, a read back of a record trimmed since;
- a GC record that drops a record's id before its writer reads it back;
- a record whose id the horizon passed before it landed (the commit fails);
- a horizon, an unknown outcome without a fault, an error (`notSent`);
- a record twice in the log, and a copy that can land after its commit reported;
- a failed commit whose record is in the log;
- throttle retries, the last attempt spent, an unknown outcome;
- a commit resolving an ambiguous write after a read of it lagged.

### Calibration

Each switch puts back a mistake this kind of log admits. The first five were
planned with the model; the last three are what the model found (below):

| Switch | What it does | Caught by |
|---|---|---|
| `ckclock` | a checkpoint drops the ids its compactor's clock says are past the retention, as the file's did before GC records | `Deterministic` |
| `setonly` | a commit judges its record by the committed set alone, as `landed` did before stage 3 | `HonestFailed` |
| `trimearly` | a trim before a checkpoint covers what it drops | `Readable` |
| `noverify` | after an ambiguous write, a commit whose record is not in the read fails without asking whether its transaction holds | `HonestFailed` |
| `freshid` | the retry after an ambiguous write takes a new id | `ExactlyOnce` |
| `dup` | a throttled send before the acknowledged one is not counted as a write that may have landed | `HonestFailed` |
| `lagread` | the read that resolves an ambiguous write may lag behind the one the commit staged on | `HonestFailed` |
| `p3` | `readThrough` may answer from a checkpoint that already holds the record | `HonestFailed` |
| `basever` | an entity's version set from the one its record was staged on (see the file model) | `StampsCount` |

### What the model found

The protocol holds on every configuration once three rules are in place.
`LogRuntime` follows each of them, and `TestSharedLogRuntime` has the schedule
that breaks each without it:

1. **A throttled send is an earlier write** (`dup`). On a log without
   conditional appends, a send the log throttled may have landed, and its
   retry lands beside it. If a compactor checkpoints between the two copies and
   then appends a GC record that drops the id, a read through the acknowledged
   copy starts from that checkpoint. There replay skips the copy (its id is
   committed), and the committed set can no longer say. The commit must report
   unknown, not failed. `LogRuntime.write` kept its throttle exception local,
   so `landed` did not see it; it now keeps it as `duplicate`, an earlier
   write. On one object this cannot happen: the retry fails its condition
   (P4).
2. **A commit's reads do not go back** (`lagread`). P1 lets a read return any
   prefix of the log. After an ambiguous write, a commit fails if its
   transaction does not hold on a fresh read. That is sound only on a read at or
   past the one the transaction was staged on: S2 holds from there, and every
   copy of the record lands after it. On a read that lags behind it, the
   transaction may not hold *yet*, while its record has applied. Each of
   the commit's reads must reach its floor (`continues`): the tail it staged
   on, or a later read it adopted. One that lags is read again, then the log
   is unreadable (`LogLagException`).
3. **`readThrough` returns a view whose records include the record** (`p3`),
   or fails (`TrimmedException`, which leaves the write ambiguous). A view
   whose checkpoint already holds the record cannot tell an applied record
   whose id GC dropped from one that never applied, and the commit would report
   failed. The plan proposed instead counting the acknowledged append as a
   write that may have landed. That would also be safe, but it reports unknown
   where this rule reports exactly. `SharedLog.readThrough` states the
   obligation, and the runtime checks it: a read back whose records do not
   include the record (`Replay.Result.seen`) leaves the write ambiguous.

Two of the planned rules hold as designed. A commit judges its record by
replay's judgment before the committed set (`setonly`): another compactor's GC
record can land between its record and its read back. And a GC record is the
only thing that moves the horizon (`ckclock`): a checkpoint with GC of its own
makes readers disagree.

### The oracles

`SharedLogOracles.tla` states two oracles a fuzzer of the shared log can
assert. Its harness owns the log, so it can replay all of it, trimmed records
included, and it knows the faults it injected; it cannot see the runtime's
reads.

- `now`: each commit judged as it ends, against a replay of the whole log at
  that moment, as `TestMultiWriterProperties` judges commits on the file. A
  committed commit applied, a failed one did not, an error follows a fault,
  and an unknown outcome follows a fault (or GC).
- `drain`: `now`, and once the run is over and every delayed append has landed,
  every outcome is checked against the final log. Each commit applied at most
  once (the harness tags each intention with its commit), every stored
  checkpoint equals a replay through it (`InMemorySharedLog.putCheckpoint`),
  and the state is the serial execution of what applied.

`check.py --matrix` runs both with each switch, on free schedules, with the
same three checks as the file model's matrix:

| Switch | `now` | `drain` |
|---|---|---|
| none (also with write skew, and with lagging reads) | passes | passes |
| `setonly`, `trimearly`, `p3`, `dup` | caught | caught |
| `ckclock`, `freshid`, `lagread` | caught on some schedules, **missed** on others | caught |
| `noverify` | **missed** | caught |

`drain` is adequate everywhere: wherever it passes, the properties hold. `now`
misses what happens after a commit has ended. A copy of a failed commit's
record lands later and applies (`noverify`, `lagread`), and a retry under a new
id applies beside the original (`freshid`). It also misses a checkpoint that
disagrees with the log, unless a writer happens to read from it and misjudge
its own record (`ckclock`). So the Java fuzzers of the shared log do not judge
only as each commit ends: `CommitProtocol` and `TestMultiWriterProperties`
deliver what they delayed, judge the whole run again, and check each
checkpoint against a replay.

### Calibration in Java

Each switch was put back in `LogRuntime` or `Replay` in turn, and each fuzzer
run against it, with two more mutants of the committed set: `nodedup` (replay
ignores the committed set) and `ckempty` (a checkpoint drops it). The tries
are the failing try of the default run (2,000 for the protocol fuzzers, 3,000
for MW) unless stated:

| Mutant | protocol, memory | protocol, file | MW, shared log |
|---|---|---|---|
| `dup` | caught (1,126 tries) | missed | missed |
| `lagread` | caught at 20,000 tries (602) | missed | caught |
| `p3` | caught at 20,000 tries (4,354) | missed | not run |
| `setonly` | caught (69) | missed | missed |
| `noverify` | caught (19) | missed | caught |
| `freshid` | caught (1) | missed | caught |
| `ckclock` | caught (57) | missed | caught |
| `trimearly` | caught (8) | not applicable (no trim) | not run |
| `nodedup` | caught (5) | caught (1) | caught (9) |
| `ckempty` | caught (2) | caught (2) | caught (1) |

"protocol, file" is `TestAtomicFileCommitProtocol`, which misses the
shared-log switches at 20,000 tries, as the model predicts: on one object a
conditional write makes each of those mistakes harmless. MW misses `dup` and
`setonly`, which need a compactor's GC record to land inside a commit; only
the protocol fuzzer schedules that. The schedules that break the three rules,
`setonly`, `noverify` and `freshid` are also unit tests in
`TestSharedLogRuntime`.

## Paging the committed set

`docs/COMMITTED_PAGING.md` moves the committed set's old ids out of
checkpoints into immutable pages. This is its stage 2: the design in both
models, written before its Java (stage 3), as the shared log was. With paging
off (`Paging` FALSE on the file, `MaxPage` 0 on the shared log), the floor
equals the horizon and there are no pages. Every earlier configuration then
reaches the same states as before.

### What is modelled

**The set** (`RSM.tla`) is `[C, H, F, P]`:

- `C`, the inline ids, all above the floor `F`;
- `P`, references to pages, which hold the ids in `(H, F]`;
- `H`, the horizon, at or below which nothing is known.

Replay admits an id only above `F` (`MayApply`), so it never reads a page.
Writers mint after `F`.

**Blocks.** Stamps lie on a grid of `2^GridBits` ms (8 in every
configuration). An id's block is the largest node that holds it and at most
`BlockIds` (N) inline ids. The blocks are computed from `C`, not stored: that
is the design's claim that any two replicas at one position agree. The oldest
block is the oldest id's. The configurations use N = 1, and N = 2 in
`log-page-n2`.

**Records.**

- `PageOut(node, digest)` seals the oldest block if it is still the block its
  writer paged (the node, then the digest) and is not the newest. It raises
  `F` to the block's top. Otherwise it changes nothing (`PageOut`, as in the
  design's pseudo-code). The model's digest is the set of ids itself.
- `AdvanceHorizon(w)` drops the pages whose top is at or below `w`, and the
  inline ids at or below it. It raises `H` to the newest stamp it dropped (for
  a page, its newest id), and `F` to at least `H`.

**Lookups.** `landed()` on an id in `(H, F]` fetches the page that holds it
from the page store (`Lookup`): the oldest reference whose top is at or above
the id's stamp.

- If the page holds the id, the commit is committed. If it lacks it, the
  commit fails (`BEYOND_FLOOR`), since replay rejects the id from then on.
- A get that fails (`pageFail`, a fault) is a failed read, under the usual
  rules: in a read back the write is ambiguous; elsewhere the outcome is
  unknown if a write may have landed, else an error.
- A page that is not in the store was deleted after GC dropped it, so the read
  was a stale replica's. The runtime reads again. In the model, that read
  decides nothing: the step is not taken.
- When no reference covers the stamp, see finding 1 below.

**Pages.** The page store is a set of names. A name is the node and the
digest, so a page is its name. A page is put before its PageOut is sent:

- **On the shared log**, a compactor reads the log from any usable checkpoint
  (with `Lag`, through any position). It takes the oldest block if that block
  is sealable and its top is at or below the compactor's clock less
  `Admission` (A). It puts the page (`PutPage`), and later appends the PageOut
  (`SendPageOut`), or never does: a crash after the put. A put that fails
  (`putFail`), or lands with its reply lost (`putLost`), leaves no PageOut to
  send.
- **On the file**, a CAS writes `[checkpoint][gc][page-outs][record]`. Its
  PageOuts seal the oldest blocks of its view after its own GC, while their
  tops are at or below `min(clock − A, own stamp − 1)`. `PrepCas` fixes the
  payload and puts its pages, one step before the CAS (`Write`). After a put
  fault, the CAS goes without its PageOuts.
- **Sweeps.** `Sweep` (any compactor, on any read) deletes every page its read
  does not reference whose node ends at or below its floor. Deleting the pages
  a GC record dropped, once it has landed, is a sweep on a read through it, so
  it is not a separate action.

### Actions and code

The Java is stage 3 (built 2026-11-04 on branch `committed-paging`); the code
column names the design's parts, which the Java names alike (`PageOut` replays
as `CommittedSet.pagedOut`, the policy is `LogRuntime.page`).

| Action | Design (COMMITTED_PAGING.md) |
|---|---|
| `PageOut` (replay), `Gc` | "Paging out: the log record", "GC with pages": `CommittedSet.pageOut`, `dropThrough` |
| `MayApply`, `Stage` (mint after `F`) | "Lookups and the commit loop": `mayApply`, minting |
| `Lookup`, `Judge` | `landed` resolving `PagedOut` through `pages()` |
| `PutPage`, `SendPageOut` (shared log); `PrepCas`, `Write` (file) | "Compaction: when and how to page": the policy, then the write |
| `Sweep` | "Page objects": deletion after GC, orphans |
| `pageFail`, `putFail`, `putLost` | `InMemorySharedLog`'s page-store hooks |

### Properties

Both models' `Safety` gains:

| Property | Statement |
|---|---|
| `PagesExist` | every page the log references is in the store (the design's oracle 2: a landed PageOut's page exists until GC drops it) |
| `PagesExact` | every referenced page holds exactly the ids that applied in its range |
| `Shape` | `H ≤ F`; the inline ids lie above `F`; the pages' tops lie in `(H, F]` |
| `OnGrid` | every stamp lies on the grid |

`Deterministic` and `CheckpointIsReplay` now compare the floor and the pages
too.

### Results

Every configuration of the correct design passes:

| Config | What it adds | States |
|---|---|---|
| `page` | two transactions each, a tick, a CAS that pages, one sweep | 8,185 |
| `page-cas` | CAS only, two ticks | 23,654 |
| `page-gc` | retention 2, three ticks: pages that GC later drops, and the sweep that deletes them | 138,132 |
| `page-faults` | every fault, page faults included, strict versions | 126,770 |
| `page-skew` | a writer's clock ahead by 2: the other mints after a floor above its clock | 7,995 |
| `log-page`, `log-page2` | two transactions each; one page put (its PageOut sent, or never) and one sweep; two pages | 10,211; 17,259 |
| `log-page-n2` | blocks of two ids | 7,885 |
| `log-page-gc` | one transaction each; a GC record, a checkpoint, a trim | 23,662 |
| `log-page-faults` | one transaction each; every fault, page faults included | 10,686 |
| `log-page-lag` | GC and lagging reads (stale PageOuts, the same PageOut twice) | 82,911 |
| `log-page-skew` | GC at retention 1, a writer's and a compactor's clocks ahead by 2 | 60,060 |

**Retention and paging.** A CAS pages the ids in
`(clock − R, min(clock − A, own stamp − 1)]`: it pages the view after its own
GC. With A = 0, a file-model CAS therefore pages nothing below a retention of
2 ticks. `page-gc` first ran at retention 0 and paged nothing, with the same
state count at retention 1. The witnesses below caught that.

**Coverage.** `check.py --witnesses` reaches, in the passing paging
configurations:

- a floor above the horizon, a PageOut in the catalog file, two pages at once,
  two blocks, a block of two ids;
- a PageOut that changed nothing, and the same PageOut twice in the log;
- a writer about to judge its record when its id is paged, and a commit that
  failed because its record landed below the floor;
- a copy of a record landing after a PageOut sealed its id;
- an orphan page, a sweep that deleted a page, and a page that GC dropped.

### Calibration

Each switch is a mistake the design rules out. Each breaks a property:

| Switch | What it does | Shared log | File |
|---|---|---|---|
| `floorgap` | replay admits ids above `H`, as before paging, not above `F` | `PagesExact`; with blind appends, `ExactlyOnce` | `HonestFailed` |
| `absentpage` | a page that cannot be fetched reads as absent | `HonestFailed` | `HonestFailed` |
| `nodigest` | PageOut checks the node only | `PagesExact` | unreachable; with resent compactions, `PagesExact` (finding 5) |
| `sweepabove` | the sweep deletes unreferenced pages above the floor too | `PagesExist` | `PagesExist` |
| `mintH` | writers mint after `H`, not `F` | `SoloCommits` | `CasAppliesItsRecord` |
| `pageown` | a CAS's PageOuts are not kept below its own record | | `CasAppliesItsRecord` |
| `missingfail` | a page deleted since the read fails the lookup; the runtime does not read again | `UnknownExplained` (precision only, finding 2) | |

What each counterexample is:

- `floorgap`: on the shared log, W2's record is paged. W1's record, stamped in
  the same ms but staged before the tick, then lands below the floor. It
  applies, and the page lacks it, so a later lookup would answer ABSENT for a
  commit that applied. With blind appends, which verify anywhere, a copy of a
  paged record applies a second time. On the file, see finding 4.
- `absentpage`: a commit whose record applied resolves a lost reply, the get
  of its page fails, and the commit reports failed.
- `nodigest`: a late id lands in the block between the put and the PageOut.
  The PageOut still seals the block, under a page that lacks the id.
- `sweepabove`: the sweep runs between a page's put and its PageOut (on the
  file, between `PrepCas` and the CAS). The PageOut then names a deleted page.
- `mintH`: a writer whose clock is behind mints below the floor, and its own
  record is rejected although nothing raced it.
- `pageown`: a CAS's PageOut raises the floor past its own record, which
  follows it.

### What the model found

The design holds on every configuration. Where the model refines it:

1. **A lookup can find no page.** GC raises `H` to a dropped page's newest
   id, not to its top. An id stamped above that id and at or below the top is
   in `(H, F]`, but no page holds it. Nothing applied there, and nothing can,
   so the answer is ABSENT without a fetch. The design's `contains(q)` throws
   `PagedOut(page holding q)` for every `q` in `(H, F]`; it needs this case.
2. **A missing page means read again** (`missingfail`). A page is missing
   only if GC dropped it and a sweep deleted it, so the read was a stale
   replica's. The design says the runtime reads again and finds the id at or
   below `H`. It may also find the id in the gap of finding 1, which is
   ABSENT. A runtime that gives up instead reports UNKNOWN. That is never
   wrong, so the switch breaks only `UnknownExplained`: in its counterexample,
   a commit resolving on a lagging read finds its page deleted (GC dropped it,
   a sweep deleted it). Reading again would also have found UNKNOWN, with the
   id below the horizon; for an id in the gap it finds REJECTED. Reading again
   is a gain in precision, not a safety rule.
3. **The floor makes pages exact, not only exactly-once.** `floorgap` fails
   first on `PagesExact`, through a late id rather than a copy. An id that
   lands below the floor after its block was sealed must be rejected, or its
   page lacks an id that applied.
4. **On one object, a copy can land after its id was paged.**
   `AtomicFileLog`'s tail `continues` another if the object did not shrink. A
   CAS that makes the object longer therefore reads as an append, and a writer
   whose append was reported as a lost race resends the same record without
   reading ("replay applies it once"). With PageOuts, a CAS is longer more
   often. If that CAS paged the record's id, the copy lands below the floor.
   Only the floor keeps replay from applying or judging it: with `floorgap`,
   replay judged the copy, which no longer verified, and the commit reported
   failed although its first copy had applied. The rule holds; the resend's
   comment in `LogRuntime` should say that the floor, not the inline set,
   rejects such a copy.
5. **On one object, the digest check never refuses a PageOut.** A CAS is
   conditional, so its PageOuts land on the version they were computed on.
   `nodigest` breaks nothing on the file, as the shared log's switches did not
   on one object (`TestAtomicFileCommitProtocol` missed them). The check
   matters for `compact()` on a shared log. Since a compaction after the
   commit that lost to appends is sent again with the records that beat it
   (2026-11-10), it matters on one object too: the resend's PageOuts were
   computed before those records, and `after-nodigest` breaks `PagesExact`.
6. **The catalog fuzzers cannot see a missing page** (the oracles, below).
   Replay never reads a page, so a fuzzer that only reads the catalog misses
   `sweepabove`. `OracleHolds2` adds `PagesOK` (`PagesExist /\ PagesExact`),
   which `CheckpointRoundTrip` can check on every write in stage 4. With it,
   both catalog fuzzers' tightened oracles catch the switch.

### The oracles, with paging

`check.py --matrix` has rows for the paging switches. The file model's rows
run the catalog fuzzers' oracles, with what each fuzzer draws plus the page
store's faults:

| Switch | MW | fault | race / race1 |
|---|---|---|---|
| none | passes | passes | passes |
| `floorgap`, `absentpage` | unreachable | unreachable | no page (below) |
| `sweepabove` | **missed**; caught by `OracleHolds2` | **missed**; caught by `OracleHolds2` | no page |
| `mintH` | **missed** (spurious rejections only); caught by `OracleMW2`'s solo rule | unreachable | no page |
| `pageown` | **missed** | **missed** | no page |

- The race schedules page nothing at these bounds. With one operation per
  writer, no CAS finds a sealable block below its own record. In stage 4, the
  race property needs schedules that page (a commit in an older block before
  A), or it calibrates nothing about paging.
- `pageown` is reachable on the MW and fault schedules: the matrix checks
  invariants only, so it ran `CasAppliesItsRecord` apart. Both oracles miss
  it, because the ids the CAS pages landed after its snapshot: the commit was
  raced, so no solo rule applies. This is Bug 64b's pattern. The check that
  caught 64b catches this too: `CheckpointRoundTrip`'s compacting commit's own
  record applies if it holds on the checkpoint, on writes that carry
  PageOuts.

The shared log's rows run `now` and `drain`, each also checking `PagesOK`
after every step: the harness owns the log and the page store.

| Switch | `now` | `drain` |
|---|---|---|
| none | passes | passes |
| `floorgap` (blind appends: a copy applies twice) | **missed** (the copy lands after its commit ended) | caught |
| `nodigest`, `sweepabove` | caught (`PagesOK`) | caught |
| `absentpage` | caught | caught |
| `mintH` | **missed** | **missed** |

`drain` is adequate on every row. Neither oracle asserts that a commit nothing
raced succeeds, so `mintH`, which costs only spurious rejections, passes both.
The shared log's fuzzers need the solo rule that `OracleMW2` gave the
multi-writer test: a commit that no record raced, on a log that did not fail,
commits.

In Java (stage 4, 2026-11-04; docs/COMMITTED_PAGING.md "Order of work"):
`CommitProtocol` checks `PagesOK` and more after every step, against the
history (the set's inline ids, its pages' contents, and its bytes); it already
had the solo rule, which catches `mintH` on both providers once stamps are
drawn below the floor. It catches `nodigest` on one object too, since it lands
a split's PageOuts and copies of PageOuts as records there, which the protocol
does not. The catalog fuzzers check `PagesExist` (and each page's shape, not
`PagesExact`, which needs the history) on the newest catalog file and shared
log (`CheckpointRoundTrip.assertPagesExist`), and the race property's
schedules page: its setup's ids are older, in blocks a racer seals before A
writes.

## The state's pages

`docs/SNAPSHOT_PAGING.md` step 4 pages a table's old snapshots out of the root:
the state machine itself names pages, which a compactor puts before the root
that names them. The file model has them (the shared log does not yet:
`SealEvery` is 0 in every `log-*` configuration).

- **The state** (`RSM.tla`) holds `sealed`, the names of its pages: `[e, v]`,
  entity `e`'s history sealed at its version `v`. A record that moves `e` to a
  multiple of `SealEvery` seals one; one that moves it to a multiple of
  `2 * SealEvery` drops the one before (an expiry that empties a block). A
  function of the log, so every replica names the same pages.
- **Puts.** `PrepCas` puts the pages of the root a CAS writes before the CAS
  (`LogRuntime.checkpoint`). A page a record seals after the root is pending:
  the next root's compactor puts it. Put faults touch only the committed set's
  pages here; the Java fails the checkpoint on a failed state put.
- **Sweeps.** `SweepState(w)` deletes from writer `w`'s snapshot, however
  stale, the pages garbage under it (`CatalogStateMachine.garbagePages`):
  sealed at a version it has reached and not named by it.
- **`StatePagesExist`**: every page the root names that the catalog still
  names is in the store. A page a record after the root dropped is garbage
  once that record is in the log: a reader replays it before it loads (one
  that loaded before fails, as Iceberg's readers do once an expiry deletes a
  manifest list). The first version required every page the root names, and
  TLC found a sweep from the live state deleting a page a later record had
  dropped: the property was too strong, not the protocol.
  `CheckpointRoundTrip.snapshotPagesProblem` checks the same in Java.

| Configuration | Expect | What |
|---|---|---|
| `statepages` | pass | writer 1 writes `a` three times (`SealEvery` 1: seals at each version, drops one behind), CAS every commit, sweeps |
| `rootfirst` | `StatePagesExist` | a root written before its pages are put |
| `sweepstale` | `StatePagesExist` | a sweep that deletes every page its (stale) view does not name, whatever version |

## Tail reads

[TAIL_READS.md](../../docs/TAIL_READS.md) step 4, written ahead of step 5's Java
(2026-11-10).

**The file model.** The object carries its incarnation (`inc`) and each record
the incarnation its frame names (`fi`): a CAS draws a fresh one (`<<w, n, nw>>`,
unique per attempt) for every frame it writes, and an append frames its record
with the incarnation of the version its condition names, `cur.inc`, which a
writer takes from the read it staged or rebased on. `Readable` now also asks
that every frame name the object (`FramesTrue`): a checked read of an object
holding another's frame fails, forever. A new invariant, `NoUnheldState`, asks
that every writer's replica be a state the object held (each read marks its
view `held`).

After an append loses its race the loop tail-reads (`TailRead`, with
`TailReads` TRUE, the default): if the object is still the incarnation of the
writer's replica and no shorter (`Continues`), the records after it are folded
onto the replica (`Folded`); else the whole object is read. Either way the
commit judges its own record on the read, as `landed` does, and appends again
from the fresh replica. With `TailReads` FALSE the loop probes (`Refresh`) and
resends, its frame naming the probed version's incarnation (the last trailer's,
read pinned to it).

| Switch | Puts back | Breaks |
|---|---|---|
| `lengthonly` | a tail read that compares lengths only (`FileTail.continues` before step 5): a longer replace reads as growth, and its records are folded onto a replica of another incarnation | `NoUnheldState` |
| `notrailer` | whole reads whose frames are not checked (format 2): a read torn by a write between its lookup and its GET (`Spliced`: the bound version's checkpoint and first records, the live version's after them) is replayed | `NoUnheldState` |
| `probeinc` | a resend after a probe framed with the incarnation the writer last read (A4 broken) | `Readable` |

**The shared-log model.** A compactor's read may be served with a gap after its
checkpoint (`ReadFromGap`); the runtime refuses it (LOG.md "Positions"), so
without the switch the compactor reads again. `nojoint` replays it, and the
checkpoint it stores is not the replay of the log through its position
(`Deterministic`, `CheckpointIsReplay`).

| Config | What it adds |
|---|---|
| `probe`, `probe-gc`, `probe-faults` | the probe path (`TailReads` FALSE): base, with GC (a replace can grow the object past a writer's version), with every fault |
| `tail-long` | tail reads with the shrink path and GC |
| `lengthonly`, `notrailer`, `probeinc` | the switches above, on `gc0`'s bounds (`probeinc` on the probe path) |
| `log-nojoint` | the shared log's switch, with compaction |

Every other configuration runs with tail reads (the state counts in "Results"
above predate them: `gc0`, for one, now has 21,777 states). Bugs 26, 64a and `floorgap`
need a resend after a probe without a read, so their calibrations run on the
probe path: with tail reads the loop reads before it resends, and a read that
puts the commit's id at or below the horizon ends it.

## Compaction after the commit

[TAIL_READS.md](../../docs/TAIL_READS.md) "Compaction after the commit",
written ahead of its Java (2026-11-10).

**The file model.** With `CompactAfter`, the count and size limits no longer
make a commit replace the object: `MustCAS` holds only for a full object, a
store without append, or a policy due on a log with no records (`DueEmpty`:
max.append.count 0, or a size no record fits under). A commit that a read
finds landed (`Judge`, on every path) may, if `CompactDue` holds of that read
`R`, move to `compact` instead of ending (`Committed`): the model takes both
choices, as the writers' drawn points and backoff may give either. `Compact`
is one conditional replace of `R`'s version with `CompactPayload`: a
checkpoint of `R`, a GC record at the writer's clock less the retention, and
with `Paging` the PageOuts of the view after it up to the clock less `A`
(`PrepCas` puts their pages first), and no record, so neither is kept below
one. Every write fault applies (`inflight` lands later, on the same version
only). Whatever it does, the commit ends committed. A compaction is not one
of the commit's writes (`cw`, `nw`), and no racer runs just before it in the
race schedules: TestCatalogRaceProperties tells it apart by A's trace
(`CommitTrace.Kind.COMPACT`), TestMultiWriterProperties by its write, a
replace with no intention record.

Every configuration above runs with `CompactAfter` FALSE: the fused
compaction, which `fileio.catalog.compact.after.commit` false puts back, and
whose replace a full object still takes. Their states are as before. The
`after-*` family runs the same kinds of schedule with compaction after the
commit, at a count of 1 so that every commit may compact:

| Config | What it adds | States |
|---|---|---|
| `after-base`, `after-cas-only`, `after-gcs` | two writers contending; CAS only (every commit replaces, none compacts after); no append support | 208; 55; 55 |
| `after-size` | compaction by size (`MaxSize` 3) | 4,908 |
| `after-two-each`, `after-long` | compactions racing appends; the shrink path | 14,985; 2,617 |
| `after-faults`, `after-faults-strict`, `after-faults2` | every fault, late landings included (a compaction's too); strict versions; two faults | 2,167; 2,650; 2,621,932 |
| `after-gc0`, `after-skew` | retention 0 (a compaction's GC record, with no record of its own to stay below); a clock behind by more than the retention | 267,011; 17,469,599 |
| `after-seg` | a segment cap: a full object makes the commit replace it with its record | 3,415 |
| `after-long-faults`, `after-three` | faults with the shrink path; three writers | 352,926; 765,928 |
| `after-probe`, `after-probe-faults` | tail reads off, with GC; with every fault | 339,513; 4,596 |
| `after-sched-mw`, `after-sched-fault`, `after-sched-race`, `after-sched-race-op` | each fuzzer's schedules | 48,458; 4,028; 73; 111 |
| `after-concerns-faults`, `after-blind-appends` | conflict encodings with every fault; blind appends | 393,824; 2,415 |
| `after-page`, `after-page-gc`, `after-page-faults`, `after-page-skew` | paging: a compaction pages up to its clock less `A`; with GC; with every fault and the page store's; skewed clocks | 128,712; 2,563,427; 3,600,508; 122,051 |
| `after-statepages` | the state's pages, put before a compaction's root | 87,734 |
| `after-resend`, `after-resend-three` | a compaction that lost to appends sent again, twice at most; three writers | 15,491; 24,425 |
| `after-resend-faults`, `after-resend-gc`, `after-resend-page` | with every fault; GC at retention 0 and a clock behind; paging, a clock behind | 371,882; 341,470; 126,240 |

Every one passes. A compaction that lost its race to appends may be sent again
(`CompactRead`, `CompactResends` times: 1 in every `after-*` configuration, 2
in the `after-resend*` ones): a tail read after the version it was
conditioned on; if the object is that incarnation and no shorter, `compact`
again, conditioned on the version read, with the records after the lost
version (`cp`) copied in front of its GC record and PageOuts, and a fresh
incarnation; else the compaction is dropped. The model takes both choices
where the runtime's rule decides. `CheckpointIsReplay` now says that a CAS's
checkpoint is the replay of a prefix of the version it replaces, and its log
begins with the rest of that version (the whole version, for every CAS but a
resend). A resend's PageOuts were computed before the records it copies, so
on one object the digest check now refuses some: `after-nodigest` (paging,
resends, a clock behind, `nodigest`) breaks `PagesExact`.

Its switches:

| Switch | Puts back | Breaks |
|---|---|---|
| `compactbase` | a compaction that checkpoints the replica the commit staged or rebased on, before its record, conditioned on the version its record landed in | `HonestCommitted` |
| `compactresend` | a compaction that lost its race sent again on the object as it now is, as if the records that beat it were in its checkpoint | `HistGrows` |
| `compactfail` | a compaction that lost its race fails the commit | `HonestFailed` |
| `compactcopyafter` | a resend with the records that beat it after its GC record and PageOuts, where a GC at retention 0 drops their ids | `CheckpointIsReplay` (the matrix: `HonestCommitted`) |
| `compactreplaced` | a resend that takes a replace as long as the version it lost on for growth | `HonestCommitted` |

Writing the Java found one rule the model did not have: with CAS only, every
commit replaced the object and its writer then compacted it again (a second
replace, which also raced the next commit). `DueEmpty` keeps a writer from
compacting after a commit the policy would have replace on any log.

Witnesses (`--witnesses`): a compaction landed, one lost its race, an append
lost its race to one (`Not_CompactionLanded`, `Not_CompactionLost`,
`Not_LostToCompaction`), a full object with compaction after the commit, a
compaction on the multi-writer and race schedules, a resent compaction landed
(`Not_CompactionResent`, with GC and with paging), and one whose PageOut its
copied records made replay refuse (`Not_ResentPageOutRefused`).

The oracle matrix (`--matrix`) adds `none-after` and the three switches on
each fuzzer's schedules:

| Bug | `mw` | `fault` | `race`, `race1` |
|---|---|---|---|
| `none-after` | passes | passes | passes |
| `compactbase` | caught | caught | caught |
| `compactresend` | caught | unreachable | unreachable |
| `compactfail` | caught | unreachable | unreachable |
| `compactcopyafter` | caught | unreachable | unreachable |
| `compactreplaced` | unreachable | unreachable | unreachable |

Every cell is adequate. The fault schedules run nothing between a commit's
first write and its end, and the race schedules no racer before a
compaction, so neither loses a compaction race. The multi-writer oracle
first missed `compactbase` and `compactresend`: it judged a commit on its own
writes, and the damage is in the compaction's. It now also asks of each
compaction that it change no state (`OracleMW`'s `cc`), as
TestMultiWriterProperties does, which catches both. With resends it compared
too little: the entity versions (`View`). A `compactbase` resend dropped
writer 1's second record, which let writer 2's, rejected in the original
log, apply in its place, to the same versions; `Adequate` failed. The check
now compares the replayed state whole, which commits made it included, as
TestMultiWriterProperties compares a compaction's catalog: its namespaces and
each table's whole metadata (it compared locations and a few properties,
blind to an inline table's snapshots). In Java, the one-object
commit-protocol fuzzer (5,000 tries) and the multi-writer fuzzer (300 tries)
each catch all three as mutants (`fuzz/mutants/compaction.sh`, where
`compactresend` is a compaction conditioned on a probe of the object); the
resend's (`compactnocopy`, `compactcopyafter`, `compactreplaced`) and the
three before, the protocol fuzzer at 50,000 tries.

## The Azure log

`AzureLog.tla` models docs/AZURE_LOG.md's log on Azure Append Blobs: the provider below the SPI
that `SharedLogCommit.tla` takes as one log. Like that model, it was written before the Java
(`intentlog-azure`'s `AppendBlobLog`). Its configurations are `az-*`, through `MCAzure.tla`.

**What is modelled.** Sizes are in units: a record's frame is 1, and a pad is whatever reaches
the segment's limit L = W + 1.
- **Writers** append their records blind, each append carrying the writer limit W. Faults: a
  lost reply, nothing sent, and an append still in flight that lands later under the
  conditions it was sent with.
- **Rotation.** A writer that finds the last segment full reads the head, and if it still
  names that segment last, closes it. The close finds it closed (at L, or at the block limit),
  or pads it to L at the length it read. Then the writer creates a candidate and CASes the
  head.
- **Compactors** put a checkpoint at a record boundary of a live segment and CAS the head to
  name it, trim (a CAS that drops the segments before the checkpoint's) and sweep (deleting a
  segment the head doesn't name, at or below its last ordinal).
- **Readers** read from the head they hold, however stale. A closed segment must read to its
  recorded length. A closed last segment sends the reader to the head again for its successor.
  A read may come back short, as a fault.
- **Crashes** need no action: a writer that stops is a behaviour TLC already explores.

**The log the provider presents** (`Abstract`, a ghost) is the records of the segments every
head has named, in ordinal order, each up to its closed length (the last up to its size), pads
left out.

| Property | Statement |
|---|---|
| `AbstractGrows` (action) | the log only grows: every record keeps its place (P1) |
| `ClosedSegments` | AZ1: every segment the head names but the last is closed, at the length it records |
| `NothingPastClose` | AZ1 on the ghost: nothing lies past a closed length |
| `ReadsAreSlices` | AZ2: every read was a slice of the log from its checkpoint |
| `AckedAtReceipt` | AZ3: every acknowledged append is in the log at its receipt |
| `NamedExist` | AZ5: every segment the head names exists |
| `Readable` | P5: the head's checkpoint lies in a live segment |
| `CheckpointsStable` | a checkpoint keeps its place in the log |
| `CopiesKnown` | no copy of a record is in the log that its writer doesn't know may have landed (the shared-log model's `dup`) |

**Results** (2026-11-10). Every passing configuration holds:

| Configuration | What it adds | Distinct states |
|---|---|---:|
| `az-base` | two writers, one record each, a segment of one record | 102 |
| `az-two-each` | two records each, three segments | 31,296 |
| `az-w2` | two records a segment | 2,230 |
| `az-blocks` | segments closed by the block limit, not a pad | 1,248 |
| `az-three` | three writers | 5,721 |
| `az-compact` | a checkpoint, a trim and two sweeps, over two rotations; two reads | 334,744 |
| `az-faults` | one fault: lost reply, not sent, in flight, short read | 17,922 |
| `az-faults-compact` | a fault, with a checkpoint, a trim and a sweep | 331,898 |

Each bug switch (AZURE_LOG.md "Mistakes the model must catch") breaks a property:

| Switch | Breaks |
|---|---|
| `casbeforeclose` | `ClosedSegments` |
| `nolimit` (writer 1's appends carry no limit) | `ClosedSegments` |
| `createonmissing` | `NothingPastClose` |
| `noclosedlength` | `ReadsAreSlices` |
| `sweepnamed` | `NamedExist` |
| `trimearly` | `Readable` |
| `sdkretry` | `CopiesKnown` |
| `offsetfromlength` | `AckedAtReceipt` |

`nolimit` first dropped the limit from every writer's appends, and broke nothing: with no
writer stopping short of L, no pad is ever needed and segments close only at the block limit.
The mistake is a code path that forgets the limit while others keep it, so the switch drops it
from writer 1's appends only.

The witnesses (`check.py --witnesses -k az-`) are each reached:
- writes: a rotation, two rotations, a pad, a block-limit close, a lost CAS, a late landing,
  and a record in the log twice;
- compaction: a checkpoint named by the head, a trim and a sweep;
- reads: a reader stepping past a closed last segment, and a 404 for a stale writer and for a
  stale reader.

**In Java** (`intentlog-azure`), the switches are mutants (`fuzz/mutants/azurelog.sh`), and each
fails `TestAzureCommitProtocol` or `TestAppendBlobLog`. `offsetfromlength` is equivalent there:
the receipt carries the frame's CRC, which a read through it checks, and the runtime judges a
commit by its id. Positions in Java are global offsets (each segment's base in the head), so
the end of a closed segment and the start of the next are one position. The model's (ordinal,
offset) pairs are the same places.

## History

This replaces `FileIOCatalog.tla` (PR #1; last changed 2026-10-14 for Bug 10). It
was a PlusCal model in which every append landed, each writer committed one
transaction, and there were no lost replies, transaction contents, GC or segment
cap. Its `MC_ids_bug` is the `bug10` configuration here.
