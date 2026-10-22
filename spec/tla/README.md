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

The model reproduces the commit loop, not Iceberg. Table metadata (inline TM and
ML, deltas, row lineage, DVs), byte encoding, SDK error mapping and performance
are out of scope.

| File | What it holds |
|---|---|
| `RSM.tla` | The state machine and replay (`Replay`, `CommittedSet`, `landed`'s verdict), and the serial execution the properties compare with |
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
table takes about 35 minutes. Most configurations take seconds; `mix`,
`mix-lenient`, `skew` and `log-mix` take 3 to 6 minutes each, and `log-three`
8 to 11.

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
| `ReadBack` | `write`'s `readThrough` :613, `validate` :720 | all; strict versions: MW, fault |
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
records (`MaxSize` counts records), and backoff timing.

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
| `HistGrows` (action) | the history only grows, one transaction at a time |
| `CasAppliesItsRecord` (action) | a CAS applies its own record when the record holds on the checkpoint it wrote |
| `CheckpointIsReplay` (action) | a CAS's checkpoint is the replay of the version it replaces: a function of the log, not of its writer's clock (I11) |

## Results

Every configuration passes:

| Config | What it adds | States |
|---|---|---|
| `base` | two writers contending; append, then compaction by count | 61 |
| `cas-only`, `gcs`, `size` | max.append.count 0; no append support; compaction by size | 55, 55, 61 |
| `two-each`, `long` | several transactions per writer: compaction racing appends, the shrink path | 958, 538 |
| `creates` | creates in different namespaces | 61 |
| `faults`, `faults-strict`, `faults2` | every fault, late landings included; strict versions; two faults | 1,021; 1,183; 160,075 |
| `gc0`, `skew` | retention 0; a writer's clock behind by more than the retention | 22,049; 768,774 |
| `seg` | a segment cap below max.append.count | 77 |
| `mix`, `mix-lenient` | everything at once | 401,396; 620,919 |
| `long-faults`, `three` | faults with the shrink path; three writers | 100,928; 106,249 |
| `sched-*` | each fuzzer's schedules | 33 to 7,636 |

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
Not modelled: creating the log (its first checkpoint, `create` :528), and
`refresh`.

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
| `log-three` | three writers, with compaction | 1,259,308 |

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

## History

This replaces `FileIOCatalog.tla` (PR #1; last changed 2026-10-14 for Bug 10). It
was a PlusCal model in which every append landed, each writer committed one
transaction, and there were no lost replies, transaction contents, GC or segment
cap. Its `MC_ids_bug` is the `bug10` configuration here.
