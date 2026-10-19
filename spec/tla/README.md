# TLA+ model of the catalog commit protocol

`CatalogCommit.tla` models `ProtoCatalogFormat.Mut.commit` and replay over one
catalog object in storage with conditional writes. `CatalogOracles.tla` states
the jqwik fuzzers' oracles as predicates over the same model. The model's purpose
is to check the oracles: that what the property tests assert is enough to imply
the protocol's properties, and that each historical protocol bug breaks a
property and is caught by an oracle.

The model reproduces the commit loop, not Iceberg. Table metadata (inline TM and
ML, deltas, row lineage, DVs), byte encoding, SDK error mapping and performance
are out of scope.

| File | What it holds |
|---|---|
| `CatalogCommit.tla` | The protocol: storage, replay, the commit loop, faults, GC, the bug switches, the properties |
| `CatalogOracles.tla` | The three fuzzers' oracles, their tightened versions, and `Adequate` |
| `MC.tla` | Scenarios (what each writer commits) and the coverage witnesses |
| `check.py` | The configurations, and what TLC must report for each |

## Running

```bash
mkdir -p spec/tla/tools
curl -L -o spec/tla/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

spec/tla/check.py               # every configuration; each must give its expected verdict
spec/tla/check.py -k bug        # the configurations whose names contain "bug"
spec/tla/check.py --witnesses   # every branch and interleaving listed below is reached
spec/tla/check.py --matrix      # the oracle matrix (about 40 minutes)
spec/tla/check.py --matrix --tight  # the tightened oracles on the same cells
```

`check.py` writes each configuration to `states/cfg/<name>.cfg` and TLC's output
to `states/out/<name>.txt` (both ignored by git). To rerun one by hand, from
`spec/tla`:

```bash
java -cp tools/tla2tools.jar tlc2.TLC -workers 2 -config states/cfg/mix.cfg MC
```

`TLC_WORKERS` (default 2) and `TLC_HEAP` (default 2g) size the runs. Checked with
TLC 2026.10.13.164326 (rev 210df3a), the version that jar reported. The whole
table takes about 20 minutes; most configurations take seconds, and `mix`,
`mix-lenient` and `skew` take 2 to 4 minutes each.

## The model

**Storage.** One object: `[gen, chk, log, segs]`. `chk` is the checkpoint: the
state, the committed set `C` and its GC horizon `H`. `log` holds the records
appended since, and `segs` the writes since the object was created. Every write
gets a fresh generation, and every conditional write names the generation it
expects. An append past `SegLimit` writes is refused.

**State.** Each entity (namespace or table, alike) has a version. A transaction
reads some entities at the versions its snapshot had, writes some of them, and
may create an entity under one it writes. Replay applies a record if `mayApply`
admits its id (not in `C`, above `H`) and its reads still hold
(`Transaction.verify`). The checkpoint carries a ghost history: the records
replay applied, in order. The properties are stated over it.

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

Clocks tick (`Tick`), each writer has a skew, and a CAS drops committed ids at
its GC watermark.

**Schedules.** `Sched = "free"` allows any interleaving. The other modes give the
schedules of one fuzzer each: operations run one at a time, and another may run
to completion inside one.

- `"mw"` (TestMultiWriterProperties): one operation, before any storage request, on a snapshot taken earlier.
- `"fault"` (TestCatalogFaultProperties): one operation, before the first write.
- `"race"` (TestCatalogRaceProperties): writer 1 is A, and each racer runs fresh, just before A's k-th write.

Operations (`retry`) are restaged on a fresh read after `CommitFailedException`,
as FileIOCatalog and Iceberg do. Catalog transactions are not.

### Actions, code, and the jqwik checks that cover them

Line numbers are of `ProtoCatalogFormat.java` at 4a44e16 unless stated.

| Action | Code | Covered by |
|---|---|---|
| `Begin`, `Stage` | `CatalogFile.Mut`, `buildTransaction` :358; id minted after the read's horizon (`ProtoCodec.java:2107`) | all three fuzzers |
| `Start` | `commit()` :595 | all |
| `Write` (CAS) | `tryCAS` :900, `buildFullFile` :1023 (GC watermark :1040), `retryThrottled` :966 | all |
| `Write` (APPEND) | `tryAppend` :933, `retryThrottled` :966 | all |
| `Write` with `SegOK` false | `catch SegmentLimitException` :709 (Bug 66) | MW, fault (segment limits drawn) |
| `Write` with faults | `writeAndRead` :1006, `retryThrottled` :966 | MW, fault (`InMemoryAtomicFileIO.Fault`) |
| `ReadBack` | `writeAndRead`'s read :1006, `validateCommit` :836 | all; strict versions: MW, fault |
| `Rebase` | `catch CASException` :682 and the rebase after it | all |
| `Refresh` | `catch AppendException` :716, `length(current)` :725 | all |
| `Shrink` | the re-read when the file shrank :725 | MW |
| `FreshForCAS` | `mustCAS && !baseIsCurrent` :659 (Bugs 12, 26) | MW |
| `Resolve` | `catch AmbiguousWrite` :744 | MW, fault |
| `Exhaust`, `FinalRead` | after the loop :780, :783 | MW (faults) |
| `Unreadable` | `catch UnreadableCatalog` :769 | MW, fault (read faults) |
| `Judge` | `landed` :815; `containsTransaction` (`ProtoCatalogFile.java:897`) | MW, fault, race (gc) |
| `Restage` | `FileIOCatalog.commitOnAFreshRead` (`FileIOCatalog.java:270`); Iceberg's table commit retry | fault, race |
| `Replay`, `Fold` | `replay` :210, `mayApply` (`ProtoCatalogFile.java:920`), `verify` :250 | all (every read), `CheckpointRoundTrip` |
| `LandInflight` | none: a timed-out request that lands later | none |
| `Tick`, `Skew` | `System.currentTimeMillis` | gc runs (wall clock); no fuzzer skews clocks |

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

## Results

Every configuration passes:

| Config | What it adds | States |
|---|---|---|
| `base` | two writers contending; append, then compaction by count | 61 |
| `cas-only`, `gcs`, `size` | max.append.count 0; no append support; compaction by size | 55, 55, 61 |
| `two-each`, `long` | several transactions per writer: compaction racing appends, the shrink path | 958, 538 |
| `creates` | creates in different namespaces | 61 |
| `faults`, `faults-strict`, `faults2` | every fault, late landings included; strict versions; two faults | 1,021; 1,183; 160,075 |
| `gc0`, `skew` | retention 0; a writer's clock behind by more than the retention | 21,991; 750,212 |
| `seg` | a segment cap below max.append.count | 77 |
| `mix`, `mix-lenient` | everything at once | 401,402; 712,767 |
| `long-faults`, `three` | faults with the shrink path; three writers | 99,183; 106,249 |
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

**Coverage.** `check.py --witnesses` checks, for each `Not_` predicate in
`MC.tla`, that some passing configuration violates it, so that configuration
reaches the branch:

- the shrink path, the CAS after a fresh read, a CAS rebase, a stale append retry;
- throttle retries, a full segment, the final read after the last attempt;
- a record twice in the log (so exactly-once is not vacuous);
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

## History

This replaces `FileIOCatalog.tla` (PR #1; last changed 2026-10-14 for Bug 10). It
was a PlusCal model in which every append landed, each writer committed one
transaction, and there were no lost replies, transaction contents, GC or segment
cap. Its `MC_ids_bug` is the `bug10` configuration here.
