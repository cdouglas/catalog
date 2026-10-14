# TLA+ model of the FileIOCatalog commit loop

Companion to the jqwik property suite at
`src/test/java/org/apache/iceberg/io/spec/`.

The model is deliberately abstract: the catalog file is a record of `etag`,
`length`, a log of transaction records, and a bookkeeping set `committed`;
a transaction is a record `[uuid, created]` carrying the UUID and the set
of entity ids it allocates. Writers read, choose append or CAS, and retry
on conflict. Append always lands (content-addressed storage), so the
offset check is not what keeps concurrent creates apart — binding entity
ids at replay is (`LateBinding`; see MC_ids_bug). See
[Known gaps](#known-gaps) for what that abstraction cannot detect.

## Property catalogue

| ID | Property | What it checks | Config |
|----|----------|----------------|--------|
| S1 | `S1_LogEntriesCommitted` | Every log entry's UUID is in `committed` (holds by construction) | MC_small, MC_cas_only, MC_medium |
| S2 | `S2_NoLostCommit` | The set of committed UUIDs never shrinks | MC_small, MC_cas_only, MC_medium |
| S3 | `S3_IdempotentReplay` | No UUID appears twice in the log (vacuous; see below) | MC_small, MC_cas_only, MC_medium |
| S4 | `S4_UniqueEntityIds` | No entity id is allocated by two committed transactions | MC_small, MC_cas_only, MC_medium; MC_ids_bug (expected to FAIL) |
| L1 | `L1_Termination` | Every writer eventually finishes (committed or exhausted) | MC_liveness_L1 |
| L2 | `L2_NoStarvation` | Every writer eventually commits or exhausts its retries | MC_liveness_L2 |
| L3 | `L3_BoundedRetries` | `attempt <= MaxRetries` (an invariant) | MC_liveness_L3 |

## Configurations

| Config | Writers | Tables | MaxLog | MaxRetries | LateBinding | Properties | Distinct states | Expected |
|--------|---------|--------|--------|------------|-------------|------------|-----------------|----------|
| MC_small | 2 | 2 | 4 | 3 | TRUE | S1, S2, S3, S4 | 76 | pass |
| MC_cas_only | 2 | 2 | 0 | 3 | TRUE | S1, S2, S3, S4 | 108 | pass |
| MC_medium | 3 | 3 | 4 | 3 | TRUE | S1, S2, S3, S4 | 2,168 | pass |
| MC_liveness_L1 | 2 | 2 | 4 | 3 | TRUE | L1 | 76 | pass |
| MC_liveness_L2 | 2 | 2 | 4 | 3 | TRUE | L2 | 76 | pass |
| MC_liveness_L3 | 2 | 2 | 4 | 3 | TRUE | L3 | 76 | pass |
| MC_ids_bug | 2 | 2 | 4 | 3 | FALSE | S4 | — | **FAIL** (reproduces 808bca7) |

`Tables` is declared but not used by the model.

## Running

```bash
bash spec/tla/run-tlc.sh MC_small.cfg
```

or, from `spec/tla`:

```bash
java -XX:+UseParallelGC -cp tools/tla2tools.jar tlc2.TLC -workers auto -config MC_small.cfg FileIOCatalog
```

Each 2-writer config takes about a second; `MC_medium` takes a few seconds.
Deadlock checking is on (TLC's default). A finished run is not reported as a
deadlock because the PlusCal translation's `Terminating` disjunct lets the
system stutter once every writer is done.

### MC_ids_bug: an expected failure

`MC_ids_bug.cfg` is designed to produce a counterexample — it reproduces the
Bug 10 commit-collision fixed by 808bca7 (`Bind new entity ids at replay`).
With `LateBinding = FALSE`, writers allocate entity ids eagerly from their
snapshot, so two writers that read the same empty catalog both pick id 1
and both successfully append. S4 catches it:

```
Error: Invariant S4_UniqueEntityIds is violated.
```

The fix in production code is late binding at replay (`ProtoCodec.IdBinding`):
`Transaction.apply` binds the writer's negative placeholder id to the
replaying state's next id in log order. The model's `Bind` does the same,
at the moment the record lands: replay is a deterministic fold over the
file, so binding against the checkpoint plus the log prefix before the
record gives the ids every reader assigns. Every other config sets
`LateBinding = TRUE` and checks S4; the two runs differ only in that
constant.

## TLC version

Pinned to `tla2tools.jar` from the TLA+ v1.8.0 release. Fetch:

```bash
mkdir -p spec/tla/tools
curl -L -o spec/tla/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
```

## Known gaps

What TLC explores (TLC `-coverage`, v1.8.0):

| Config | `DoAppend` | `DoCAS` | `Exhaust` | "already committed" branches |
|---|---|---|---|---|
| MC_small, MC_medium | taken | never | never | never |
| MC_cas_only | never | taken | never | never |
| MC_ids_bug | taken | never | never | never |

- **Append always lands; the offset check is not modelled as a mutex.**
  Real backends (content-addressed append) return the actual landing
  offset, so a stale view does not block the write. This matches
  production behaviour and is what makes MC_ids_bug reach the collision.
- **Compaction never interleaves with appends.** Each writer commits one
  transaction, so the log holds at most `|Writers|` entries and never
  reaches `MaxLog = 4`. No config explores a compaction racing an append.
- **`Exhaust` is unreachable.** A writer only fails when the CAS path
  loses a race, and each writer commits once in the configs shipped.
- **S3 is vacuous.** Lost replies (the write lands, the writer does not
  learn it) are not modelled, so no writer retries a committed UUID.

Not modelled:

- transaction content beyond allocated-id sets (no action payloads,
  no entity versions, no read-time replay rejecting a landed record);
- the CAS path failing instead of retrying;
- the etag changing on appends (a CAS over appends it did not see would
  succeed in the model; unreachable in the shipped configs); the
  size-based compaction trigger;
- committed-set GC and its retention window;
- byte-level encoding and the inline-metadata structures (partly covered
  by the jqwik suite).
