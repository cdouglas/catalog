# TLA+ model of the FileIOCatalog commit loop

Companion to the jqwik property suite at
`src/test/java/org/apache/iceberg/io/spec/`.

The model is deliberately abstract: the catalog file is a record of `etag`,
`length`, a log of transaction UUIDs, and a bookkeeping set `committed`; a
transaction is just a UUID. Writers read, choose append or CAS, and retry on
conflict. See [Known gaps](#known-gaps) for what that abstraction cannot
detect.

## Property catalogue

| ID | Property | What it checks | Config |
|----|----------|----------------|--------|
| S1 | `S1_LogEntriesCommitted` | Every log entry is in `committed` (holds by construction) | MC_small, MC_cas_only, MC_medium |
| S2 | `S2_NoLostCommit` | `committed` never shrinks | MC_small, MC_cas_only, MC_medium |
| S3 | `S3_IdempotentReplay` | No UUID appears twice in the log (vacuous; see below) | MC_small, MC_cas_only, MC_medium |
| L1 | `L1_Termination` | Every writer eventually finishes (committed or exhausted) | MC_liveness_L1 |
| L2 | `L2_NoStarvation` | Every writer eventually commits or exhausts its retries | MC_liveness_L2 |
| L3 | `L3_BoundedRetries` | `attempt <= MaxRetries` (an invariant) | MC_liveness_L3 |

## Configurations

| Config | Writers | Tables | MaxLog | MaxRetries | Properties | Distinct states |
|--------|---------|--------|--------|------------|------------|-----------------|
| MC_small | 2 | 2 | 4 | 3 | S1, S2, S3 | 108 |
| MC_cas_only | 2 | 2 | 0 | 3 | S1, S2, S3 | 108 |
| MC_medium | 3 | 3 | 4 | 3 | S1, S2, S3 | 6,104 |
| MC_liveness_L1 | 2 | 2 | 4 | 3 | L1 | 108 |
| MC_liveness_L2 | 2 | 2 | 4 | 3 | L2 | 108 |
| MC_liveness_L3 | 2 | 2 | 4 | 3 | L3 | 108 |

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

- **Compaction never interleaves with appends.** Each writer commits one
  transaction, so the log holds at most `|Writers|` entries and never reaches
  `MaxLog = 4`. No config explores a compaction racing an append.
- **`Exhaust` is unreachable.** A writer only fails when another commits, and
  each writer commits once, so no writer fails `MaxRetries` times.
- **S3 is vacuous.** Lost replies (the write lands, the writer does not learn
  it) are not modelled, so no writer retries a committed UUID.
- **The append path is unchecked.** Removing the offset check from `DoAppend`
  still passes MC_small and MC_medium: an unconditional append also adds its
  UUID to `committed`. (Removing the etag check from `DoCAS` does violate S2.)

Not modelled:

- transaction content — actions, entity versions, ids — and read-time
  replay, where `verify` can reject a record whose append succeeded;
- append retry with the same bytes at the new offset without re-reading
  (the model re-reads), and the CAS path failing instead of retrying;
- the etag changing on appends; the size-based compaction trigger;
- committed-set GC and its retention window;
- byte-level encoding, the inline-metadata structures, and id allocation
  (some of these are covered by the jqwik suite).
