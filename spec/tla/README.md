# TLA+ model for FileIOCatalog (V1a)

Companion artifact to the jqwik property suite at
`src/test/java/org/apache/iceberg/io/spec/`.

## Property catalogue

| ID | Property | Config | SPEC reference |
|----|----------|--------|----------------|
| S1 | Linearizable commit order | MC_small, MC_cas_only, MC_medium | SPEC.md §Commit Protocol |
| S2 | No lost committed txn | MC_small, MC_cas_only, MC_medium | design.md I2 |
| S3 | Idempotent replay | MC_small, MC_cas_only, MC_medium | design.md I2 |
| S4 | Compaction preserves committed set | MC_small, MC_cas_only, MC_medium | SPEC.md §Compaction |
| L1 | Eventual commit | MC_liveness_L1 | — |
| L2 | No starvation | MC_liveness_L2 | — |
| L3 | Bounded retries | MC_liveness_L3 | — |

## Configurations

| Config | Writers | Tables | MaxLog | MaxRetries | Properties | Distinct states |
|--------|---------|--------|--------|------------|------------|-----------------|
| MC_small | 2 | 2 | 4 | 3 | S1, S2, S3, S4 | 108 |
| MC_cas_only | 2 | 2 | 0 | 3 | S1, S2, S3, S4 (CAS-only branch) | 108 |
| MC_medium | 3 | 3 | 4 | 3 | S1, S2, S3, S4 | 6,104 |
| MC_liveness_L1 | 2 | 2 | 4 | 3 | L1 | 108 |
| MC_liveness_L2 | 2 | 2 | 4 | 3 | L2 | 108 |
| MC_liveness_L3 | 2 | 2 | 4 | 3 | L3 | 108 |

## Running

```bash
bash spec/tla/run-tlc.sh MC_small.cfg
```

or 

```java
cd spec\tla
java -cp tools\tla2tools.jar tlc2.TLC -workers auto -config MC_small.cfg FileIOCatalog
```

Each 2-writer config takes ~1 second (108 distinct states, depth 15).
`MC_medium` at 3 writers takes ~5 seconds (6,104 distinct states,
depth 28). The `-deadlock` flag is deliberately omitted — the PlusCal
translator emits a self-looping `Terminating` disjunct that would otherwise
be flagged as a deadlock.

## TLC version

Pinned to `tla2tools.jar` from the TLA+ v1.8.0 release. Fetch:

```bash
mkdir -p spec/tla/tools
curl -L -o spec/tla/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
```

Runs confirmed on SapMachine OpenJDK 25.

## Known gaps

The abstract model projects out byte-level encoding, inline-ML sentinel/pool structural invariants, and the detailed virtual-ID allocator; these are covered by the jqwik property suite.
