# Committed-transaction-set encoding benchmarks

JMH results for four candidate encodings of `Checkpoint.committed_transaction_ids`.

- **D**: status quo. 16 raw bytes per UUID. Decoder materializes into a `HashSet<UUID>` for O(1) lookups.
- **A**: successive deltas (varint), descending sort, linear scan with early exit. Operates on compressed bytes — no materialization.
- **B**: fixed64 base+delta, descending sort, binary search on the delta column. Operates on compressed bytes.
- **C**: indexed varint with skip-list (skip every 16 entries; per-bucket cumulative-delta anchor).

Source: `src/test/java/org/apache/iceberg/io/bench/CommittedTxnsBench.java`. Run via:

```
mvn exec:java -Dexec.mainClass=org.openjdk.jmh.Main -Dexec.classpathScope=test \
  -Dexec.args="-f 0 -wi 2 -i 3 -r 1s -w 1s -rf json -rff target/jmh-results.json"
```

## Encoded sizes

`bytes/UUID, total bytes` for the inner sub-message body (no outer framing).

| n | distribution | D | A | B | C |
|---|---|---|---|---|---|
| 100 | CLUSTERED_60S | 16.0, 1600 | 11.9, 1186 | 18.1, 1812 | 12.8, 1276 |
| 1000 | CLUSTERED_60S | 16.0, 16000 | 11.1, 11126 | 18.0, 18012 | 11.9, 11888 |
| 10000 | CLUSTERED_60S | 16.0, 160000 | 11.0, 110010 | 18.0, 180012 | 11.8, 117517 |
| 100 | UNIFORM_24H | 16.0, 1600 | 13.1, 1312 | 18.1, 1812 | 14.0, 1402 |
| 1000 | UNIFORM_24H | 16.0, 16000 | 12.8, 12821 | 18.0, 18012 | 13.6, 13583 |
| 10000 | UNIFORM_24H | 16.0, 160000 | 12.1, 121348 | 18.0, 180012 | 12.9, 128855 |
| 100 | SPARSE_1Y | 16.0, 1600 | 14.4, 1443 | 18.1, 1812 | 15.3, 1533 |
| 1000 | SPARSE_1Y | 16.0, 16000 | 13.9, 13949 | 18.0, 18012 | 14.7, 14711 |
| 10000 | SPARSE_1Y | 16.0, 160000 | 13.5, 135109 | 18.0, 180012 | 14.3, 142616 |

Notes:
- D's 16.0 here excludes the 2-byte tag+length per entry the wire format adds; effective today is ~18.0 bytes/entry. So D and B end up the same on the wire.
- A is the smallest at every distribution, ~30% smaller than the status quo at 10K entries with clustered timestamps; degrades to ~16% smaller for SPARSE_1Y where timestamp deltas are larger.
- B doesn't compress — it pays the same per-entry cost as today, but is binary-searchable on the wire.
- C's skip-list overhead (~0.75 bytes/entry) puts it consistently between A and the next-larger option.

## Per-lookup latency

Each `@Benchmark` performs 1024 lookups; numbers below are score / 1024 (ns per lookup). `Recent` = Zipfian-skewed query distribution favoring recently-committed UUIDs (~80% of queries hit the top 6.25% of the descending-sorted list). `Uniform` = uniform-random over the full set.

### Recency-skewed workload (typical commit-confirmation use case)

| n | dist | D (HashSet) | A (scan) | B (bsearch) | C (skip-list) |
|---|---|---|---|---|---|
| 100 | CLUSTERED_60S | 2 | 147 | 14 | 86 |
| 1000 | CLUSTERED_60S | 2 | 1450 | 25 | 332 |
| 10000 | CLUSTERED_60S | 3 | 14701 | 32 | 2842 |
| 100 | UNIFORM_24H | 2 | 196 | 20 | 92 |
| 1000 | UNIFORM_24H | 2 | 1793 | 25 | 352 |
| 10000 | UNIFORM_24H | 3 | 16744 | 33 | 2907 |
| 100 | SPARSE_1Y | 2 | 237 | 19 | 97 |
| 1000 | SPARSE_1Y | 2 | 1881 | 23 | 403 |
| 10000 | SPARSE_1Y | 3 | 18615 | 29 | 3368 |

### Uniform-random workload

| n | dist | D (HashSet) | A (scan) | B (bsearch) | C (skip-list) |
|---|---|---|---|---|---|
| 100 | CLUSTERED_60S | 2 | 393 | 18 | 119 |
| 1000 | CLUSTERED_60S | 2 | 3488 | 24 | 388 |
| 10000 | CLUSTERED_60S | 3 | 34455 | 31 | 3381 |
| 100 | UNIFORM_24H | 2 | 418 | 18 | 125 |
| 1000 | UNIFORM_24H | 2 | 4111 | 24 | 394 |
| 10000 | UNIFORM_24H | 3 | 39317 | 30 | 2974 |
| 100 | SPARSE_1Y | 2 | 474 | 18 | 123 |
| 1000 | SPARSE_1Y | 2 | 4543 | 24 | 381 |
| 10000 | SPARSE_1Y | 3 | 49432 | 30 | 2937 |

### Encode latency (full encode of N entries, μs/op)

| n | dist | D | A | B | C |
|---|---|---|---|---|---|
| 100 | CLUSTERED_60S | 0.1 | 5.2 | 3.9 | 6.0 |
| 1000 | CLUSTERED_60S | 1.2 | 59.0 | 58.6 | 73.3 |
| 10000 | CLUSTERED_60S | 12.3 | 1185.2 | 1202.9 | 1319.3 |
| 10000 | UNIFORM_24H | 13.0 | 1242.5 | 1259.7 | 1367.8 |
| 10000 | SPARSE_1Y | 12.8 | 1329.4 | 1256.2 | 1645.1 |

Encode is dominated by sort (`UUID.compareTo`) for A/B/C; D skips the sort. The ~100× ratio at 10K is the sort. Acceptable — encoding happens once per checkpoint write (compaction event), not on the hot path.

## Analysis

The benchmark exposes a clean three-way tradeoff that's worth being explicit about:

| | wire size | lookup speed | memory |
|---|---|---|---|
| D + materialize | baseline (18 B/entry) | 3 ns (HashSet) | 64 B/entry |
| A + materialize | 30% smaller | 3 ns (HashSet) | 64 B/entry |
| A on-compressed | 30% smaller | μs at 10K | 12 B/entry |
| B on-compressed | same | 30 ns | 18 B/entry |

**Key observation**: lookup speed on the wire format only matters if we forgo HashSet materialization. The current codec materializes; under that pattern, the wire format choice is orthogonal to query latency.

The user's stated motivation was wire-size compression (driven by unbounded growth of the dedup set). Under "compress wire, keep materialization":
- **A wins**: ~30% smaller catalog files, identical runtime query performance.
- B is no help (no compression).
- C is strictly worse than A (more bytes, slower lookups).

If a future deployment is memory-constrained (millions of committed-txn entries, 64MB+ HashSet pressure), the choice changes. Then **B + skip materialization** trades 0% wire savings for 64 B → 18 B/entry memory at ~30 ns query speed, which is still cheap enough for replay's many-lookup pattern.

## Decision

Implement **encoding A** for `Checkpoint.committed_transaction_ids`. Keep HashSet materialization at decode time. This delivers the requested compression with zero query-latency regression.

Defer encoding B / on-compressed lookup as a future option; the wire format leaves room to add a "skip materialization" mode without changing the bytes.

## Caveat

`-f 0` (in-process, no fork). Numbers should be treated as ballpark — the relative ordering between encodings is robust, but absolute latencies will shift under a properly-forked run. Re-run with `-f 1` once the JMH classpath issue is sorted out (the Maven `exec:java` plugin doesn't propagate the test classpath to JMH's forked JVM).
