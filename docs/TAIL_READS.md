# Reading the log's tail (design)

Status: proposed 2026-11-06; nothing built. It answers errata D15 (whole-object
reads) and the one-object half of S5 (a read's continuity is the store's
word), with T6 (the fuzzers pin reads) as its test; S4 (shared logs' checkpoint
joints) stays open. It comes before R2.3 of [CHECKPOINT_PLAN.md](CHECKPOINT_PLAN.md)
(a cost-based compaction policy), whose costs it changes.

## Context

`AtomicFileLog` reads the whole catalog object every time: a HEAD for its
version and length, then a GET of every byte. A writer that holds a replica
replays only the records after it (LOG.md "Replay"), but still transfers the
checkpoint. Since D20 (four reads per commit) a commit through Iceberg reads
the object twice: the table's load and `SnapshotProducer.apply`'s refresh,
both `CatalogLog.read()`. An append that lost a race reads it once more: the
loop probes the new length and resends without reading, so the read through
the resend has no bytes to extend and reads the object back.

catalog-bench measured it (campaign `e1-d20`, S3 Express `s3x-2026-11`, which
prices a read at 7 ms a MiB; the lowest offered load, three runs each):

| Mode | Tables | MiB a GET | GETs a merge append | MiB read a merge append | p50 |
|---|---|---|---|---|---|
| inline ML | 100 | 2.5 | 2.1 | 5.1 | 149 ms |
| | 300 | 7.5 | 2.4 | 18.3 | 350 ms |
| | 1,000 | 24.3 | 4.1 | 99.3 | 8,528 ms (saturated) |
| inline TM | 100 | 0.8 | 2.1 | 1.7 | 128 ms |
| | 300 | 2.5 | 2.3 | 5.7 | 163 ms |
| | 1,000 | 8.2 | 3.1 | 25.4 | 558 ms |
| pointer | 3,000 | 1.1 | 3.2 | 3.5 | 242 ms |

Every one of these commits appended. E1's tables are shallow, so snapshot
paging (SNAPSHOT_PAGING.md) does not shrink them: the object grows with the
number of tables, about 25 KB a table inline ML and 8 KB inline TM here. At
1,000 inline-ML tables one GET of the catalog is about 170 ms of transfer, and
a merge append made four.

Only a store that appends gains: S3 Express and ADLS. On S3 general-purpose
buckets and GCS every commit replaces the object, so a read after another
writer's commit reads a new object, whole, as now.

## What a tail read must show

A reader holds a replica read at a tail: the object's version and its length
N, and the bytes up to N. It wants the bytes from N to the end, and the
object's version there, as the next conditional write's condition. The bytes
from N on are the log's records after the replica only if the object is still
the one the replica read, extended by appends. Nothing in the format says so
today:

- A length does not. A replace can write an object longer than N (a
  compaction that keeps a large tail, or a checkpoint that grew), and the
  bytes at N then fall anywhere in it. `FileTail.continues` compares lengths
  and so reads such a replace as growth; the commit loop survives it (a
  resend of the same record applies once), but a reader that took the bytes
  at N for records would replay a state the log never held.
- A version does not. An append changes the ETag or generation, and no store
  says whether one version extends another.
- A read pinned to a version (S3's `If-Match`, which the fork does not send;
  ADLS's ETag and GCS's generation, which it does) shows that the bytes come
  from one object, not that the object extends the prefix the reader holds.

And on S3 a read is not pinned at all (T6): a GET that races a replace, or a
stream the SDK reopens mid-read, can return the start of one object and the
rest of another. A splice at a frame boundary decodes today (S5).

So the format carries what a tail read checks, as S5 proposes: each frame
names the object it belongs to and where it starts, with a checksum over
both and its bytes.

## The format (version 3)

The header stays `LCAT` and a version, now 3. Each frame (the checkpoint, and
every record) gains a fixed 20-byte trailer:

```
frame   = varint-length body trailer
trailer = incarnation:fixed64 offset:fixed64 crc:fixed32
```

- **incarnation**: a random 64-bit id that each replace draws for the object
  it writes, in every frame it writes (the checkpoint and the records after
  it); an append writes the incarnation of the object its condition names.
  No two objects share one, so a frame naming X was written into X.
- **offset**: the byte offset at which the frame starts. An append is
  conditioned on the exact version and length it extends (ATOMIC_FILE_LOG.md
  "Tails"), so within one incarnation the frames are contiguous and each
  offset is the end of the frame before.
- **crc**: CRC32C over the length, the body, the incarnation and the offset.
  It catches a splice inside a frame, which the other two fields cannot: a
  trailer from one object after a body spliced from another.

The trailer comes last so that a reader can check the frame that ends at N
from its last 20 bytes, whatever the frame's size (the checkpoint's is the
object's bulk). It costs 20 bytes a frame, about 6% of a 330-byte record. The
trailer is `AtomicFileLog`'s framing, not log.proto's: the runtime's records
and checkpoints are the same bytes as now.

Reading checks every frame: one incarnation throughout, each offset where the
last frame ended, each checksum. A read that fails a check raced a replace
(S3) or was served wrong; `AtomicFileLog` reads again, up to the throttle
attempts, and then fails the read. Today such a read, if it split at a frame
boundary, decoded.

## Reads

**A whole read** (`read()`) is as now, a HEAD then a GET of every byte,
checked as above. A view records its incarnation and its frames' offsets.

**A tail read** (`readAfter(since)`), from a tail whose bytes this log read or
wrote (incarnation X, length N):

1. A HEAD: the version V and its length L. If L < N, the object was replaced:
   a whole read (a GET of V's bytes).
2. A ranged GET of [N − 20, L): the trailer of the frame that ends at N, then
   the frames after it.
3. If those 20 bytes are not the trailer the replica holds, the object was
   replaced (it is no longer X, or no longer X's bytes at N): a whole read.
   Else every frame after N must name X, start where the last ended, and
   check; a read that fails is read again, as above.
4. The view: the replica's checkpoint and records (the same arrays) and the
   new records, at a tail bound to V and L.

On S3 the GET is not pinned to V, but it follows the HEAD, and S3's reads
are strongly consistent: the GET serves V or a later version. X's versions all come before
any replace of X, and no later object is X. So a GET whose bytes at N − 20
are X's trailer read a version of X at or after V, and V is X's too; up to L
its bytes are V's, since appends only extend. A GET that spliced a replace in
fails a check. ADLS and GCS read the version the HEAD found, as now.

The view a tail read returns continues the replica's view byte for byte, so
the runtime folds the new records onto the replica (LOG.md "Replay") and the
comparison is of the same arrays. A tail read's tail is one this log read,
so an append from it knows what the object holds, and its read through needs
no read (D20).

**A probe** (`probe()`, a HEAD) stays for what only needs existence and length
(`LogRuntime.empty`, `bootstrap`). The commit loop stops probing after a lost
append race and tail-reads instead, for two reasons:

- the resend must name the incarnation of the object it lands in, and a HEAD
  does not say it (a resend framed with an old incarnation into a replace
  would make the object unreadable);
- with the records that beat it in hand, the resend's read through needs no
  read: today that read is a whole GET.

So an append from a tail whose bytes the log does not know is refused
(`IllegalArgumentException`); the runtime never sends one. `FileTail.continues`
becomes: the same incarnation, and no shorter. A replace never continues a
tail, so the commit loop's "a replace that made the log longer reads as
growth" case goes, though the floor still rejects a duplicate a write in
flight lands.

## The runtime and the catalog

- `LogRuntime.read(floor)`: a tail read from the floor's tail when the floor
  is a replica of this log with a view (the fold then replays only the new
  records), else a whole read. `CatalogLog.read()` passes the newest replica
  its catalog has seen, so every load and refresh through Iceberg becomes a
  tail read once the catalog has read the object.
- The commit loop: its reads after a lost race, an ambiguous write and an
  exhausted loop tail-read from the replica it holds (`current`); a replace
  since sends them to a whole read.
- `refresh(prior)` already calls `readAfter`.

What remains whole: a client's first read, and the first read after each
replace (a compaction), once per client. With `max.append.count` at 100 and k
clients, about k/100 of the object a commit, against two objects a commit
now. That shifts the compaction trade toward compacting less often, which is
R2.3's to weigh: after a compaction every reader pays the whole object.

A process that makes a `FileIOCatalog` per query holds no replica and reads
whole; a replica shared by location across a process's catalogs is open.

## Pricing

The latency model must price a ranged GET by its bytes. `LatencyFileIO`
(fileio-catalog's tests, and catalog-bench's `sut-fileio`) charges each
`newStream` the latency and the bytes of the whole object (`Requests`). It
should charge a stream's GET when it first reads, by the range it reads (a
`RangeReadable.readFully`, or from the position a seek left to the end), so a
tail read costs a HEAD and a GET of its bytes. catalog-bench's tiered FileIO
already counts a GET per stream and the bytes a stream returns (`IoStats`);
its time is `LatencyFileIO`'s.

## What changes in the code

- `AtomicFileLog`: format 3 (trailers on encode, checks on decode, a read
  again on a failed check), incarnations (drawn per replace, carried by
  views and tails), `readAfter`, `FileTail.continues` by incarnation, an
  append refused without a view. `encode`/`decode` keep serving tests and
  tools.
- `LogRuntime`: `read(floor)` and the commit loop's reads through
  `readAfter`; no probe after a lost append race.
- The ablation switch `fileio.catalog.read.tail` (default true; false reads
  whole, as before), since the paper reports it (CHECKPOINT_PLAN.md
  "Ablation switches"). The format's checks have no switch.
- `LatencyFileIO`, `InMemoryAtomicFileIO` (ranged reads of a bound file, and
  the read modes below), catalog-bench's tiered FileIO.
- The format explorer decodes version 3 (frames and trailers;
  `FormatExplorerFixtures` regenerated, `check.mjs`).
- ATOMIC_FILE_LOG.md (layout, operations, tails), LOG.md (`readAfter` on one
  object), errata D15, S5, T6.

## Testing

- `InMemoryAtomicFileIO` read modes (T6): `pinned` (a bound read reads its
  version, as now), `live` (a bound read returns the live generation's bytes,
  as an unpinned S3 GET does), `splice` (a read returns the start of one
  generation and the rest of the live one, at a random offset, as a reopened
  S3 stream can). The fuzzers draw the mode: `TestAtomicFileCommitProtocol`,
  `TestCatalogFaultProperties`, `TestMultiWriterProperties`,
  `TestCatalogRaceProperties`. Their oracles check replicas against the
  history (the commit protocol fuzzer's committed-set oracles, the fault
  fuzzer's loads through a fresh reader); they gain one: no replica holds a
  state the log never held.
- `AtomicFileLog` unit tests: a replace longer than the tail, a replace of the
  same length, a splice at and inside a frame, a tail read with nothing new,
  a tail read whose GET finds the object shorter than its HEAD did.
  `SharedLogContract`'s `readAfter` case on `AtomicFileLog`.
- Mutants (`fuzz/mutants`): a tail read that checks the length only, no
  checksum, a resend after a probe framed with its old tail's incarnation.
  Each must break a fuzzer.
- The file model (`spec/tla/CatalogCommit.tla`): the object gains its
  incarnation and each record its frame; readers hold replicas and tail-read;
  a read may return a later version or splice two (S3); the oracle that no
  replica holds a state the log never held; bug switches for continues by
  length, no trailer check, and a resend after a probe. The
  shared-log model is unchanged (S4).
- Havoc rounds in each read mode, as for the earlier steps.

## Order of work

Each step stands alone and is measured before the next:

1. **Pricing.** Ranged reads priced by their bytes in `LatencyFileIO` and
   catalog-bench. No behaviour changes; catalog-bench's numbers must not
   move.
2. **Format 3 and checked reads.** Trailers, the checks, a read again on a
   failed check, the explorer, the read modes in `InMemoryAtomicFileIO`, and
   fuzz rounds in each mode. This alone closes T6 and the one-object half of
   S5: whole reads detect a splice, whatever the store returns.
3. **The model.** Incarnations, splices and tail reads in
   `CatalogCommit.tla`, with the switches, before the Java that relies on
   them (as for COMMITTED_PAGING.md's stage 2).
4. **Tail reads.** `readAfter`, `read(floor)`, the commit loop, the switch,
   the mutants, havoc rounds.
5. **Measure.** catalog-bench E1 at 300 and 1,000 tables on S3 Express, inline
   ML and TM, tail reads on and off; and E3's deep tables, whose sizes after
   paging campaign `e3f-sp` gives.
6. **One request a read** (if step 5 shows the HEAD): a ranged GET that
   reports the version and length it read (S3's `ETag` and `Content-Range`,
   ADLS's and GCS's equivalents), so a tail read is one request. A fork
   change to the input files.

Then R2.3, with a read's cost known.

## Alternatives considered

- **Pin S3's reads by `If-Match`** (the fork's `S3InputStream`). It stops a
  whole read splicing, but not a tail read misreading a replace that grew
  (above), and it is S3's guarantee rather than the format's. Worth adding
  later as a cheaper failure (a 412 instead of a transfer that fails its
  checks), if S3 Express honours it on GET.
- **The incarnation in the header only**, read by a second ranged GET. S3
  serves one range a GET, so a tail read would take three requests; the
  trailer puts the check in the range the read needs anyway.
- **A digest chain** (each frame naming its predecessor's digest). It detects
  what offsets and checksums do, at the cost of a digest the writer must hold
  for the frame before; the offset is checkable from the trailer alone.
- **The trailer at the frame's head.** A reader would have to read from the
  start of the frame that ends at N, which may be the checkpoint.
- **Records in a separate object** (a tail object beside a checkpoint object).
  A different provider, with the joint S4 describes; out of scope.

## Open questions

- **The checksum's cost on a large checkpoint.** CRC32C runs at several GB/s
  in the JDK; `ReplayBench`'s `decodeCheckpoint` measures it in step 2.
- **A writer that read the records that beat it** (after a lost append race)
  could check its intention against them and stop before sending a record
  that will not apply. The loop resends now, and replay rejects it; whether
  to stop early is a commit-loop change, measured with step 4.
- **A replica shared across a process's catalogs**, for engines that make a
  catalog per query.
- **S4**: a shared log's checkpoint naming the last record it covers. No
  production shared log exists, and `SharedLogContract` checks P1 and P5 on
  the in-memory one; S4's trigger stands.
