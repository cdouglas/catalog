# Reading the log's tail (design)

Status: proposed 2026-11-06, revised 2026-11-09 after review; nothing built.
It answers errata D15 (whole-object reads) and S5 (a read's continuity is the
store's word): on one object by a format change, with T6 (the fuzzers pin
reads) as its test, and on any log by positions in the SPI, which close S4
(shared logs' checkpoint joints). It comes before R2.3 of
[CHECKPOINT_PLAN.md](CHECKPOINT_PLAN.md) (a cost-based compaction policy),
whose costs it changes.

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

| Mode      | Tables | MiB a GET | GETs a merge append | MiB read a merge append | p50                  |
| --------- | ------ | --------- | ------------------- | ----------------------- | -------------------- |
| inline ML | 100    | 2.5       | 2.1                 | 5.1                     | 149 ms               |
|           | 300    | 7.5       | 2.4                 | 18.3                    | 350 ms               |
|           | 1,000  | 24.3      | 4.1                 | 99.3                    | 8,528 ms (saturated) |
| inline TM | 100    | 0.8       | 2.1                 | 1.7                     | 128 ms               |
|           | 300    | 2.5       | 2.3                 | 5.7                     | 163 ms               |
|           | 1,000  | 8.2       | 3.1                 | 25.4                    | 558 ms               |
| pointer   | 3,000  | 1.1       | 3.2                 | 3.5                     | 242 ms               |

Every one of these commits appended. E1's tables are shallow, so snapshot
paging (SNAPSHOT_PAGING.md) does not shrink them: the object grows with the
number of tables, about 25 KB a table inline ML and 8 KB inline TM here. At
1,000 inline-ML tables one GET of the catalog is about 170 ms of transfer, and
a merge append made four.

Only a store that appends gains: S3 Express and ADLS. On S3 general-purpose
buckets and GCS every commit replaces the object, so a read after another
writer's commit reads a new object, whole, as now.

## Positions: what every log's reads must show

Tail reads are one object's case of a question every log answers: how a
reader knows that what a read returned is the log (P1, LOG.md). It needs
three things, whatever the deployment:

1. **One prefix.** The view's checkpoint and records are one prefix of the
   log, not pieces of two versions of it (on S3 today, T6).
2. **The joint.** The records start where the checkpoint ends, with none
   missing between (S4: a log that stores its checkpoints apart, such as
   checkpoints in S3 and records in Kafka).
3. **A suffix continues.** The records a read returns after a tail the
   reader holds (`SharedLog.readAfter`) follow that tail, and are not those
   of a log that replaced it (D15).

Each is a statement about positions. Every record has one in the log's
order, and every checkpoint covers the log through one; a read is right if
each record's position follows the one before it, starting at the
checkpoint's or at the held tail's. An overlap is harmless: a record that a
checkpoint holds, read again after it, does not apply twice (replay drops it
by its id in the committed set, or by the floor; the commit-protocol
fuzzer's DUPLICATE steps). A gap or a splice is not: the missing records'
effects are lost, and a spliced view is a state the log never held.

What prepared a record does not matter here. A record written against
checkpoint C_k that lands after C_{k+1} is judged where it lands: replay
checks its preconditions there. The reader needs only to know truly where
each record landed.

Who knows the positions depends on the log:

- **A log that assigns them** (CORFU's sequencer, Kafka's offsets,
  `InMemorySharedLog`'s 1, 2, ...) returns each record with its position, so
  its reads are already positions. What it lacks is the joint. The SPI hands
  a provider the position a checkpoint covers (`putCheckpoint(LogTail
  through, ...)`), and `InMemorySharedLog` keys its checkpoints by it, but
  the checkpoint's bytes do not carry it, and no read checks it. With
  checkpoints in S3 and records in Kafka, Kafka's retention deletes records
  whatever the checkpoints say, and a consumer whose reset policy is
  `earliest` skips the gap silently.
- **One object** (`AtomicFileLog`) has none to return: a byte store returns
  bytes. Its writer knows each record's position exactly, though, since its
  append is conditioned on the exact version and length it extends; so it
  writes the position into the frame. That is the trailer below: the
  object's incarnation and the frame's offset are the record's position,
  and the checksum binds them to the frame's bytes. A replace writes the
  checkpoint and its records in one write, so the trailers check the joint
  too.

Writes need none of this. A write is conditioned on the tail its writer
read (P4), so it lands only where the writer expected. `AtomicFileLog`'s
receipt for an append from a view it read or wrote holds that view and the
new record (`append`), contiguous by the store's condition, not by a check.
Only reads rest on the store's word.

So, for every log:

- A checkpoint names the position it covers: `LogCheckpoint.through`,
  opaque bytes the provider encodes from the tail the compaction read. A
  read checks that its records start there.
- A view of records alone (`readAfter`) states the position they start
  after, and the runtime folds them onto a replica only if that is the
  replica's tail. `LogRuntime.refresh` folds them unchecked today; the
  catalog does not call it (only tests do), but tail reads would. A whole
  view's fold already compares bytes (`LogRuntime.continued`).
- Each provider checks its records' chain by its own rule: dense for a
  sequencer, the trailers for one object, and its own for a log whose
  positions skip (Kafka's transaction markers take offsets).

Resyncing on a suffix is then an optimization a log offers or not. From a
tail the reader holds, the provider returns what follows it, or a whole view,
or fails (`TrimmedException`), and the runtime reads whole. The hazard is a
suffix read that should fail and does not. A Kafka fetch at a trimmed offset
fails if the consumer's reset policy is `none`. An S3 GET cannot be
conditioned on "still the object I read" (an append changes the ETag), so on
one object the check is on the bytes that come back.

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
  at N for records would replay a state the log never held. The worst case
  is a frame boundary at N in the new object: the compaction that wrote
  C_{k+1} kept records whose frames happen to end there. The frames from N
  decode, and the reader would replay them after its own, missing whatever
  C_{k+1} holds before N. Only the position tells N in C_{k+1}'s object from
  N in C_k's. The reader need not reread the header to see it: the frame
  that ends at N names its object, in the 20 bytes the tail read's GET
  starts with (below).
- A version does not. An append changes the ETag or generation, and no store
  says whether one version extends another.
- A read pinned to a version (S3's `If-Match`, which the fork does not send;
  ADLS's ETag and GCS's generation, which it does) shows that the bytes come
  from one object, not that the object extends the prefix the reader holds.

And on S3 a read is not pinned at all (T6): a GET that races a replace, or a
stream the SDK reopens mid-read, can return the start of one object and the
rest of another. A splice at a frame boundary decodes today (S5).

The fork's S3 reads send no `If-Match`. `S3InputStream.openStream` and
`readRange` ask for the bucket, the key and a range; `S3InputFile`'s ETag
conditions writes alone. The contract suite
(`SupportsAtomicOperationsContractTest`) does not notice, because it tests
writes: its 16 cases are creates, CAS, appends, their races and the segment
limit, and none reads across a write. A read is unpinned only between its
HEAD and its GET, or across a reopen after a read error, and nothing opens
that window: the catalog suites' races are writes, and a read that raced
one fails at its reader's next write, which is conditioned on the HEAD's
version (P4). Under the fuzzers, `InMemoryAtomicFileIO` pins every read
(T6). A contract case would open it: bind a file to a version, replace the
object, then read the file. It must return the old version's bytes or fail.
It would fail on S3 today, and it would say whether S3 Express honours
`If-Match` on a GET.

So the format carries what a tail read checks, as S5 proposes: each frame
names the object it belongs to and where it starts, with a checksum over
both and its bytes. A nonce in the checkpoint alone, which a provider of
separate checkpoints needs ([Positions](#positions-what-every-logs-reads-must-show)),
is not enough on one object: the checkpoint is at the object's head, so a
tail read would need another request to see it, and a whole read spliced
after the checkpoint would still decode, since the frames after it would
name nothing.

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
- Positions in the SPI (intentlog-core): `LogCheckpoint.through` (the
  provider's encoding of the tail a compaction read), a view of records
  alone stating the tail it starts after, and the runtime's checks of both
  (a whole view's joint; a fold only onto the tail a view starts after).
  `InMemorySharedLog` encodes its positions; `AtomicFileLog`'s joint is its
  trailers. LOG.md's P1 says what a provider must check, not only provide.
- The fork's S3 reads pinned by `If-Match` to the version the file is bound
  to, across reopens (`S3InputStream`), and the contract case for a read
  across a write (`SupportsAtomicOperationsContractTest`).
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
- Gaps on a shared log (S4): a `MemoryLogHarness` mode that serves a
  checkpoint and records with a gap between them, or trims past its newest
  checkpoint, and a `SharedLogContract` case; every such read must fail.
- The contract case for a read across a write, on every store (real cloud),
  before and after S3's reads are pinned.
- Mutants (`fuzz/mutants`): a tail read that checks the length only, no
  checksum, a resend after a probe framed with its old tail's incarnation.
  Each must break a fuzzer.
- The file model (`spec/tla/CatalogCommit.tla`): the object gains its
  incarnation and each record its frame; readers hold replicas and tail-read;
  a read may return a later version or splice two (S3); the oracle that no
  replica holds a state the log never held; bug switches for continues by
  length, no trailer check, and a resend after a probe. The shared-log
  model (`SharedLogCommit.tla`) gains a provider that serves a gap, the
  same oracle, and a switch for an unchecked joint (S4).
- Havoc rounds in each read mode, as for the earlier steps.

## Order of work

Each step stands alone and is measured before the next:

0. **Pin S3's reads** (a fork change), with the contract case for a read
   across a write. It closes T6's whole-read half on the store's word, at
   no cost, and turns a read that raced a replace into a 412 instead of a
   transfer that fails the format's checks later. If S3 Express does not
   honour `If-Match` on a GET, the case says so, and step 2 is its only
   check.
1. **Pricing.** Ranged reads priced by their bytes in `LatencyFileIO` and
   catalog-bench. No behaviour changes; catalog-bench's numbers must not
   move.
2. **Format 3 and checked reads.** Trailers, the checks, a read again on a
   failed check, the explorer, the read modes in `InMemoryAtomicFileIO`, and
   fuzz rounds in each mode. This alone closes T6 and the one-object half of
   S5: whole reads detect a splice, whatever the store returns.
3. **Positions in the SPI** (S4). The checkpoint's `through`, the joint's
   check, views of records that say where they start, and the gap-serving
   harness and contract case, on `InMemorySharedLog`. It closes S5's
   shared-log half for providers that pass the contract.
4. **The model.** Incarnations, splices and tail reads in
   `CatalogCommit.tla`, and gaps in `SharedLogCommit.tla`, with the
   switches, before the Java that relies on them (as for
   COMMITTED_PAGING.md's stage 2).
5. **Tail reads.** `readAfter`, `read(floor)`, the commit loop, the switch,
   the mutants, havoc rounds.
6. **Measure.** catalog-bench E1 at 300 and 1,000 tables on S3 Express, inline
   ML and TM, tail reads on and off; and E3's deep tables, whose sizes after
   paging campaign `e3f-sp4` gives.
7. **One request a read** (if step 6 shows the HEAD): a ranged GET that
   reports the version and length it read (S3's `ETag` and `Content-Range`,
   ADLS's and GCS's equivalents), so a tail read is one request. A fork
   change to the input files.

Then R2.3, with a read's cost known.

## Alternatives considered

- **Pinned reads alone** (step 0, without format 3). They stop a whole
  read splicing, but not a tail read misreading a replace that grew
  (above), and they are each store's guarantee rather than the format's.
- **The incarnation in the object's metadata** (S3's `x-amz-meta-*`, ADLS's
  metadata), which a replace sets, an append keeps, and the HEAD returns.
  With pinned reads, a tail read would be a HEAD (version, length,
  incarnation) and a GET pinned to that version, with no change to the
  format, and a probe would learn the incarnation, so the commit loop could
  resend after one. Each check would rest on three behaviours of each store
  (a pinned GET, across reopens; metadata kept by an append; metadata in a
  HEAD), each a contract case on real cloud, where the trailer rests on
  none, is tested by the fuzzers' read modes alone, and leaves the bytes
  self-describing for tools (the format explorer) and copies. A smaller
  change, if the user prefers a store's word per deployment.
- **The incarnation in the header or the checkpoint only**, read by a
  second ranged GET. S3 serves one range a GET, so a tail read would take
  three requests, and a whole read spliced after the checkpoint would still
  decode; the trailer puts the check in the range the read needs anyway.
- **A digest chain** (each frame naming its predecessor's digest). It detects
  what offsets and checksums do, at the cost of a digest the writer must hold
  for the frame before; the offset is checkable from the trailer alone.
- **The trailer at the frame's head.** A reader would have to read from the
  start of the frame that ends at N, which may be the checkpoint.
- **Records in a separate object** (a tail object beside a checkpoint object).
  A different provider, whose joint the checkpoint's `through` checks
  ([Positions](#positions-what-every-logs-reads-must-show)); out of scope.

## Open questions

- **The checksum's cost on a large checkpoint.** CRC32C runs at several GB/s
  in the JDK; `ReplayBench`'s `decodeCheckpoint` measures it in step 2.
- **A writer that read the records that beat it** (after a lost append race)
  could check its intention against them and stop before sending a record
  that will not apply. The loop resends now, and replay rejects it; whether
  to stop early is a commit-loop change, measured with step 4.
- **A replica shared across a process's catalogs**, for engines that make a
  catalog per query.
- **A log whose positions skip**, such as Kafka's, where transaction
  markers take offsets: its provider's rule for one record following
  another. No production shared log exists; step 3 builds positions on the
  in-memory one, whose positions are dense.
- **S3 Express and `If-Match` on a GET**: step 0's contract case says
  whether directory buckets honour it.
