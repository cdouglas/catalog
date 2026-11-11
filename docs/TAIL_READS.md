# Reading the log's tail (design)

Status: proposed 2026-11-06, revised 2026-11-09 after two reviews, approved
2026-11-10: steps 1-7 with the trailer (not the incarnation in metadata),
step 3 included. Step 0 (reads of one version on every store) is built and
passed its real-cloud run, and step 0b's probes of the metadata alternative
passed on S3 Express and ADLS (2026-11-09).
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
   log, not pieces of two versions of it (on S3 and GCS before step 0, T6;
   since, on the store's word).
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
  replica's tail. `LogRuntime.refresh` folded them unchecked before step 3;
  the catalog does not call it (only tests do), but tail reads would. A whole
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
- A read pinned to a version (S3's and ADLS's `If-Match`, GCS's
  `if-generation-match`, since step 0) shows that the bytes come from one
  object, not that the object extends the prefix the reader holds.
- A precondition on the GET cannot say it either. A ranged GET can carry
  `If-Match`, and is then served from that version or not at all. But the
  version the reader holds is the one before the appends it wants, so
  `If-Match` on it fails exactly when there is something new to read; what
  the reader needs is "the same incarnation, extended", and no store offers
  that condition (each append changes the ETag or generation). So whatever
  names the object must come back with the bytes, in the same response: in
  the bytes themselves (the trailer, inside the range the read needs anyway)
  or in the response's headers (an incarnation in object metadata, which a
  GET returns; [Alternatives](#alternatives-considered)).

Before step 0, S3 and GCS reads were not pinned at all (T6): a GET that
raced a replace, or a stream reopened mid-read, could return the start of
one object and the rest of another. `S3InputStream` sent no `If-Match`, and
`GCSInputStream` read the live generation, not the one the file's lookup
found; only ADLS asked for one version. The contract suite did not notice,
because its 16 cases were writes. Step 0 (fork 5f14343338) pins every read to
the version its file has seen, fails a read of a superseded version as
`StaleReadException`, and adds three read cases to the suite: a file that
looked reads that version after a replace and after an append, and on a store
whose reads record their version (S3, ADLS), a file whose first request was a
read reads that version by a reopened stream, a range read and a new stream,
keeps its length, and fails a CAS rather than replace a version it did not
read. `AtomicFileLog` reads again after a stale read. A splice at a frame
boundary still decodes if a store breaks its contract (S5).

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

  The trailer is the writer's statement, and the writer can only make it
  truly: [Why a tail read is sound](#why-a-tail-read-is-sound) proves, by
  induction over the store's writes, that every frame names the object it
  is in, and walks two writers' retries through a replace.
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
   a whole read (a GET of V's bytes). On a store without appends
   (`supportsAppend()` false: S3 Standard, GCS) every change is a replace, so
   L > N is a whole read too: step 2's GET would only find a trailer that is
   not X's, and the whole read after it would cost a GET more (campaign `p4r`
   on S3 Standard: about one GET a commit, and more lost CAS races; see
   [Measured on S3 Standard](#measured-on-s3-standard)). L = N goes on to
   step 2, whose 20 bytes tell an unchanged object from a replace of the same
   length.
2. A ranged GET of [N − 20, L): the trailer of the frame that ends at N, then
   the frames after it.
3. If those 20 bytes are not the trailer the replica holds, the object was
   replaced (it is no longer X, or no longer X's bytes at N): a whole read.
   Else every frame after N must name X, start where the last ended, and
   check; a read that fails is read again, as above.
4. The view: the replica's checkpoint and records (the same arrays) and the
   new records, at a tail bound to V and L.

The GET asks for V (step 0), so its bytes are V's or it fails as stale and
the read starts again. A GET whose bytes at N − 20 are X's trailer read V,
and V is X extended: within one incarnation every write is an append, so V's
bytes up to N are the replica's. The trailer stands in for the byte
comparison a whole read's fold makes ([Replaying a suffix](#replaying-a-suffix)).

The view a tail read returns continues the replica's view byte for byte, so
the runtime folds the new records onto the replica (LOG.md "Replay") and the
comparison is of the same arrays. A tail read's tail is one this log read,
so an append from it knows what the object holds, and its read through needs
no read (D20).

**A probe** (`probe()`, a HEAD) stays for what only needs existence and length
(`LogRuntime.empty`, `bootstrap`). The commit loop stops probing after a lost
append race and tail-reads instead.

Today the loop resends the same record onto whatever base a probe finds.
After a lost append race it takes a HEAD (the ETag and the length) and, if
the object is no shorter, appends the record again conditioned on that ETag,
without reading what it now follows. That includes a replace that made the
object longer: `continues` compares lengths, so a new checkpoint reads as
growth, and the record lands after a checkpoint its writer never read. That
is safe for the record. Its bytes name no base, replay checks its
preconditions where it lands, and a copy of a record that already landed
applies once (by its id in the committed set, or the floor). The writer only
learns nothing about what it appended after, so its read through the write
reads the object back (D20).

In format 3 the record's bytes stay the same on every send, but its frame does
not. `append` builds each frame from the tail the send is conditioned on: the
incarnation from that tail's view, the offset from its length, and the
condition from its version, all from one pinned read. A probe's tail has a
version and no view, so a frame for it would take its incarnation from an
older read. Appended under the probe's ETag, it would carry the old object's
incarnation into the new one, and every check of the new object would fail
from that frame on, until a replace. So:

- the resend must name the incarnation of the object it lands in, and a HEAD
  does not say it (with the incarnation in metadata, a HEAD would: see
  [Alternatives](#alternatives-considered));
- with the records that beat it in hand, the resend's read through needs no
  read: today that read is a whole GET.

So an append from a tail whose bytes the log does not know frames its record
from that version's last trailer, read pinned to it (a 20-byte GET; built in
step 2), rather than from an older read: every frame then comes from a pinned
read of the version its condition names (A4). With tail reads (step 5) the
commit loop sends none on one object; with them off (the ablation switch) it
still probes and resends this way, which the model checks (`probe-*`) and the
`probeinc` switch breaks. `FileTail.continues` becomes: the same incarnation,
and no shorter, where the log read or wrote the bytes at both tails; a probed
tail is compared by length, and its resend's frame is still true.

### Measured on S3 Standard

The step 6 campaigns (`tr6`, `tr6k`, `tr6m`) ran S3 Express only. Campaign
`p4r` (catalog-bench, 2026-11-11: P4's E1 and E3 at fileio main after step
6b) was the first on S3 Standard, a store without appends, and every one of
its S3 Standard points was slower than before. Where every commit replaces
the object, a tail read of a changed object no shorter than the replica's
GETs the bytes from N − 20, finds a trailer that is not X's, and reads the
object again whole: a GET more a read. Campaign `tr6s` ran p4r's S3 Standard
points with tail reads on and off (catalog-bench f03ea57, fileio c015f5d,
havoc, two runs an arm). E1 at 300 tables, a merge append's latency and its
catalog requests:

| Mode | Workers (offered a second) | Tail reads | committed a second | p50 | p99 | GETs | CAS lost |
| ---- | -------------------------- | ---------- | ------------------ | --- | --- | ---- | -------- |
| pointer | 32 (5.1) | on | 5.1 | 823 ms | 4.7 s | 7.7 | 2.1 |
|         |          | off | 5.1 | 640 ms | 2.9 s | 3.5 | 1.5 |
|         | 64 (15.3) | on | 9.8 | 4,034 ms | 25.8 s | 26.6 | 10.8 |
|         |           | off | 13.6 | 2,859 ms | 21.3 s | 11.4 | 8.9 |
|         | 128 (51.6) | on | 11.2 | 6,243 ms | 27.8 s | 34.5 | 14.4 |
|         |            | off | 15.3 | 4,484 ms | 25.2 s | 15.2 | 12.4 |
| inline TM | 32 (5.1) | on | 5.1 | 1,565 ms | 13.3 s | 12.7 | 4.5 |
|           |          | off | 5.1 | 847 ms | 6.5 s | 4.6 | 2.6 |
|           | 64 (15.3) | on | 6.2 | 6,796 ms | 31.5 s | 31.9 | 13.6 |
|           |           | off | 7.8 | 5,113 ms | 29.3 s | 15.1 | 12.3 |
|           | 128 (51.6) | on | 6.1 | 9,980 ms | 39.7 s | 40.7 | 17.5 |
|           |            | off | 7.7 | 8,231 ms | 33.6 s | 19.2 | 16.0 |

- The GET is the whole difference: tail reads on took 2.2-2.5 GETs an
  attempt, off 1.1-1.4, as `e1-d20` (whole reads, before format 3: 1.1-1.5).
  It lengthens each attempt's window between its read and its CAS, so more
  attempts lose, and each lost one reads again.
- In E3 (10 tables, 8 workers, depths 6,000 s and 60,000 s) tail reads on
  took 1.2-1.7 GETs a merge append more, and its median was 7-24% longer
  (pointer at 6,000 s: 511 ms against 459).
- With tail reads off, pointer at 64 and 128 workers still committed below
  `e1-d20` (13.6 and 15.3 a second against 15.3 and 19.0), losing more races
  a commit at the same GETs an attempt; inline TM matched or passed it. Not
  explained yet.

So on a store without appends a tail read of a longer object reads it whole
at once (step 1 above: `TestAtomicFileLog`'s
`aLongerObjectOnAStoreWithoutAppendsIsReadWholeAtOnce`): as a whole read
where the object changed, and still a lookup and a 20-byte GET where it did
not. A whole read is always a read the model allows, so the protocol is
unchanged.

## Why a tail read is sound

The claim: a replica holds the first N bytes of an object of incarnation X,
and the last 20 of them are the trailer t of its last frame. If a tail read's
GET, pinned to a version V at least N long, finds t at N − 20, then V's first
N bytes are the replica's, and folding V's frames after N onto the replica
replays V ([Replaying a suffix](#replaying-a-suffix)). It rests on six
assumptions:

- **A1, the append condition.** An append lands on exactly the version it
  names, at that version's length, or nowhere
  (`appendPinnedToAVersionReplacedAtTheSameLengthFails`, every store).
- **A2, pinned reads** (step 0). A read of a version returns that version's
  bytes or fails (the contract's read cases, every store).
- **A3, fresh incarnations.** Each replace attempt draws its incarnation at
  random (64 bits), so no two objects share one.
- **A4, frames from reads.** `AtomicFileLog` frames an append only from the
  tail its condition names, and only if that tail has a view: bytes it read,
  pinned, from that version. No append from a probe (["A probe"](#reads)).
- **A5, no rewrites.** A version changes only by an append, which extends it,
  or by a replace, which starts a new incarnation.
- **A6, no forgery by accident.** The 20 bytes at N − 20 of an object of
  another incarnation are not t: they would have to hold X's 64 random bits
  at that offset.

**The invariant.** In every version V the store holds, every frame names
V's incarnation and its own offset; and of two versions with one
incarnation, the shorter is a prefix of the longer.

**By induction on the store's writes:**

- *A replace* writes a new object. Its frames name the incarnation it drew,
  by construction, and no earlier version has that incarnation (A3), so the
  prefix half holds trivially.
- *An append* to V writes V's bytes and a frame f. By A4 its writer read V,
  and by A2 what it read is V's bytes, whose frames name V's incarnation
  (the induction); it gives f that incarnation and V's length as f's offset.
  By A1 the append lands only on V, so the new version extends V, keeps its
  incarnation, and f names both truly. A transport retry of the append
  resends the same request under the same condition: it lands on V or
  nowhere.
- Nothing else changes a version (A5).

**The claim follows.** t names X. If V's incarnation is X, V and the version
the replica read share an incarnation, so the shorter is a prefix of the
longer (the invariant); both are at least N long, so their first N bytes
agree. If V's incarnation is not X, every frame of V names another
incarnation (the invariant), so V does not hold t at N − 20 (A6): the check
fails, and the reader reads V whole.

**Two writers through a replace.** X = C_k l_1 … l_n, of length a, read by
writers T_i and T_j, each preparing a record. A compaction replaces X with
Y = C_{k+1} l_{n+1} … l_m, of length b.

1. T_i sends l_i framed (X, a) under X's version at a. It fails: the object
   is no longer that version (A1). T_i tail-reads from (X, a): the bytes at
   a − 20 of Y are Y's, not l_n's trailer, so it reads Y whole, replays it,
   frames l_i (Y, b) and sends it under Y's version at b. It lands.
2. T_j sends l_j framed (X, a). It fails too, and so does its tail read from
   (X, a), for the same reason. So T_j reads Y whole, l_i included, and
   replays it: it knows whether l_i applied before it sends anything. It
   frames l_j (Y, b + |l_i|) and sends it under the version that holds l_i.
3. Had T_j read Y before l_i landed, its tail read from that point would have
   found its own trailer and returned l_i, and its fold would have replayed
   it.

A writer cannot land a record after one it has not read, because its
condition names the version it read (A1, A4). Whether l_i applied is
replay's verdict on those bytes, the same for every reader, and l_j is
judged on the state l_i left, applied or not. What the writer prepared
against does not enter: the proof is about which bytes are the log, and
replay decides what they do.

Today's loop breaks A4. Its resend after a probe frames from no read, which
is harmless only because format 2's frames name nothing. Step 4 states the
invariant in `CatalogCommit.tla` with incarnations, and its bug switches
(a check of the length only, no trailer check, a resend after a probe, which
drops A4) must each break it.

## Replaying a suffix

A tail read hands replay only the records after the replica's tail, never
the checkpoint they follow. That is already how most reads replay. Replay is
a deterministic left fold over the records (LOG.md "Replay"), so the state a
replica holds, folded with the records after it, is the state a replay of
the checkpoint and every record reaches. `LogRuntime` folds whenever a new
view continues the one a replica was replayed from (`continued`): every read
with a floor, and since D20 a commit's read through its own write. What the
checkpoint fixes (the floor, the committed set's horizon, the conflict
encoding, snapshot paging) is in the replica's state, and changes only by a
record (a GC record, a `PageOut`), which a fold applies as a replay does. In
the test suites `-Dintentlog.replay.check=true` checks every fold against a
replay of the whole view: the state's checkpoint bytes, the committed set's
bytes, the record count and the outcome.

So a suffix cannot be read differently from the whole; it can only be the
wrong suffix: records that do not follow the replica's tail (a gap or a
splice; an overlap applies once). Today `continued` rules that out by
comparing the bytes the replica holds with the new view's. A tail read does
not have them, and the trailer at N − 20 stands in: the object is still X,
whose bytes before N cannot change. Under the check flag a tail read is also
checked the old way: the test reads the whole object at the version the
tail read found and replays it, and the fold must match. That makes the
trailer's argument the thing the suites test.

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
(fileio-catalog's tests, and catalog-bench's `sut-fileio`) charged each
`newStream` the latency and the bytes of the whole object (`Requests`). Since
step 1 it charges a stream's GET when it first reads, by the bytes from the
position a seek left to the end, and a seek the open GET cannot read through
(backward, or forward past S3FileIO's 1 MiB skip size) starts another, as
`S3InputStream` does. catalog-bench's tiered FileIO already counts a GET per
stream and the bytes a stream returns (`IoStats`); its time is
`LatencyFileIO`'s.

**The HEAD.** Step 1 found that the model charges no HEAD for a whole read.
Opening a stream binds its file in the model (and in catalog-bench's
`ModelStore`, for every client), as a GET's response reports a version and a
length, so `AtomicFileLog`'s lookup after the open (`getLength`) costs nothing.
On S3, ADLS and GCS that read is a HEAD, then a GET: the lookup binds the
version the GET asks for, an order GCS needs, whose reads record no
generation. A tail read takes the same order (open, look up, read from
N − 20), so in the model it is one ranged GET, as a whole read is one whole
GET, and on the stores each is a HEAD and a GET: steps 5 and 6 compare like
with like. Step 7's question, whether the HEAD matters, is the one the model
cannot answer as it stands. Charging the lookup after an open moves every
FileIOCatalog number by a HEAD a read (on `s3x-2026-11` about 6 ms, on S3
Standard about 27 ms, two or more a commit), and the baselines' too where
Iceberg looks a file up after opening it (an Avro manifest list without a
length). That is a change to the benchmark's model for every client, so it
waits for step 6's numbers and a decision (errata D23).

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
- Done in step 0: the fork's reads pinned to the version the file has seen
  (S3's `If-Match`, across reopens; GCS's `if-generation-match`; ADLS's 412s
  mapped), `StaleReadException`, the contract's read cases, and
  `AtomicFileLog` reading again after a stale read.
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
- Under `intentlog.replay.check`, each tail read's fold checked against a
  replay of the whole object at the version the tail read found
  ([Replaying a suffix](#replaying-a-suffix)).
- `AtomicFileLog` unit tests: a replace longer than the tail, a replace of the
  same length, a splice at and inside a frame, a tail read with nothing new,
  a tail read whose GET finds the object shorter than its HEAD did.
  `SharedLogContract`'s `readAfter` case on `AtomicFileLog`.
- Gaps on a shared log (S4): a `MemoryLogHarness` mode that serves a
  checkpoint and records with a gap between them, or trims past its newest
  checkpoint, and a `SharedLogContract` case; every such read must fail.
- The contract's read cases on every store (real cloud), step 0's open
  item.
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

0. **Reads of one version** (built 2026-11-09: fork 5f14343338, fileio
   b4a9061). Every store's reads ask for the version their file has seen,
   a read of a superseded one fails as stale, and `AtomicFileLog` reads
   again; the contract suite has read cases. It closes T6's whole-read half
   on the store's word, at no cost, and turns a read that raced a replace
   into a 412 instead of a transfer that fails the format's checks later.
   The read cases passed on every store (fork 7a5d42de5e, `cloud-runs/`
   2026-11-09-contract-b): S3 Express, S3 Standard, ADLS, GCS and GCS Rapid,
   each case its store supports. S3 Express honours `If-Match` on a GET. A
   havoc round at fileio 69fdfdd (`sr0-r0`, 2026-11-10) was clean.
   - **0b. Probe the metadata alternative** (fork 18c87db437,
     `S3ExpressMetadataProbes`, `AdlsMetadataProbes`, in the same run): small
     cases, as `GcsRapidCanaries` probes Rapid buckets, of the three
     behaviours the alternative rests on. All three hold on S3 Express and
     ADLS ([Alternatives](#alternatives-considered)). With both possible,
     step 2 takes the trailer (decided 2026-11-10), which rests on no store
     behaviour.
1. **Pricing.** Ranged reads priced by their bytes in `LatencyFileIO` and
   catalog-bench. No behaviour changes; catalog-bench's numbers must not
   move. Built 2026-11-10 (fileio 1b3314e), and they did not: catalog-bench
   campaign `tr1-pricing` (E1 at 300 tables on `s3x-2026-11`, 32 workers,
   two runs each, 120 s measured) on fileio 69fdfdd and 1b3314e gave a
   merge append's p50 of 154.8 and 153.5 ms inline ML, 108.7 and 109.5 ms
   inline TM; 2.29 and 2.32 catalog GETs a commit (ML), 2.28 and 2.29 (TM);
   11.7 and 11.9 MiB read (ML), 6.04 and 6.08 (TM). Its HEADs are only the
   probes after lost appends: the model charges a whole read none
   ([Pricing](#pricing), "The HEAD").
2. **Format 3 and checked reads.** Trailers, the checks, a read again on a
   failed check, the explorer, the read modes in `InMemoryAtomicFileIO`, and
   fuzz rounds in each mode. This alone closes T6 and the one-object half of
   S5: whole reads detect a splice, whatever the store returns.
   Built 2026-11-10 (ATOMIC_FILE_LOG.md "Layout"). Until step 5 the commit
   loop still resends after a probe; the resend frames its record with the
   probed version's incarnation, which it reads from that version's last
   trailer (a pinned 20-byte GET), so A4 holds: every frame comes from a
   pinned read of the version its condition names. `InMemoryAtomicFileIO`
   keeps binding a stream as it opens by default (`OPEN`); the fuzzers draw
   `OPEN`, `PINNED`, `LIVE` or `SPLICE` per try (the fault fuzzer `OPEN` or
   `PINNED`, errata T6), and the commit-protocol
   fuzzer checks that no read returns a state the log never held (a mutant
   that skips the checks fails it within a few hundred tries).
3. **Positions in the SPI** (S4). The checkpoint's `through`, the joint's
   check, views of records that say where they start, and the gap-serving
   harness and contract case, on `InMemorySharedLog`. It closes S5's
   shared-log half for providers that pass the contract.
   Built 2026-11-10 (LOG.md "Positions"). `LogTail.position()` and
   `LogView.start()` are opaque bytes the runtime compares for equality;
   `LogCheckpoint.through` (log.proto field 5) is written only by
   `putCheckpoint`, since a replace's checkpoint and records are one write
   whose provider checks the joint (on one object, the trailers), so
   `AtomicFileLog`'s views say nothing of their start and its checkpoints
   are unchanged. `AtomicFileLog`'s tails have positions (incarnation and
   length) when the log read or wrote their bytes, for step 5's folds. The
   testkit's gap reads (`ReadFault.GAP`) and its oracle that every replica a
   read or commit returns is a state the log held (`LogHarness.held`) catch
   a runtime that skips the joint's check in seconds.
4. **The model.** Incarnations, splices and tail reads in
   `CatalogCommit.tla`, and gaps in `SharedLogCommit.tla`, with the
   switches, before the Java that relies on them (as for
   COMMITTED_PAGING.md's stage 2).
   Built 2026-11-10 (spec/tla/README.md "Tail reads"): every configuration
   passes with tail reads on, and the probe path (`probe-*`) too; the
   switches `lengthonly` and `notrailer` break the new invariant
   `NoUnheldState`, `probeinc` breaks `Readable` (which now asks that every
   frame name its object), and the shared log's `nojoint` breaks
   `Deterministic`. Bugs 26, 64a and `floorgap` reach their violations only
   through a resend after a probe, so their calibrations run with tail reads
   off.
5. **Tail reads.** `readAfter`, `read(floor)`, the commit loop, the switch,
   the mutants, havoc rounds.
   Built 2026-11-10. `AtomicFileLog.readAfter` is the tail read of
   [Reads](#reads), its view the replica's arrays and the new frames, which
   `LogRuntime` folds (`continued`). `LogRuntime.read(floor)` reads after the
   floor's tail, so every load and refresh through `CatalogLog` is a tail
   read once the catalog has read the object; the commit loop's reads (after
   a CAS conflict, an ambiguous write, a stale base, an exhausted loop) read
   after the replica it holds. After a lost append race the loop tail-reads
   and appends from the fresh replica, with no read back, on a log that
   replaces itself; on a shared log, whose reads may lag its appends, a read
   need not show the commit's own record (the memory fuzzer found a commit
   reported rejected that had applied), so there it still resends and its
   read through the resend settles it (P3). The joint's check (step 3)
   applies to logs that store checkpoints apart: a one-object log checks its
   own. The switch is `fileio.catalog.read.tail` (`RuntimeConfig.tailReads`);
   off, the loop probes and resends as in step 2. Under
   `intentlog.fileio.tail.check` (the suites) every tail read is compared
   with a whole read of the version it found, except in the fuzzers that
   preempt a commit at a request (the fault and multi-writer fuzzers, and
   the protocol fuzzer under reads that serve the live version, `LIVE` and
   `SPLICE`): the check's read is a request of its own, and under reads that
   do not pin a version it reads what the preempting commit wrote (round
   `tr5b-r0`'s multi-writer job failed so, on a replace; the protocol
   fuzzer at 0a00685, whose whole read of a longer object on a store
   without appends moved its preemption onto the check's read, on a
   replace of the same length). The fuzzers draw the switch
   (the protocol fuzzer from its seed, the fault fuzzer per writer, the
   others per run); the mutant set `fuzz/mutants/tailreads.sh` holds the
   model's switches and a read without checksums. Its first calibration on
   havoc (`tr5-calib`, 300,000 tries) failed unmutated: on a shared log a
   commit's read after a fold (whose replica keeps no view) of records it
   could not fold read the log whole without the commit's floor, and a read
   lagging behind the base rejected a commit whose resend had landed (the
   model's `lagread`, which `SharedLogCommit.tla` states for whole reads; it
   does not model reads after a tail on a shared log). Fixed, with
   `TestSharedLogRuntime.aWholeReadAfterAFoldReadsUpToTheCommitsFloor`.
   Calibrated again at the fix (`tr5b-calib`, 409bbad): the unmutated
   oracles clean, every mutant caught (`lengthonly`, `notrailer` and
   `probeinc` by the file commit-protocol fuzzer and `TestAtomicFileLog`,
   `nochecksum` by `TestAtomicFileLog` alone, `nojoint` by the memory
   commit-protocol fuzzer). The round at 409bbad (`tr5b-r0`) was clean but
   for the fault and multi-writer jobs, both the harnesses' (the fault
   fuzzer's model and the tail-read check under unpinned reads, above);
   rerun at their fixes, the fault jobs at 61ff771 (`tr5c-fault2`) and the
   multi-writer job at b6abb0c (`tr5d-mw`) were clean.
6. **Measure.** catalog-bench E1 at 300 and 1,000 tables on S3 Express, inline
   ML and TM, tail reads on and off; and E3's deep tables, whose sizes after
   paging campaign `e3f-sp4` gives.
   Run 2026-11-10 on havoc (catalog-bench campaign `tr6`: 5ca116a, fileio
   66cee32, whose FileIOCatalog paths 409bbad does not change; three runs a
   point, 300 s measured; `s3x-2026-11`). A merge append, tail reads off and
   on:

   | Point | Mode | p50 | p90 | p99 | catalog MiB read | catalog GETs | lost appends |
   | ----- | ---- | --- | --- | --- | ---------------- | ------------ | ------------ |
   | E1, 300 tables | inline ML | 265.6 → 74.9 ms | 508 → 365 | 1,136 → 929 | 12.5 → 1.55 | 2.81 → 2.22 | 0.42 → 0.17 |
   |                | inline TM | 136.8 → 94.2 ms | 278 → 188 | 592 → 673 | 6.26 → 0.80 | 2.64 → 2.25 | 0.32 → 0.23 |
   | E1, 1,000 tables | inline ML | 7,642 → 2,366 ms | 33,227 → 40,491 | 65,474 → 66,243 | 73.2 → 15.2 | 11.3 → 12.7 | 7.16 → 9.14 |
   |                  | inline TM | 2,439 → 594 ms | 10,481 → 12,015 | 23,478 → 26,191 | 33.1 → 7.27 | 8.28 → 9.07 | 4.23 → 5.88 |
   | E3, 1,000 snapshots | inline ML | 69.0 → 35.9 ms | 157 → 122 | 378 → 341 | 3.67 → 0.12 | 2.11 → 2.03 | 0.053 → 0.029 |
   |                     | inline TM | 79.1 → 66.6 ms | 131 → 112 | 247 → 229 | 1.50 → 0.05 | 2.15 → 2.06 | 0.075 → 0.058 |
   | E3, 7,200 snapshots | inline ML | 113.5 → 43.9 ms | 235 → 159 | 682 → 720 | 7.55 → 0.24 | 2.18 → 2.05 | 0.088 → 0.044 |
   |                     | inline TM | 117.0 → 74.1 ms | 211 → 156 | 431 → 411 | 4.85 → 0.16 | 2.20 → 2.07 | 0.100 → 0.063 |

   At 300 tables and in E3 every merge append committed in both arms. TM's
   p99 at 300 tables rose in every run (612-703 ms, against 549-631): in its
   slowest 1% a commit's catalog requests took less time with tail reads (129
   against 190 ms) but it lost more append races (1.64 against 1.38), though
   by requests a tail read after a lost race leaves the narrower window before
   the resend (a GET, against a probe's HEAD and its GET of the probed
   version's trailer; the model charges no lookup after an open, errata D23):
   the question 1,000 tables asks louder.
   At 1,000 tables (16.7 merge appends a second offered) the driver is the
   limit in both arms, as it was in campaign `e1` (CPU 66-94% of its 16, GC
   17-38%: `run_flags.saturated`), and with tail reads not every merge append
   committed: 105 of 4,003 inline ML and 42 of 13,582 inline TM failed
   (Iceberg's last try out of attempts, after 44-46 lost append races), against
   4 and 1 off. The commits that lost no compaction (CAS) race, 61-65% of them, do
   better with tail reads: a p50 of 564 against 2,356 ms (ML) and 276 against
   840 (TM), and fewer lost appends (3.1 against 4.5, 2.3 against 3.5). The
   rest lose about twice the append races with tail reads (20.8 against 10.7,
   12.9 against 5.4) and take more of Iceberg's tries (2.58 against 1.77, 1.98
   against 1.31), and every failure is among them. Why is open
   ([Open questions](#open-questions)). Nor is the off arm the catalog before
   format 3, whose resend after a probe read no trailer: at this point fileio
   e505a0f (campaign `e1`, 2026-11-02, the same configuration) committed
   inline TM at a p50 of 558 ms, 16.7 a second, with no failures, 1.35 lost
   appends and 0.18 lost CASes a commit, against 4.23 and 0.95 off here (2,439
   ms, 13.2 a second) and 5.88 and 0.54 on (594 ms, 15.0 a second); inline ML
   was as saturated then (8,528 ms, 3.9 a second).
   The first runs of this point overlapped about 24 fuzz jobs on havoc (the
   rounds of steps 0 and 5); rerun on a quieter havoc (`tr6q`), they gave the
   same picture, and the table gives the rerun (catalog-bench's lab marks the
   first runs superseded). The 300-table and E3 runs overlapped the rounds
   too, each point's arms side by side.
6b. **Compaction after the commit** (2026-11-10, before step 7: the user's
   call on the knee). Traced, then modelled, built and measured: [Compaction
   after the commit](#compaction-after-the-commit). At 300 tables a compaction
   after the commit lands, sent again with the records that beat it. At the
   1,000-table knee none lands and the store's cap bounds the log, yet it is
   the best arm there too (campaign `tr6m`).
7. **One request a read**: a ranged GET that
   reports the version and length it read (S3's `ETag` and `Content-Range`,
   ADLS's and GCS's equivalents), so a tail read is one request. A fork
   change to the input files. Planned if step 6 showed the HEAD, which it
   cannot: the model charges a lookup after an open nothing (errata D23), so
   building it waits on that decision, and on the open question of tail reads
   past the knee.

Then R2.3, with a read's cost known.

## Compaction after the commit

**What the traces showed.** Step 6's failures at E1's 1,000 tables were
traced (intentlog's `CommitTrace`, catalog-bench's `trace_commits`; campaign
`tr6t`, one run an arm). With tail reads every writer learns the log's exact
record count, so at 100 records they all turn to compaction together: about
55 writers encoded a checkpoint for each compaction that landed (inline TM:
2,549 compactions lost, 46 landed; ML: 771 and 14), and every one that lost
lost to another writer's compaction. While they compacted, nobody appended;
then everybody did, and appends in the burst after a compaction lost 91-94%
of their races, against 59-71% otherwise. With tail reads off, writers learn
the count late and keep appending, and compactions lose to appends instead
(TM: 31 of about 4,073 landed; ML: 10 of 1,022). Either way a compaction
cannot beat concurrent appends: its checkpoint (8.5 MiB TM, 16.6 MiB ML at
1,000 tables) takes about 2 s (TM) or 5.5 s (ML) to encode (550-650 ms of CPU
on a saturated driver) and 0.5-1.8 s to PUT, while appends land about every
65 ms at 15 a second. A commit that failed spent its ten attempts losing
appends as the count climbed, one compaction of 2-6 s that lost, and more lost
appends in the burst after; Iceberg retried it five times, each the same way.
A resend after a tail read was not slower than one after a probe (6-13 ms of
think time against a probe's extra request): the herd was the cause, not the
read.

**The design.** On one object the commit no longer compacts:

- Past the policy's limits a commit still appends. Once its record has landed
  in a replica `R`, its writer may compact: it replaces `R`'s version with a
  checkpoint of `R`, a GC record and PageOuts, and no record of its own. The
  replace is conditioned on `R`'s version, so it lands only if no write landed
  since, and then holds exactly the state the log held (I11). One that loses
  its race, fails or stays throttled is dropped; the commit stands whatever
  becomes of it, as a compaction after a commit already did on a shared log
  (LOG.md "Compaction").
- A writer compacts once the log's progress past the limits (the larger of
  its count's and size's fractions) reaches a point it draws in [1, 2) of the
  limits, so that writers do not all compact at the same record. After each
  compaction it tried, its next point is the progress then plus a draw in
  [0, 1), doubled for each loss in a row: a loss says the log is busier than
  one compaction's encode and PUT, and writers that keep it busy try ever
  more seldom. A log compacted since (its progress fell) resets both.
- A commit still replaces the object with its record when no append can
  carry it: the store refused an append (its cap on writes to one object:
  10,000 on S3 Express, 50,000 for an Azure append blob), it has no append,
  the record is larger than an append stores, or the policy is due even on a
  log with no records (CAS only). Then nobody compacts after it.
- `fileio.catalog.max.append.count` goes from 100 to 1,000 (provisional,
  errata D14): a reader that folds pays for a record once, and a record is
  about 200-230 bytes, so 1,000 add 0.2 MB to a checkpoint of 8.5-16.6 MB;
  the count bounds a cold reader's whole read and how often every writer
  decodes a new checkpoint.
- `fileio.catalog.compact.after.commit` (`RuntimeConfig.compactAfterCommit`,
  default true) puts back the fused compaction, for ablation.

**Its limit.** A compaction lands only between appends. Under load that
keeps every gap shorter than one compaction's encode and PUT, none lands,
and the log grows until the store refuses an append; then every writer's
commit replaces the object with its record, one wins, and the rest append
to its short log: the herd once a cap, not once a hundred records. A small
checkpoint is the structural fix: paged checkpoints
([CHECKPOINT_PLAN.md](CHECKPOINT_PLAN.md)) would make a compaction short
enough to land between appends. Until catalog-bench f5b9237 the bench
modelled no cap on writes to one object, so its runs showed the growth and not
the cap; it now refuses an append past the store's (`StoreProfile.maxAppends`).

**Resending a compaction that lost to appends.** The first measurement
(campaign `tr6k`, E1 at 300 tables on `s3x-2026-11`, one run an arm) found
compactions after the commit seldom landing at about 5 commits a second, far
below the knee. In inline ML, 9 of 126 landed at a count of 100 (7%), and none
of 33 at 1,000. A compaction's window was mostly its encode (a median of
500 ms of the 600 ms a lost one took at a count of 100, 750 of 900 at 1,000),
against an append every 200 ms. A writer whose compaction lost dropped it, and
its next point was twice as far, so a log that fell behind stayed behind: the
1,000 arm ran its whole measurement 31,000-32,300 records past its checkpoint.
That was its pack's: setup clients compacted by size alone, so aged packs ended
far past their checkpoints, and the fused arms replaced them at their first
commit. A fused commit's replace gets through because the commit retries it
until it lands. So now a compaction that lost to appends is sent again:

- Its writer reads the log after the version it lost on (a tail read). If the
  object is that version, grown, it sends the checkpoint it already encoded,
  conditioned on the version it read, with the records that beat it copied in
  front of its GC record and PageOuts: `[checkpoint][records that beat
  it][gc][page-outs]`. They replay on its checkpoint as they did on the version
  it checkpointed, and its GC and PageOuts follow them; a PageOut whose block
  they changed is refused, and its page swept later. A resend costs a tail read
  and a PUT, not an encode.
- It is sent again if, at the rate the appends that beat it landed over its
  window, less than one is expected while it is read and sent again, so that it
  lands with probability above 1/e (`CompactionPolicy.resend`); at most 4 times
  (`LogRuntime.MAX_COMPACTION_RESENDS`). At 300 tables (a PUT of about 90 ms,
  3 appends in a 600 ms window) that says yes; at the 1,000-table knee (a PUT
  of 0.5-1.8 s, an append every 65 ms) it says no, and the limit above stands.
- One that lost to a replace (another writer's compaction, or a commit that
  replaced the object) is dropped, as before.

And the bench's setup clients now compact every 1,000 records (fused, whatever
the arm), so a pack ends fewer than 1,000 records past its checkpoint, as a
catalog compacting at the default count would.

**The model.** `CatalogCommit.tla` (written first): `CompactAfter`; past the
limits `MustCAS` holds only when the policy is due on an empty log
(`DueEmpty`); a commit judged committed (`Judge`'s `Committed`, on any path:
read back, rebase, tail read, resolve, final read) may move to `compact`
(`CompactDue`; the model takes both choices, as any draws give); `Compact` is
one conditional replace of `R`'s version, `[chk][gc][page-outs]`, with every
write fault, after `PrepCas` put its pages with Paging; the commit ends
committed whatever it does. A compaction is not one of the commit's writes
for the fuzzers' oracles, and no racer runs before it in the race schedules
(the race fuzzer can tell: below). Its switches each break a property:
`compactbase` (a checkpoint of the replica the commit staged on, conditioned
on the version its record landed in: `HonestCommitted`), `compactresend`
(one that lost resent on the object as it is, as if the records that beat it
were in its checkpoint: `HistGrows`) and `compactfail` (a loss fails the
commit: `HonestFailed`). The resend is `CompactRead` (`CompactResends`
times, 1 in every `after-*` configuration and 2 in the `after-resend*`
ones): after a loss a tail read, and if the object grew, `compact` again,
conditioned on the version read, with the records after the lost version
(`cp`) in front of its payload's. `CheckpointIsReplay` now says that a CAS's
checkpoint is the replay of a prefix of the version it replaces and its log
begins with the rest (the prefix whole, for every CAS but a resend). Its
switches: `compactcopyafter` (the records after the GC and PageOuts:
`CheckpointIsReplay`) and `compactreplaced` (a replace as long as the lost
version taken for growth: `HonestCommitted`). Its matrix found the
multi-writer oracle comparing a compaction's state too coarsely: a
`compactbase` resend dropped a record, which let one rejected before apply in
its place, to the same entity versions. The model's oracle now compares the
replayed state whole, and TestMultiWriterProperties each table's whole
metadata (it compared locations and a few properties, blind to an inline
table's snapshots). On one object the digest check of a PageOut now matters:
a resend's PageOuts were computed before the records it copies
(`after-nodigest` breaks `PagesExact`). Writing the Java found one rule the model then took:
CAS only, every commit replaced and then compacted again (`DueEmpty`). The
existing configurations keep the fused compaction (their states as before);
the `after-*` family runs the same kinds of schedule with compaction after
the commit, and every one passes, up to 9.7 million states (`after-skew`;
spec/tla/README.md "Compaction after the commit"). The oracle matrix found
the multi-writer oracle blind to `compactbase` and `compactresend`: it judged
a commit by its own writes, and the damage is in the compaction's. It now
checks each compaction apart (it changes no state), in the model and in
TestMultiWriterProperties, and catches all three.

**The code.** `LogRuntime.commit` calls `compactAfter(landed)` on a log that
replaces itself; `prepare(on, record, ownMs)` builds a commit's replace and a
compaction's (no record, nothing kept below); `drawn`/`tried` keep a
runtime's point; `CompactionPolicy.progress`, `draw` and `withDraws`;
`RuntimeConfig.compactAfterCommit`; `LogCatalogProperties.COMPACT_AFTER_COMMIT`;
`CommitTrace.Kind.COMPACT`. The resend: `compactAfter`'s loop, `grown` (the
tail read, or a whole read that continues the lost version byte for byte),
`CompactionPolicy.resend` (`withDraws` draws it too, for tests). Tests:
`TestLogRuntime` (a commit past the limits appends and its writer compacts; a
compaction that loses is dropped, or sent again with the record that beat it;
one that loses to a replace is dropped; at most 4 resends; the rule; the
drawn points and the backoff; fused; CAS only; a traced compaction),
`TestProtoCommitKnobs`.

**The fuzzers** draw it: the protocol fuzzer from its seed (and its points
from the seed), the fault fuzzer per writer (each writer's points fixed:
0, 0.4 and 0.8 of the limits past them), the race fuzzer and the multi-writer
fuzzer per run. The race fuzzer on one object counts A's writes from its
trace, which tells a compaction after A's commit from A's commit's writes; the
multi-writer fuzzer checks a compaction apart from the commit before it (it
changes no state, and its checkpoint is a replay). `CatalogTestSupport`'s logs
and `LocalCatalogMode`'s catalogs compact at the limits themselves, so suites
repeat. The mutant set `fuzz/mutants/compaction.sh` holds the model's
switches (`compactresend` as a compaction conditioned on a probe of the
object); the one-object protocol fuzzer catches each in 5,000 tries, and the
multi-writer fuzzer in 300. The resend added three: `compactnocopy` (sent again
without the records that beat it), `compactcopyafter` and `compactreplaced`
(the model's). The protocol fuzzer reaches resends only where a commit's
compaction loses to an append: its preempting commits now run on a runtime of
their own (another process, with its own drawn point; on the writers' shared
runtime a preempting commit compacted too, so compactions lost to
compactions), and runs that compact after the commit also preempt the first
request after a commit's first write (its compaction's replace, on one
object). Its statistics count compactions sent, sent again, lost and landed,
from the runtime's trace. Its checkpoint oracle on one object
(`AtomicFileLogHarness`) says what the model's `CheckpointIsReplay` says: a
replace's checkpoint is the replay of a prefix of the version it replaced,
and the replace's records begin with the rest of that version's. The first
calibration (`tr6c-calib`) found the oracle still asking for the whole
version. Its unmutated run failed at about try 51,650 of 300,000, on a resend
that copied a record that had applied: the fuzzer reaches one about once in
50,000 tries. Round `tr6c-r0` found the catalog's oracles making the same
assumption. The multi-writer fuzzer took a compaction to be a replace with no
intention record, so it counted a resend, which holds the records it copied,
as a second write of the commit before it ("one commit made 2 conditional
writes"). It now takes a replace whose intention records are the last ones of
the version it replaced for a compaction. `CheckpointRoundTrip`, which the
multi-writer and fault fuzzers run on every write, compared a replace's
checkpoint with a replay of the whole version it replaced, and took the
first intention record after it for the replace's own. It now recognises a
resend (`copied`: the replace's records begin with the version's last ones,
and its checkpoint is the replay of the rest), and asks neither of it.
Calibrated again with both fixes (`tr6f-calib`), the protocol fuzzer caught
all six mutants. A calibration's build stops at the first module that fails,
so it runs the multi-writer fuzzer only on mutants the protocol fuzzer
misses. Run alone on each (3,000 tries, on the dev box), the multi-writer
fuzzer catches `compactbase`, `compactprobe` and `compactfail`, and none of
the resend's three. It reaches a resend about 18 times in 300,000 tries, each
after one record that replay rejected, so a resend that drops that record
(`compactnocopy`) changes nothing it sees. Over the 300,000 tries of a seed
that reaches them, it catches `compactcopyafter` (at try 87,001: the copied
record, after the GC record, is below its floor) and not `compactnocopy`.
Resends that copy a record that applied are the protocol fuzzer's.

Two tests changed meaning: `CheckpointRoundTrip`
asked of every catalog file that the first record after its checkpoint apply
where it holds (Bug 64: a replace's own record), which after a compaction is
some later append, which a compaction's GC record at a retention of 0 may
pass, as on a shared log; it now asks it of a replace's file. And Bug 66's
race to the store's cap now sets the count past the cap, so that the cap,
not a compaction after the racer's commit, makes the commit replace.

**Measured** (catalog-bench campaign `tr6m`, 2026-11-11: catalog-bench
bdd280c, fileio 3765a17; `s3x-2026-11` with its cap of 10,000 appends; packs
fewer than 1,000 records past a checkpoint; havoc, with the 300- and
1,000-table runs side by side and a load of 16-33 from other users). E1 with
tail reads, merge appends, fused or after the commit at a count of 100 or
1,000. At 300 tables (5 a second offered), two runs an arm of 900 s, every
merge append committed:

| Mode | Arm | p50 | p90 | p99 | compactions landed (after a resend) / begun |
| ---- | --- | --- | --- | --- | -------------------------------------------- |
| inline ML | fused, 100 | 75.4 ms | 358 | 994 | |
|           | fused, 1,000 | 51.8 ms | 133 | 606 | |
|           | after, 100 | 69.5 ms | 323 | 706 | 77 (65) / 124 |
|           | after, 1,000 | 44.9 ms | 122 | 413 | 10 (10) / 14 |
| inline TM | fused, 100 | 89.5 ms | 181 | 540 | |
|           | fused, 1,000 | 69.5 ms | 146 | 365 | |
|           | after, 100 | 88.6 ms | 172 | 452 | 78 (52) / 116 |
|           | after, 1,000 | 67.2 ms | 140 | 285 | 10 (10) / 11 |

At 1,000 tables (16.7 a second offered), one run an arm of 1,200 s:

| Mode | Arm | p50 | p99 | committed a second | failed | compactions landed / begun | records past the checkpoint (median, max) |
| ---- | --- | --- | --- | ------------------ | ------ | -------------------------- | ----------------------------------------- |
| inline ML | fused, 100 | 1,767 ms | 102.6 s | 3.05 | 108 of 3,762 | | 46, 100 |
|           | fused, 1,000 | 918 ms | 76.9 s | 8.91 | 67 of 10,763 | | 502, 1,000 |
|           | after, 100 | 1,282 ms | 24.3 s | 14.57 | 63 of 17,550 | 0 / 752 | 4,552, 9,999 |
|           | after, 1,000 | 1,460 ms | 25.1 s | 13.89 | 56 of 16,718 | 0 / 406 | 4,955, 10,000 |
| inline TM | fused, 100 | 272 ms | 5.0 s | 16.66 | 0 of 19,995 | | 53, 100 |
|           | fused, 1,000 | 410 ms | 30.5 s | 16.32 | 20 of 19,608 | | 485, 1,000 |
|           | after, 100 | 179 ms | 9.4 s | 16.66 | 1 of 19,995 | 0 / 914 | 4,849, 10,000 |
|           | after, 1,000 | 172 ms | 3.2 s | 16.67 | 0 of 19,998 | 2 / 608 | 2,932, 10,007 |

- At 300 tables the resend is what lands compactions: at a count of 1,000,
  20 of 25 landed, every one sent again, against none of 33
  without it (`tr6k`, whose packs started 31,000 records behind), and the
  logs stayed within 1,102 records of a checkpoint. After the commit at
  1,000 is the best arm in both modes at every percentile: its p99 is 32%
  (ML) and 22% (TM) below the fused arm's at 1,000.
- At 1,000 tables, as [its limit](#compaction-after-the-commit) says, a
  compaction after the commit does not land: 2 of 2,680 begun in the four
  runs. The resend's rule declined almost every resend (1.0 sends a
  compaction). The logs grew to the store's cap, and there commits replaced
  the object (7-80 a run, one or two landing) and the log began again.
  Even so it is the best arm. Inline TM committed every merge append at the
  offered rate with a p99 of 3.2 s, against 20 failures and 30.5 s fused at
  1,000. Inline ML, bound by the driver's CPU in every arm (65-94% busy,
  `run_flags.saturated`) and losing 7-8 appends a merge append in each,
  committed 13.9-14.6 a second against 8.9 fused at 1,000. Writers that
  hold a replica read only the log's tail (0.09-0.12 MiB a merge append).
- The count: 1,000 beat 100 at 300 tables in every arm, and in E3 (`tr6k`,
  10 tables at depths 1,000 and 7,200: the p99 28-74% lower, fused or
  after). At 1,000 tables, fused ML committed 8.9 a second at
  1,000 against 3.05 at 100 (`tr6k`: 9.65 against 3.27). Fused TM at the
  knee is noise at one run an arm: `tr6k` failed 15 at 100 and 2 at 1,000,
  `tr6m` 0 and 20. The default stays 1,000 (still provisional, errata D14).
- What the knee still costs: a log at the cap makes a cold reader read up
  to 10,000 records whole, and every writer still encodes compactions that
  cannot land. A writer could skip them by the resend's rule, estimating
  the window from its last compaction (errata D14). Paged checkpoints
  remain the structural fix.

## Alternatives considered

- **Pinned reads alone** (step 0, without format 3). They stop a whole
  read splicing, but not a tail read misreading a replace that grew
  (above), and they are each store's guarantee rather than the format's.
- **The incarnation in the object's metadata** (S3's `x-amz-meta-*`, ADLS's
  metadata), which a replace sets, an append keeps, and a lookup and a GET
  return. With pinned reads, a tail read would be one ranged GET from N
  whose response names the version, the length and the incarnation (the
  headers), with no change to the format, and a probe would learn the
  incarnation, so the commit loop could resend after one. Each check would
  rest on three behaviours of each store, which step 0b found on S3 Express
  and ADLS (below), and which a store could change: the probes would run
  with the contract, as canaries. The trailer rests on none, is tested by
  the fuzzers' read modes alone, catches a store that breaks its read
  contract (S5), and leaves the bytes self-describing for tools (the format
  explorer) and copies. Metadata is the smaller change, for a deployment
  that prefers a store's word; not chosen (2026-11-10), and the probes stay
  as a record of what the stores do.

  It would fit `SupportsAtomicOperations` as one more thing a file reports:
  an incarnation that a CAS draws and sets with the object it writes, that
  an append keeps, and that a lookup and each GET report for the version
  they found (a fork interface beside `InputFile`). GCS needs none: the
  fork's GCS writes are replaces only (Rapid buckets too: nothing appends to
  a catalog's live object, `GcsRapidCanaries`), so a tail read never
  applies. Step 0b's probes, on S3 Express and ADLS, all passed
  (2026-11-09):

  - A replace sets the metadata with its bytes: S3's conditional
    `PutObject`, and on ADLS the staged Append Blob's metadata, which the
    rename that replaces the live object carries onto it.
  - An append keeps it: S3 Express's `PutObject` at a write offset, ADLS's
    `AppendBlock`. Neither can change it: ADLS's `AppendBlock` takes no
    metadata, and S3 Express refuses an append that names some (400
    `InvalidArgument`, "Cannot specify both a write offset value and
    user-defined object metadata for existing objects").
  - A ranged read reports it, with the version and the object's length
    (`Content-Range`), for the version that served it, in one response; a
    read pinned to a version an append or a replace superseded gets a 412.
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
- **S3 Express and `If-Match` on a GET**: answered by step 0's run; directory
  buckets honour it.
- **Tail reads past the commit knee** (step 6, E1 at 1,000 tables): answered
  by the traces. Writers that learned the exact count compacted together, and
  lost to each other ([Compaction after the
  commit](#compaction-after-the-commit)). Still open: the probe path (tail
  reads off) is slower there than the catalog before format 3, its resend's
  trailer read or another change since fileio e505a0f; and whether a writer
  should estimate a compaction's chance to land (its log's growth rate
  against its last compaction's time) and skip one that cannot (errata D14).
