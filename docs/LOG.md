# The intention log

intentlog replicates a state machine through a shared log. A writer reads the
log (a checkpoint and the records after it, replayed), stages an intention on
what it read, and appends the intention as one record. Replay applies the
record where it lands if the intention's preconditions still hold there, and
skips it if they do not. Every reader replays the same records in the same
order, so every reader reaches the same state.

The catalog is one state machine (`CatalogStateMachine`, specified in
[SPEC.md](SPEC.md)). The log it runs on is one provider of a small SPI
(`SharedLog`):

- one object in a store with conditional writes (`AtomicFileLog`, laid out as
  [ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md) describes), whose commits append to
  the object or replace it with a checkpoint;
- a shared log whose checkpoints are stored apart from its records
  (`InMemorySharedLog`). Kafka, SQS and ledgers map onto the SPI on paper
  ([Providers](#providers)).

This document specifies what every state machine and every provider share:
the SPIs and their obligations, the envelope of checkpoints and records,
replay, the committed set and its GC, the commit loop, and compaction.

| Module | Holds |
|---|---|
| `intentlog-core` | The SPIs (`StateMachine`, `Intention`, `SharedLog`), replay (`Replay`), the committed set (`CommittedSet`), the commit loop (`LogRuntime`), and the envelope (`src/main/proto/log.proto`, encoded by `org.intentlog.wire`). JDK only: the enforcer bans Iceberg, Hadoop and Guava |
| `intentlog-fileio` | `AtomicFileLog`, over the Iceberg fork's `SupportsAtomicOperations` |
| `intentlog-memory` | `InMemorySharedLog`, with fault, lag and delay hooks |
| `intentlog-testkit` | `SharedLogContract`, `EntityVersionsMachine`, `LogHarness` and the `CommitProtocol` fuzzer, for providers' tests |

## State machines

```java
interface StateMachine<S, B extends StateMachine.Builder<S>, I extends Intention<B>> {
  B empty();                              // the state of a log with no checkpoint
  B decodeCheckpoint(byte[] state);
  byte[] encodeCheckpoint(S state);
  I decodeIntention(byte[] intention);
  byte[] encodeIntention(I intention);
  default B toBuilder(S state) { return decodeCheckpoint(encodeCheckpoint(state)); }
  interface Builder<S> { S build(); }
}

interface Intention<B> {
  boolean verify(B state);                // its preconditions hold on the state before it
  void apply(B state);                    // called only where verify holds
  default String explain(B state) {...}   // why verify fails there (or null): only on a rejection
}
```

`S` is the immutable state a read returns, and `B` the builder replay
mutates. The runtime owns what every state machine needs: the envelope,
record ids, the committed set that applies each record once, GC of that set,
and the commit protocol. A state machine owns its state and the bytes of its
checkpoints and intentions, which the runtime treats as opaque. Anything the
state machine resolves at replay, such as the catalog's late-bound ids
([SPEC.md](SPEC.md) §"Late-Binding"), stays inside it.

A state machine must meet four obligations, on which the commit protocol's
honesty rests:

| | Obligation | Why |
|---|---|---|
| S1 | `verify` and `apply` are deterministic, whatever the clock or the reader | every reader reaches the same state |
| S2 | `verify` is stable: once false at a point of a replay, it is false at every later point | a record that lost a race never applies later, so a writer that reads past it knows its outcome for good, even with a copy of it still in flight |
| S3 | `apply` bumps the version of every entity it changes | S2 rests on it: an intention names the versions it read |
| S4 | a checkpoint round-trips: `decodeCheckpoint(encodeCheckpoint(s)).build()` equals `s` | a reader that starts from a checkpoint reaches the state its writer replayed |

Two state machines implement the SPI:

- `CatalogStateMachine`: the catalog. Its state is `CatalogState`, its
  intention `CatalogIntention` (a transaction's actions), and its payloads
  catalog.proto's `Checkpoint` and `Intention` ([SPEC.md](SPEC.md)).
  `CatalogIntention.verify` rejects a create whose id is not late-bound, so
  that its rejections are stable (S2) for every record, crafted ones included.
- `EntityVersionsMachine` (intentlog-testkit): entities with versions, and
  transactions that read some at versions and write some, the state of the
  TLA+ models. Each transaction carries a tag, and the state keeps the tags it
  applied, in order, so a fuzzer reads from the state alone what applied.

## The log SPI

```java
interface SharedLog {
  Capabilities capabilities();
  LogView read();                                     // the latest checkpoint and the records after it
  LogView readThrough(Receipt receipt, Duration wait);   // a view whose records include receipt's (P3)
  LogTail probe();                                    // the tail, without the records
  Receipt append(byte[] record, LogTail expected);    // expected == null: unconditional
  default Receipt replace(LogTail expected, byte[] checkpoint, List<byte[]> records);  // fused
  default void putCheckpoint(LogTail through, byte[] checkpoint);  // stored apart; idempotent
  default void trim(LogTail through);                 // drops what a durable checkpoint covers
  default LogView readAfter(LogTail since);           // the records since a tail; by default read()
}

record Capabilities(boolean append, boolean conditionalAppend, boolean fusedReplace,
                    boolean separateCheckpoints, long maxRecordBytes) {}

interface LogView { LogTail tail(); Optional<byte[]> checkpoint(); List<byte[]> records();
                   default byte[] start(); }       // the position the records follow
interface LogTail { boolean exists(); long bytes(); boolean continues(LogTail earlier);
                   default byte[] position(); }    // the provider's encoding; opaque
interface Receipt {}                                  // what readThrough reads through
```

Records and checkpoints are opaque bytes to the provider (log.proto's
envelopes, below); it stores and orders them. A `LogTail` is the end of the
log as a read or probe saw it, and the condition a conditional write names:
`exists` is false before the log is created, `bytes` is what the size
threshold compares, and `continues(earlier)` says that the log only grew since
`earlier` (on one object, it was not replaced). `maxRecordBytes` is the
largest record one append stores (0: no limit): the commit loop writes a
larger record in a replace, on a log that replaces itself, and rejects it
before sending it (`TOO_LARGE`) on one that does not (Bug 71). A `Receipt` is
a provider's acknowledgement of a write: the version written, a log position,
an offset or a sequence number.

A log is created by its first write: a `replace` at a tail that does not
exist, on a log that replaces itself; or a `putCheckpoint` at such a tail, on
one that stores checkpoints apart, which fails (`LogConflictException`) if the
log exists with another first checkpoint. `readAfter(since)` returns the
records after `since` as a view without a checkpoint, for `refresh` to fold
onto the replica read at `since`, or a whole view to replay afresh; it may
throw `TrimmedException`.

**Positions** ([TAIL_READS.md](TAIL_READS.md) "Positions", 2026-11-10). A
read is right only if each record follows the one before it, starting where
its checkpoint ends (or, for records alone, at the tail they continue). A log
says so in positions, opaque bytes the runtime compares only for equality:
`LogTail.position()` (a sequencer's offset; one object's incarnation and
length; null if the provider cannot say, a tail a probe found on one object),
`LogView.start()`, the position the view's records follow as the provider
found them, and `LogCheckpoint.through` (log.proto), the position a
checkpoint covers, which the runtime writes from the tail it checkpoints at
(`putCheckpoint`; a replace, which writes the checkpoint and its records in one
write, names none, and its provider checks the joint: one object's trailers).
The runtime fails a whole view whose checkpoint covers one position and whose
records follow another, and folds a view of records alone only onto the tail
they follow (`LogGapException`): a gap (a log that trimmed past its checkpoint,
or lost records) or a splice would replay a state the log never held. Each
provider checks its own chain by its own rule: dense positions for a
sequencer, the trailers for one object. That a provider's positions are true
is its own word: one whose records' positions lie passes the runtime's check.

Any log's reads may lag its appends (P1), and its capabilities do not say
whether they do. A writer reads its own write through its receipt (P3), and a
commit keeps its reads from falling behind the one it staged on (the floor, in
the commit loop below). A reader that must not see the log go back, such as a
catalog that has just committed, passes the newest replica it has seen to
`LogRuntime.read(floor)`, which serves that replica when a fresh read lags
behind it; `refresh(prior)` never returns one behind `prior`. On a log that
replaces itself, a tail that does not continue an earlier one is a replace
since, which is newer, so the floor applies only to a log that cannot
(Bug 72). `CatalogLog` keeps the newest replica its catalog has read or
committed in, so Iceberg's refresh of a table after committing to it sees the
commit.

### Obligations

| | Obligation |
|---|---|
| P1 | One total order of records; every read returns a checkpoint and a prefix of the records after it, and says truly where its records start (`LogView.start`), so that the runtime can refuse a gap or a splice |
| P2 | An acknowledged write (append, replace, checkpoint) is durable |
| P3 | `readThrough(r)` returns a view whose records, after its checkpoint, include `r`'s record, or fails. A view whose checkpoint already holds the record cannot say whether it applied once GC drops its id |
| P4 | Of several conditional writes on the same tail, at most one lands |
| P5 | No trim before a durable checkpoint covers the records trimmed |
| P6 | `LogFullException` and `LogNotSentException` mean nothing landed |

P1 allows a read to return any prefix: a replica that lags behind the log's
appends is a legal provider. It does not allow one with a hole: a provider
need not prevent one (Kafka's retention trims whatever the checkpoints say),
but must report its records' positions truly, and the runtime refuses the read
(S4). P4 binds only a log with conditional writes. `SharedLogContract`
(intentlog-testkit) checks P1 (positions included), P3, P4 and P5 (and I11,
below) as far as a provider's capabilities allow.

### Failures

| Signal | From | Meaning | The commit loop |
|---|---|---|---|
| `LogConflictException` | a conditional write | the tail is not the one expected. No proof that the write did not land: a transport retry of a write that landed fails its condition | reads, unless only the tail is needed (a lost append race) |
| `LogThrottledException` | a write | backpressure (a 429, a 5xx, a per-object update rate). The write may have landed even so, and sending it again is safe | sends it again after a backoff, up to 8 sends; then the write is ambiguous, and a fresh read settles it |
| `LogFullException` | an append | the log holds as many writes as its store allows (S3 Express: 10,000 parts; an Azure Append Blob: 50,000 blocks). Nothing landed, and no append can | replaces, on a fresh read (Bug 66) |
| `LogNotSentException` | a write | preparing it failed: nothing was sent | rejects (`NOT_SENT`), unless an earlier write of the commit may have landed |
| `TrimmedException` | `readThrough`, `readAfter` | the records asked for are gone | `readThrough`: the outcome is unknown, and a fresh read settles it; `readAfter`: a fresh read |
| `LogGapException` | the runtime, on a view | the records do not start where the checkpoint ends, or do not follow the tail a fold needs: a gap or a splice | the read fails (the log is unreadable; unknown if a write may have landed) |
| `LogLagException` | `readThrough` after its wait; a commit's read that stays behind the commit's floor | a replica of the log lags its appends | `readThrough`: the write is ambiguous, and a fresh read settles it; a lagging read: the log is unreadable |
| any other exception | a write | the outcome is unknown | a fresh read settles it |
| any other exception | a read or probe | the log could not be read | unknown if a write may have landed; else rethrown |

## The envelope

The checkpoint and each record are log.proto's messages
(`intentlog-core/src/main/proto/log.proto`, package `intentlog`). They carry
what the runtime needs, whatever state machine the log replicates: the log's
identity, each record's id, and the committed set that dedups them. The state
machine's bytes ride in `state` and `intention`.

```protobuf
message LogCheckpoint {
  int32  min_reader_version         = 1;   // LogEnvelope.SUPPORTED_READER_VERSION, now 2; required, first
  bytes  log_id                     = 2;   // UUIDv7, 16 bytes, minted when the log is created; required
  CommittedTransactions committed   = 3;   // absent when the set is empty with no horizon
  bytes  state                      = 4;   // the state machine's checkpoint; required
}

message LogRecord {
  bytes id = 1;                            // UUIDv7, 16 bytes; required with an intention
  oneof body {
    bytes          intention       = 2;    // the state machine's intention
    AdvanceHorizon advance_horizon = 3;    // a GC of the committed set; has no id
    PageOut        page_out        = 4;    // seals the oldest block into a page; has no id
  }
}

message AdvanceHorizon {
  fixed64 watermark_ms = 1;
}

message PageOut {                          // COMMITTED_PAGING.md "Paging out: the log record"
  uint32 level  = 1;                       // the oldest block's node
  uint64 index  = 2;
  uint32 count  = 3;                       // its ids
  bytes  digest = 4;                       // SHA-256 of its CommittedBlock message, 16 bytes
}

message CommittedTransactions {
  reserved 1, 2;                               // the one-blob set of reader version 1
  // GC horizon: the largest timestamp of any id a GC record dropped.
  // 0 (absent) until the first drop; ids at or below it are UNKNOWN.
  fixed64 highest_dropped_timestamp_ms = 3;
  // The floor: replay admits no id at or below it. Absent when it equals the
  // horizon; a PageOut raises it (COMMITTED_PAGING.md).
  fixed64 floor_ms                     = 4;
  repeated CommittedPageRef pages      = 5;   // the pages of the ids in (horizon, floor], oldest first
  repeated CommittedBlock blocks       = 6;   // the ids above the floor, oldest first
}

message CommittedPageRef {     // a sealed block: its page is named <level>-<index>-<digest hex>
  uint32  level            = 1;
  uint64  index            = 2;
  uint32  count            = 3;
  fixed64 max_timestamp_ms = 4;   // its newest id: the horizon once GC drops the page
  bytes   digest           = 5;
}

message CommittedPage {        // a page's body: an object apart from the log, never changed
  int32  min_reader_version = 1;
  bytes  log_id             = 2;
  uint32 level              = 3;
  uint64 index              = 4;
  bytes  packed_entries     = 5;  // the sealed block's, verbatim
}

message CommittedBlock {
  uint32 level          = 1;   // node (level, index): ms [index * 2^level, (index + 1) * 2^level)
  uint64 index          = 2;
  // Per entry, newest first, back-to-back: varint delta_ms (gap from the
  // entry before; the first from the node's top) || 10 random bytes
  // (rand_a + rand_b + 6-bit zero pad).
  bytes  packed_entries = 3;
}
```

`org.intentlog.wire` encodes them by hand (`LogEnvelope`, `CommittedSetCodec`,
`ProtoWire`), in protobuf wire format that `protoc`-generated readers accept,
as the catalog's codecs do (see [SPEC.md](SPEC.md) §"Wire Format Details").
The encoder writes `min_reader_version` first, so a reader that cannot read
the checkpoint fails before decoding the rest. A reader fails on a
`LogCheckpoint` whose `min_reader_version` exceeds what it supports, or that
lacks it, the log id or the state; and on a `LogRecord` with no body, with
two, or with an intention and no id.

**Pages.** The ids between the horizon and the floor live in pages: immutable
objects the provider keeps apart from the log (`SharedLog.pages()`, a
`PageStore`; `AtomicFileLog` beside its object, under
`<location>.committed/<name>`). A compaction seals the oldest blocks older
than the admission window into pages, putting each page before the `PageOut`
record that names it; replay never reads a page, since it admits no id at or
below the floor. [COMMITTED_PAGING.md](COMMITTED_PAGING.md) is the design.

**The committed set's encoding.** `CommittedTransactions` exploits the
structure of UUIDv7 to compress the set. Its ids are cut into blocks on a grid
over their timestamps: node (k, a) is the aligned span [a·2^k, (a+1)·2^k) of
milliseconds, and the blocks are the maximal nodes that hold at most N ids
(2^16, a constant of the format; tests use 16), among the nodes that hold any;
a millisecond is a block however many ids it holds. The blocks are a function
of the ids alone, not of the order replay added them or where its builds fell,
so every replica at a position writes the same bytes (I11), and they are the
unit [COMMITTED_PAGING.md](COMMITTED_PAGING.md) pages. A block lists its
entries newest first: each entry's varint gap from the one before (the first's
from the node's top) and its 10 bytes of random material (the 12-bit `rand_a`
and 62-bit `rand_b`, with the fixed 4-bit version and 2-bit variant dropped on
encode and restored on decode), back to back in one `packed_entries` field.
For typical clustered commit traffic this saves about 30% per entry over a
`repeated bytes` form ([UUID_V7_BENCH_RESULTS.md](UUID_V7_BENCH_RESULTS.md)).

In memory, `CommittedSet` keeps each block's packed entries in runs, newest
first, each run's entries older than the run before it: ids newer than a
block's every entry (replay's almost always are) become a run of their own,
the newest runs merge by concatenation up to 1 MiB, and an older id merges
only with the entries at or above it. An id outside every block opens the
largest node that holds it and overlaps no block; a block that passes N ids
splits into the maximal nodes inside it; a GC drops the oldest blocks, cuts
the one its watermark falls in, and merges the oldest blocks with their
neighbours once their common node holds N or fewer. So a build costs the ids
it adds (and a split, now and then, a block), not the set, and a checkpoint
copies each block's bytes: a block decoded and unchanged is a slice of the
body it came from.

`CommittedSet.contains` finds an id's block by its node and walks that
block's runs in place, without materializing the set.
`CommittedSetCodec` encodes and decodes the whole set from scratch: the
reference the incremental set must match byte for byte
(`TestCommittedSetBlocks`). Every entry is a UUIDv7:
`CommittedSet.Builder.add` refuses any other, and replay rejects a record
whose id is not one.

## Replay

`Replay.replay` folds the records after a checkpoint into the state it holds,
in log order:

```
function replay(machine, checkpoint, records, watch):
    if checkpoint:
        env      = decodeCheckpoint(checkpoint)
        (b, set) = (machine.decodeCheckpoint(env.state), env.committed)
    else:
        (b, set) = (machine.empty(), EMPTY)
    outcome, seen, n = NOT_SEEN, false, 0
    for r in records:
        if r is AdvanceHorizon(w):
            set.advanceHorizon(w)             # GC: see below
            continue
        if r is PageOut(p):
            set.pageOut(p)                    # seals the oldest block if it is still p's
            continue
        n += 1                                # intention records: what the count threshold compares
        seen |= (r.id == watch)
        if not set.mayApply(r.id):
            continue                          # a copy of one applied, or at or below the floor
        i = isV7(r.id) ? machine.decodeIntention(r.intention) : none
        if i and i.verify(b):
            i.apply(b)
            set.add(r.id)
            if r.id == watch and outcome == NOT_SEEN: outcome = APPLIED
        elif r.id == watch and outcome == NOT_SEEN:
            outcome = REJECTED
    return (b, set, n, outcome, seen)
```

A record that fails verification, or that `mayApply` refuses, is skipped, not
an error: concurrent writers append records that conflict, and the one that
lands second has no effect. `watch` is a committing writer's own id:
`outcome` is what replay did with the first record of it that it judged, and
`seen` says whether any record of it was among the records replayed. A writer
learns its outcome from these (see [The commit loop](#the-commit-loop)).

Where replay rejects the watched record, it asks the intention why
(`Intention.explain`, on the state it was judged on), and the result's
`rejection()` carries the reason to the writer: a `CONFLICT`
`CommitRejectedException` holds it (`rejection()`), as it does when the
commit loop finds on a fresh read, before writing, that the intention no
longer holds. A replay given a `Replay.Rejections` listener tells it of every
record it rejects and why, for counting a log's conflicts by their cause.
An explanation is asked for only on a rejection, so a replay that rejects
nothing pays nothing; it never fails the read. The catalog's reasons are
`"<category>: <detail>"` strings ([SPEC.md](SPEC.md) §"Conflict Encodings").

Replay is deterministic: the state machine is (S1), the order is the log's
(P1), and only GC records move the committed set's horizon (below). So every
reader, from whichever checkpoint, reaches the same state, committed set and
horizon at the same position, and **every checkpoint is the replay of the log
through its position (I11)**, whoever wrote it and whatever their clock said.
A log whose readers start from different checkpoints needs I11 for them to
apply the same records.

`LogRuntime.read()` replays a fresh `read()` into a `Replica`: the state, the
committed set, the tail the read ended at, the number of intention records
after the checkpoint, and the log's id. `refresh(prior)` folds the records a
`readAfter(prior.tail())` returns onto `prior` (`Replay.replayOnto`), or
replays afresh if the log trimmed them or returned a whole view. By I11 the
two give the same replica.

A replica the runtime replays from a whole view keeps that view. A later whole
view that continues it byte for byte (the same checkpoint, then the same
records and more) is replayed by folding only the records after it onto the
replica: a read with a floor (`read(floor)`, `refresh` of a whole view), a
commit's reads after a lost race (onto the replica it holds) and its read
through its own write (onto the replica the write followed, so one record
after an append). The same bytes replay to the same result, so the fold is
replay, not a cache that could go stale: no provider obligation, and nothing
in the protocol, changes. Comparing the bytes costs a memory compare of the
view; replaying them again costs decoding the checkpoint, which grows with
the state (docs/REPLAY_BENCH_RESULTS.md). A view whose held records include
one of the watched id is replayed whole, since the fold cannot say what replay
did with that record. With `-Dintentlog.replay.check=true` (the test suites
set it) each fold is checked against a replay of the whole view.

## The committed set and GC

The committed set holds the id of every intention record replay applied, less
those GC dropped. Replay uses it to apply a record once: a copy of a record
(a retried append whose first attempt landed, a send throttled and sent
again, a request delayed in flight) changes nothing. A writer uses it to
learn whether its record applied.

**Ids approximate commit time.** A commit mints its id when it starts
(`LogRuntime.commit`, with `UuidV7.newUuidV7After(base.committed().floor(),
clock)`, the runtime's clock), not when its transaction began: the id embeds
the writer's clock, or the floor of the log it read plus 1 ms if the clock has
not passed it. A
transaction that ran for a week has an id from the moment it commits. So a
retention can be stated in wall-clock time without penalizing slow
transactions.

**Horizon.** `highest_dropped_timestamp_ms` is the set's GC horizon: the
largest timestamp of any id GC dropped. It is 0 until the first drop, never
decreases, and can exceed the newest id kept: a GC that drops every entry
writes a set with no blocks and a horizon.

**GC is a record.** Only a GC record (`LogRecord.advance_horizon`) drops ids
or moves the horizon. Replay applies one wherever it lands: it drops every id
the set holds there, the checkpoint's and replayed ones alike, whose timestamp
is at or below the watermark, and raises the horizon to the newest it drops.
Applying it twice, or an older one, changes nothing. A GC record carries no
id, is not deduped, and does not count as an intention record. A checkpoint
holds exactly the set the replay before it reached (I11).

```
function watermark(ceiling):                       # LogRuntime
    now = clock()
    w   = retention >= now ? 0 : now - retention
    return max(0, min(w, ceiling))                 # 0: GC off, no record written

function advanceHorizon(set, w):                   # replay of AdvanceHorizon(w)
    kept, horizon = [], set.horizon
    for entry in set:                              # descending
        if timestamp(entry) > w: kept.append(entry)
        else: horizon = max(horizon, timestamp(entry))
    return CommittedTransactions(kept, horizon)
```

Two writes carry a GC record:

- **A replace** (a compaction on a log that replaces itself) writes
  `[checkpoint][gc][page-outs][record]`: the checkpoint of the state it
  replaces, a GC record at `watermark(timestamp(id) - 1)`, the PageOuts of a
  log that keeps pages, and its own record. The ceiling keeps the horizon
  below the record that follows, which replay would otherwise reject (Bug 64),
  and so below every id minted after this one's, such as a commit's this one
  rebased on. The PageOuts keep the floor below it the same way.
- **`compact()`** on a log with separate checkpoints appends a GC record at
  `watermark(∞)` before it checkpoints ([Compaction](#compaction)).

An append of an intention never carries one.

A GC record also drops the pages whose top is at or below its watermark,
raising the horizon to each one's newest id, and the floor to at least the
horizon; a page that straddles the watermark is kept whole. The writer whose
GC record dropped a page deletes it once the record has landed: a reader that
still holds an older replica then finds the page missing, and reads again.

**Lookup is tri-state.** `CommittedSet.contains(q)`, with inline entries `P`,
floor `F` and horizon `H` (`F = H` until ids are paged):

| Outcome | Condition | Result | Meaning |
|---|---|---|---|
| `COMMITTED` | `q ∈ P` | `true` | the record applied |
| `ABSENT` | `q ∉ P` and (`F = 0` or `timestamp(q) > F`) | `false` | the record has not applied: the set would hold it |
| from the page | `H < timestamp(q) <= F` | throws `PagedOutException` naming the page | the page holds `q` exactly if its record applied; either way it never applies again |
| `ABSENT` | `H < timestamp(q) <= F`, and no page's range holds `q` or `q` is above its page's newest id | `false` | nothing applied there, and nothing can |
| `UNKNOWN` | `q ∉ P`, `0 < H` and `timestamp(q) <= H` | throws `DedupHorizonExceededException` | its id may have aged out; the set cannot tell |

A non-v7 `q` returns `false` without consulting the floor.
`LogRuntime.committed(replica, q)` answers the same, reading a page through
the log's page store (a page GC has dropped since the replica was read sends
it to a fresh read).

**Replay rejects a record at or below the floor.** Replay asks
`CommittedSet.Builder.mayApply`, not `contains`, which would throw for such a
record and so fail every read of the log (Bug 64). `mayApply` admits a record
only if the set does not hold its id (else it is a copy of one applied) and
its timestamp is above the floor (else it may be a copy of one whose id was
paged or aged out; replay never reads a page). Such records are reachable: a writer whose clock is behind another's by
more than the retention, one that stalls that long between minting its id and
writing, or a race under a short retention. The commit loop maps each to an
outcome ([The commit loop](#the-commit-loop)).

**A writer has a second oracle.** Past the horizon a writer can still learn
whether its commit landed from what it committed, which it knows: a data
commit's snapshot ids (a load of any one of its tables that finds its
snapshot proves the whole transaction applied, since all of it applies or
none does), or a namespace's or property's new value. That is the writer's
pattern, not the log's: the log keeps no index from an id to what its
intention touched, and needs none, so the retention can be short without a
writer losing the answer for good.

**Retention.** `RuntimeConfig.committedRetentionMs` (the catalog's
`fileio.catalog.committed.retention.ms`, default 30 minutes): the minimum age
before an id may be dropped. It should comfortably exceed the longest time a
writer may take to learn its outcome. `Long.MAX_VALUE` disables GC. The set's
size is bounded by retention × commit rate, at about 11 B an id: 1,800 ids
(20 KB) at one commit a second with 30 minutes, 180,000 (2 MB) at 100 a
second, and 1.8 million (20 MB) at 1,000.

**A short retention is lossy.** A concurrent GC can drop a committed id newer
than an in-flight writer's id, or minted in the same millisecond (with a
retention of 0, every GC drops every id older than its writer's clock). The
horizon then covers the writer's id, replay rejects its record, and the writer
is rejected (`BEYOND_HORIZON`) or, if a write of it may have landed before,
cannot learn its outcome. The log is never made incorrect or unreadable.

**Clock skew.** A watermark compares its writer's clock with ids other writers
minted. A writer whose clock is 10 minutes behind has its ids appear 10
minutes older, which shrinks its retention by as much. Keep clocks
NTP-synced. That is a deployment requirement, not a correctness invariant: a
writer behind by more than the retention still mints its id above the horizon
it read, and a record that lands at or below the horizon is rejected, never
applied twice or left to fail reads. Skew costs spurious rejections and
unknown outcomes. Nothing bounds a stamp from above, so a clock far ahead
costs more: its ids stay until the clocks reach them, and its compactions can
move the horizon ahead of every clock (errata D21).

**Tests.** `TestCommittedSetGc` (intentlog-core) checks the set's rewrite
(drops at the watermark, a horizon above the newest id kept, a monotonic
horizon), the tri-state lookup and a GC record's replay (ids replay added are
dropped too; idempotent); `TestCommittedTransactions` the set's codec and its
defensive decode. `CheckpointRoundTrip` checks that every checkpoint, set and
horizon included, is the replay of what it replaced (I11).
`TestCatalogRegressions.CommittedSetHorizon` covers Bug 64 (a CAS after a
conflict with retention 0, a record at the horizon, a writer behind it), and
`TestSharedLogRuntime` the shared-log cases of [The commit
loop](#the-commit-loop), each a schedule that fails with its rule removed. The
protocol fuzzers (`TestMemoryCommitProtocol`, `TestAtomicFileCommitProtocol`)
and `TestMultiWriterProperties` run compactors with retention 0, inside
commits too, and append copies of records that ever landed, none of which may
apply; `TestCatalogFaultProperties` (`gcWriters`) and
`TestCatalogRaceProperties` (`gc`) run writers with retention 0, accepting a
failed or unknown operation only if the run is serial with or without it.
`CommittedTxnsBench` (JMH) measures the set's size and lookup cost.

## The commit loop

`LogRuntime.commit(base, intention)` commits an intention staged on `base`, a
replica, and returns the replica its record landed in. It reports one of
three outcomes, and a caller can rely on each:

- **it returns**: the intention took effect, once;
- **`CommitRejectedException`**: it did not, and never will. Its `reason` is
  `CONFLICT` (its preconditions do not hold on `state()`, the replica it lost
  to), `BEYOND_HORIZON`, `CREATE_LOST` (another writer created the log first),
  `EXHAUSTED` (out of attempts with no write that may have landed),
  `NOT_SENT`, or `TOO_LARGE` (its record is larger than an append stores, on
  a log that cannot replace itself; nothing was sent);
- **`CommitOutcomeUnknownException`**: either. Its cause is the write that may
  have landed, with any read failures since suppressed on it.

A read failure while no write of the commit may have landed is rethrown as it
is. The catalog maps these in one place (`CatalogLog.commit`): `CONFLICT` to
`CatalogMutation.explain` of the state it lost to (`AlreadyExistsException`
if a pending create or a rename's destination now collides,
`NoSuchTableException` if a rename's source is gone, else
`CommitFailedException`), the other rejections to `CommitFailedException`,
and an unknown outcome to `CommitStateUnknownException`.

The loop has one branch per capability: a log that replaces itself replaces
itself with the commit's record when no append can carry it, and is compacted
after the commit otherwise ([Compaction](#compaction)); one with conditional
appends appends at the tail it read; one without appends unconditionally.

```
# landed(view, earlier): whether view holds this commit's record. Replay's
# judgment of the record comes first (APPLIED: yes; REJECTED: no); if replay
# judged none, the committed set says. If the id is in (H, F], replay rejects
# the record from then on, and the page that holds the ids there says whether
# it applied before the floor passed it: committed, or rejected (BEYOND_FLOOR);
# a page that cannot be read is a failed read (unknown if earlier is set). If
# the id is at or below the set's horizon, the set cannot say: the commit is
# unknown if earlier (a write of it that may have landed) is set, else
# rejected (BEYOND_HORIZON).
# validate(view) returns view if landed(view, earlier()), else rejects (CONFLICT);
# a page it cannot read leaves the write ambiguous.
# earlier() = ambiguous, else unresolved, else duplicate.
# gc() = [AdvanceHorizon(watermark(timestamp(id) - 1))], or [] when GC is off,
# then the PageOuts of the set after it, sealing blocks with tops at or below
# min(now - A, timestamp(id) - 1), their pages put first (COMMITTED_PAGING.md).

function COMMIT(log, base, intention):
    id      = newUuidV7After(base.committed.floor)
    record  = LogRecord{ id, intention = encode(intention) }
    caps    = log.capabilities()
    current = base;  tail = base.tail;  stale = false
    floor   = (not caps.fusedReplace and tail.exists) ? tail : none
    oversized = caps.maxRecordBytes > 0 and len(record) > caps.maxRecordBytes
    if oversized and not caps.fusedReplace:
        reject TOO_LARGE                          # no append can store it (Bug 71)

    # Case 0: the log does not exist yet.
    if not tail.exists and caps.fusedReplace:
        try:
            return validate(WRITE(log.replace(tail, checkpoint(base), gc() + [record])))
        catch Ambiguous or Conflict as e:
            # The creation may have landed: its reply was lost, reading it back failed,
            # or it failed its condition, as a transport retry of a creation that landed does.
            fresh = READ()                        # failure: unknown
            if landed(fresh, e): return fresh
            if e is Conflict: reject CREATE_LOST
            unknown
    if not tail.exists:
        CREATE()                                  # the first checkpoint, stored apart (below)

    ambiguous = unresolved = duplicate = none     # writes that may have landed
    full = false                                  # the log refused an append: it is full
    for attempt in 1..maxAttempts (10):
      try:
        # compaction after the commit (the default): past the policy's limits the commit
        # appends, and its writer compacts after it (COMMIT's caller, below); the policy
        # makes it replace only if due on a log with no records (CAS only). Fused: past them.
        due = config.compactAfterCommit
              ? policy.due(0, current.checkpointBytes, len(record))
              : policy.due(current.intentionRecords, tail.bytes, len(record))
        mustReplace = caps.fusedReplace and (full or oversized or not caps.append or due)
        if mustReplace:
            if stale:
                # The replace supersedes the log at its tail, so its checkpoint must hold
                # the state there; a checkpoint of an older read would erase every record
                # committed since (Bug 12). Read afresh: storage fails a read pinned to a
                # version another writer has replaced since (Bug 26).
                fresh = READ()
                if landed(fresh, earlier()): return fresh   # an append reported lost landed
                settle(fresh);  adopt(fresh)
            full = false
            try:
                return validate(WRITE(log.replace(tail, checkpoint(current), gc() + [record])))
            catch Conflict as e:
                unresolved = e;  raceBackoff()      # interrupted: unknown
            # Rebase: replay verifies the record against the state it lands on, so if
            # the intention holds on the new state, retry on top of it (append or
            # replace, as that state calls for). The commit is accepted exactly when a
            # serial commit would be.
            fresh = READ()
            if landed(fresh, earlier()): return fresh       # an earlier attempt landed
            settle(fresh)
            holds = intention.verify(fresh);  adopt(fresh)
            if not holds: reject CONFLICT
        else:
            try:
                expected = caps.conditionalAppend ? tail : none
                return validate(WRITE(log.append(record, expected)))
            catch Full:
                full = true;  stale = true;  continue       # replace on a fresh read (Bug 66)
            catch Conflict as e:
                unresolved = e;  lostAt(expected)   # a shared log: the furthest such tail
                raceBackoff()                       # interrupted: unknown
            # Another writer appended first. With tail reads, on a log that replaces
            # itself, read what followed current (or the whole log, if replaced) and
            # append again at that tail, knowing what the log holds there.
            if config.tailReads and caps.fusedReplace:
                fresh = READ()                    # a read after current's tail
                if landed(fresh, earlier()): return fresh
                settle(fresh);  adopt(fresh);  continue
            # Else the record is the same, and replay applies it once, so only the new
            # tail is needed.
            probed = log.probe()
            if not probed.continues(tail):        # replaced (compacted): read the new log
                fresh = READ()
                if landed(fresh, earlier()): return fresh
                settle(fresh);  adopt(fresh)
            else:                                 # it grew: resend at the new tail
                tail = probed;  stale = true      # current's record count is a lower bound
      catch Ambiguous as e:
        # The write may have landed. Its record carries id, so a fresh read says. If it
        # has not landed and the intention still holds, retry: a write still in flight
        # carries the same record (and, if conditional, a condition on the tail it saw),
        # so replay applies it once. A rejection stays one: S2.
        ambiguous = e
        fresh = READ()                            # failure: unknown
        if landed(fresh, e): return fresh
        settle(fresh)
        holds = intention.verify(fresh);  adopt(fresh)
        if not holds: reject CONFLICT
      catch Unreadable as e:
        if earlier(): unknown                     # a write may have landed
        rethrow e
    if ambiguous: unknown                         # a write may yet land
    if unresolved:                                # the last append lost a race, which
        fresh = READ()                            # its own landing also reports
        if landed(fresh, unresolved): return fresh          # failure: unknown
        if not settles(fresh): unknown            # it lags behind where the write would be (Bug 90)
    reject EXHAUSTED

# A LogNotSentException out of the loop: unknown if earlier(), else reject NOT_SENT.

function WRITE(send):                             # one write, read back
    last = none
    for attempt in 1..maxThrottleAttempts (8):
        try:
            receipt = send()
        catch Throttled as t:
            last = t;  backoff();  continue       # interrupted: unknown
        catch NotSent as e:
            if last: throw Ambiguous(e)           # the throttled send may have landed
            throw e
        catch Conflict or Full: rethrow
        catch other: throw Ambiguous(other)
        if send was unconditional and last:
            duplicate = last                      # the throttled send may have landed beside it
        view = replay(log.readThrough(receipt, visibilityWait), watch = id)
                                                  # failure: Ambiguous
        if not view.seen: throw Ambiguous         # P3: only a view with the record settles it
        return view
    throw Ambiguous(throttled out)                # a throttled send may have landed

function READ():                                  # any failure: Unreadable
    view = FETCH()
    while floor and not view.tail.continues(floor):
        if read maxThrottleAttempts times: throw Unreadable(LogLagException)
        sleep(visibilityWait / maxThrottleAttempts);  view = FETCH()
    if config.tailReads and current:              # fold what continues current's
        return replayAfter(view, current, watch = id)   # records alone: only onto its tail
    return replay(view, watch = id)

function FETCH():                                 # with tail reads, what follows current
    if config.tailReads and current:
        try: return log.readAfter(current.tail)
        catch Trimmed: return log.read()
    return log.read()

function adopt(fresh):
    current = fresh;  tail = fresh.tail;  stale = false
    if not caps.fusedReplace: floor = tail

function CREATE():                                # a log with separate checkpoints
    try: log.putCheckpoint(tail, checkpoint(base))   # the empty state, base's new log id
    catch any: pass                               # another writer's create, or ours
    fresh = READ()                                # failure: reject NOT_SENT
    if fresh.logId != base.logId: reject CREATE_LOST
    adopt(fresh)
```

`backoff()` waits `RuntimeConfig.throttleMinWaitMs` (the catalog's
`fileio.catalog.throttle.retry.min-wait-ms`, default 200 ms) at first, then
twice the previous wait up to the larger of 5 s and that value, each plus up
to 50% random jitter. `raceBackoff()` waits a time drawn uniformly below a
bound (full jitter) that follows the latency of the commit loop's writes as
the runtime observes them (`RuntimeConfig.raceByLatency`, the catalog's
`fileio.catalog.race.retry.basis` `latency`): the runtime keeps a moving
average (weight 1/8) of each send's time from request to answer, landed or
lost its race, and the bound is 4 of it after the commit's first lost race,
doubled for each later one, up to 64 of it. The writers who lost to one
winner then spread over a few write slots, then over as many as there are of
them, whatever the store and wherever the writer runs (errata D14); on S3
Express as the benchmarks model it (an append ~22 ms) that is ~88 ms first and
~1.4 s at most. The ablation, basis `fixed`, starts the bound at
`RuntimeConfig.raceMinWaitMs` (`fileio.catalog.race.retry.min-wait-ms`, default
100 ms) and doubles it up to the larger of 1 s and that value; 0 waits for
neither. The writers who lost to one winner would otherwise all read and
resend at once, and on a log that only replaces itself most of them lose
again, each loss costing a read of the whole log (the contention benchmark:
13.7 lost races per committed operation at 32 writers on an S3
general-purpose bucket). A wait changes when a write is sent, never what
it says, so the protocol (and its TLA+ models, which have no time) is
unchanged. `checkpoint(r)` is exactly `r`'s log id, committed set
and state.

**What may have landed.** A commit is rejected only before any write, or after
a read of the log that shows its record rejected or absent with no write of it
still able to land (I10). The loop tracks three kinds of write that may have
landed:

- `ambiguous`: a write whose reply was lost, whose read back failed, that
  storage throttled until the sends ran out, or whose read back did not
  include its record. Once there is one, running out of attempts is not a
  failure: the write may still be in flight.
- `unresolved`: a write that failed its condition. A transport retry of a
  write that landed fails its condition, so the failure is no proof; but that
  write landed before the reply, so a read without it rules it out, if the
  read reaches where the write would be (`settle`, below).
- `duplicate`: on a log without conditional appends, a send the log throttled
  before the one it acknowledged. Both may have landed (below).

**The landed rule.** A writer judges its record by replay's judgment where the
record landed, and only if replay judged none, by the committed set. On a log
with checkpoints apart, another compactor's GC record can land between a
writer's record and its read, and drop the id of a record that applied; the
committed set alone would report a landed commit as failed (`setonly` in
`SharedLogCommit.tla`). On one object the two always agree: a version's only
GC record precedes every record appended to it.

**Unconditional appends.** A log without conditional appends needs no fencing.
A copy of a record that applied is skipped by its id, or once GC dropped the
id, by the horizon. A commit is rejected only on a read where its intention
does not hold, and a copy that lands after that read is rejected too (S2).
The shared-log model found three rules this path needs, each a bug switch
there ([spec/tla/README.md](../spec/tla/README.md) §"The
shared-log model"):

1. **A throttled send is an earlier write** (`dup`, `duplicate` above). A
   throttled send may have landed, and its retry lands beside it. If a
   compactor checkpoints between the two copies and a GC record then drops the
   id, a read through the acknowledged copy starts from that checkpoint;
   replay skips the copy (its id is committed there) and the set can no longer
   say. The commit must report unknown, not failed. On one object the retry
   fails its condition instead (P4).
2. **A commit's reads do not go back** (`lagread`, `floor` above). After an
   ambiguous write, a commit fails if its intention does not hold on a fresh
   read. That is sound only on a read at or past the one the intention was
   staged on: S2 holds from there, and every copy of the record lands after
   it. On a read that lags behind it, the intention may not hold *yet* while
   its record has applied. So each read must reach the commit's floor (the
   tail it staged on, or a later read it adopted); one that lags is read
   again, and then the log is unreadable (`LogLagException`).
3. **A read back must include the record** (`p3`, `seen` above). A view whose
   checkpoint already holds the record cannot tell an applied record whose id
   GC dropped from one that never applied. Such a view leaves the write
   ambiguous, and a fresh read settles it.

**Conditional appends on a log that cannot replace itself** (the Azure log's
`conditional` ablation, `InMemorySharedLog`'s conditional mode). A lost
append is resent at the probed tail, and the read through the resend settles
an earlier copy (P3). A commit that runs out of attempts with every send lost
has only a plain read to settle the last loss, and such a read may lag (P1).
If the append landed, its record follows the tail it named, so a read settles
the loss only if its view is past the furthest tail a lost send named
(`lostAt`, `settle`). On a read that lags behind it, the commit's outcome is
unknown (Bug 90 (lagging read hid the record)). Without the check, a commit
whose sends had landed with replies saying they lost was rejected
(`EXHAUSTED`). The rule needs a provider's conflict to mean that the log moved
past the tail the append named, so that a read that does not lag settles the
loss: the Azure log conditions an append on its position, not its segment
(Bug 89 (moved on, same position)). A read of one object is of the object as
it is, so there any read settles a loss. `SharedLogCommit.tla` models
unconditional appends only.

**Restaging is the caller's.** The loop rejects an intention that no longer
holds on the state it lost to (`CONFLICT`); staging it again on a fresh read
is the caller's choice. The catalog restages namespace operations, renames,
drops and table creates on a fresh read, up to Iceberg's default number of
attempts (`LogCatalog.commitOnAFreshRead`, `dropTable`,
`FileIOTableOperations.commitCreate`), and Iceberg re-applies a table commit.
A catalog transaction is not restaged: its deltas are judged where they land,
and each table it pins conflicts on its whole version ([errata.md](errata.md)
§D12).

### Tracing

A thread that opens a `CommitTrace` gets the runtime's events on it, each
with the time and the thread's CPU time it came at, until it drains them; a
thread without one costs a thread-local lookup per event. A commit reports
its start and end (how it ended, the races it lost), each attempt (append or
replace), a replace's payload ready, each write sent and how it ended
(landed, lost, ambiguous), each wait after a lost race, each probe, and each
read in three steps: begun (after a tail or whole), its view arrived (with a
checkpoint or records alone, and how many), and replayed (folded onto the
replica, or replayed from its checkpoint, and how many records); then the
replica adopted (its state built). A compaction after the commit follows its
end: begun (the records it landed among, the log's bytes), its payload ready,
sent and how it ended. On one object a tail read's view holds
the replica's checkpoint and records and the frames after them, so whether a
read found the object extended or replaced is the replay's fold. A client's
reads (`read`, `read(floor)`: loads and refreshes) report the same steps,
ending built. So the time a step took is the gap to the next event, and its
CPU the gap in CPU time: a read's I/O is begun to arrived, its replay arrived
to replayed. catalog-bench records each job's trace (`trace_commits`), for
TAIL_READS.md's question of tail reads past the commit knee.

## Compaction

Compaction bounds what a reader replays, and with GC, the committed set.

**On a log that replaces itself** (`fusedReplace`: `AtomicFileLog`), the
writer of a commit compacts the log after the commit landed
(`RuntimeConfig.compactAfterCommit`, the default since 2026-11-10;
[TAIL_READS.md](TAIL_READS.md) "Compaction after the commit"), or the commit
compacts as it lands, fused (for ablation). The compaction policy says when the
log is due: `CompactionPolicy.limits(maxRecords, maxBytes, basis)` once the log
holds `maxRecords` intention records (0: every commit), or the record would take
what `basis` counts past `maxBytes`: the records after the checkpoint (`TAIL`,
the default), or the whole log (`OBJECT`, for ablation, under
which a checkpoint past the bound compacts on every commit). The policy sees
the replica's record count, its checkpoint's size and the log's
(`CompactionPolicy.Input`); the catalog's knobs are
`fileio.catalog.max.append.count`, `max.append.size`,
`max.append.size.basis` and `compact.after.commit`. Both limits are soft. Writers
configured differently disagree on when to compact, but cannot corrupt the log:
every write is conditional.

A commit replaces the log with a checkpoint of the state it read, a GC record
and its own record (`[chk][gc][page-outs][rec]`) when no append can carry the
record: the log cannot append, an append was refused as full (a store's cap on
writes to one object), the record is larger than an append stores
(`maxRecordBytes`), or the policy is due even on a log with no records after
its checkpoint (CAS only, or a record past the size limit); fused, also when
the policy says the log is due.

After the commit, once its record landed in a replica `R`, if the policy says
`R` is due (and is not due on every commit) and `R`'s progress
(`CompactionPolicy.progress`: the larger of its count's and size's fractions of
the limits) has reached the point the writer drew, the writer replaces `R`'s
version with a checkpoint of `R`, a GC record at `watermark(∞)` and the
PageOuts the policy calls for, sealing blocks with tops at or below `now - A`,
their pages put first: `[chk][gc][page-outs]`, no record of its own, so neither
is kept below one. Conditioned on `R`'s version, it lands only if no write
landed since, and then holds the state the log held (I11). One that loses its
race to appends may be sent again: a tail read after `R`'s version finds the
records that beat it, and if `CompactionPolicy.resend` says one is likely to
land (fewer than one append expected over a read and a send, at the rate they
beat it; at most 4 times), the same checkpoint goes again, conditioned on the
version read, with those records copied in front of its GC record and PageOuts:
`[chk][records][gc][page-outs]`, which replays them on the checkpoint as they
were replayed on `R`. One that loses to a replace, fails or stays throttled is
dropped (one that lands late lands on the version it was conditioned on), and
the commit stands whatever becomes of it. After it lands,
the pages its GC record dropped are deleted and the orphans swept. Each writer
(each runtime) draws its point: in [1, 2) of the limits after the log was last
compacted (a progress below the last one it saw says so), and after each
compaction it tried, the progress then plus a draw in [0, 1), doubled for each
compaction in a row that lost (up to 2^10): writers seldom compact at once, and
writers that keep a log busy try ever more seldom (the policy's `draw`; a test
passes its own, `withDraws`). A compaction lands only between appends: under
sustained load past what one compaction's encode and PUT allow, the log grows
until the store refuses an append, and the commit that meets the cap replaces
it with its record.

**On a log with separate checkpoints** (`separateCheckpoints`:
`InMemorySharedLog`, Kafka, SQS, a ledger), `LogRuntime.compact()` compacts:

1. append a GC record at `watermark(∞)`, sent again while the log throttles it
   (none when GC is off);
2. `readThrough` it, and replay the view;
3. if the log keeps pages, seal the oldest blocks older than the admission
   window while the inline set exceeds the threshold: put each page, append
   its `PageOut`, and `readThrough` the last ([COMMITTED_PAGING.md](COMMITTED_PAGING.md));
4. `putCheckpoint` the replay at the view's tail;
5. `trim` through that tail;
6. delete the pages the GC records its replays folded dropped, its own record
   and any other in its view, whether the checkpoint named them or a PageOut
   after it did (replay's set builder lists them, `droppedPages`), and, if it
   dropped or sealed any, sweep the orphans (unreferenced pages whose node ends
   at or below the floor).

Any compactor may run it at any time, and each step is safe to repeat or to
abandon: a GC record that lands twice drops nothing new, a PageOut that lands
twice or after its block changed changes nothing, a checkpoint is the replay
of the log through its position whoever writes it (I11), and the trim follows
a durable checkpoint (P5). A writer whose commit landed compacts, best
effort, when the policy says the log it landed in is due and its progress has
reached a point the writer drew past the limits (`LogRuntime.compactApart`); a
failure is logged, the commit stands, and the writer's next point is twice as
far on. The policy counts the records and bytes since the checkpoint or since
the last GC record after it (`Replica.recordsSinceGc`, `bytesSinceGc`): a
writer that reads only what follows its own tail never sees another writer's
checkpoint, but replays the GC record its compaction began with, and counts
again from there. Counting from the checkpoint each writer last read, every
writer compacted at every threshold (AZURE_LOG.md "When to compact"). A
background compactor, or a queue's archiver, can call `compact()` (the
catalog's `CatalogLog.compact()`) as well or instead.

A compactor's GC record has no commit of its own to stay below: its watermark
can pass the id of a record still in flight, which replay then rejects, as a
short retention does on one object. With a retention longer than any commit,
it does not.

## Providers

| Provider | append | conditional | fused replace | checkpoints / trim | `readThrough` (P3) | receipt |
|---|---|---|---|---|---|---|
| `AtomicFileLog`, store with append (S3 Express, ADLS) | at an offset | yes | `[chk][gc][page-outs][rec]` by CAS | in the object | the object the write left: known from the write, or read | the version written |
| `AtomicFileLog`, CAS only (GCS, S3 standard) | no | – | the same | in the object | what the replace wrote | the version written |
| `InMemorySharedLog` | yes | optional | no | a store apart, and a trim point | from a checkpoint before the record | a position |
| Kafka (one partition) and an object store | idempotent produce | no | no | `chk/<offset>` objects; `deleteRecords` | wait for the high watermark | an offset |
| SQS FIFO and an archiver | `SendMessage`, deduplication id = record id | no | no | the archiver's: it appends GC records, checkpoints and trims | poll the archive | a sequence number |
| A ledger (BookKeeper, Pulsar, a confidential ledger) | append | no | no | external, or in the ledger | wait for commit | an entry id |

**`AtomicFileLog`** keeps the log in one object: a header, a checkpoint, and
the records appended after it ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)). A
tail is the object's version (ETag or generation) and length, and continues an
earlier one if the object did not shrink; a replace usually makes it shorter.
A read through a write returns the object the write left, so P3 holds
trivially; it reads nothing when the write says what that is (a replace holds
what it wrote, and an append conditioned on a tail whose bytes the log read or
wrote holds those bytes and the record: the condition names the version, which
no later write leaves in place), and reads the version written after an append
at a tail a probe found. It maps the
fork's exceptions onto the SPI's: `StorageThrottleException` to throttled
(before `CASException`, its superclass), `SegmentLimitException` to full,
`CASException` and `AppendException` to conflict, and a failure to prepare a
write to not sent. Its `maxRecordBytes` is the FileIO's
`maxAppendBytes(location)` less the record's length prefix: the ADLS FileIO's
is its client's `AppendBlock` limit (4 MiB before service version 2022-11-02,
100 MiB since); the others set none.

**`InMemorySharedLog`** is a CORFU-like log: records at positions 1, 2, ...;
checkpoints stored apart, each the state through a position; and a trim
point. It is created by its first checkpoint, at position 0. Appends are
unconditional, or conditional if it is built so. A read starts from the newest
checkpoint that covers the trim point; `readThrough` starts from one before
the receipt's record (P3) and throws `TrimmedException` if every checkpoint it
keeps is past it. A trim needs a checkpoint at or past it (P5), and two
checkpoints at one position must be the same bytes (I11): one that differs is
refused and recorded (`violations()`). Its positions are record counts (a
big-endian int64), and each view says where its records start. Its hooks see
every request: they fail a write before or after it lands, throttle it, delay
it (it lands later, at `deliverDelayed`), fail a read, serve one from an older
checkpoint or past the record, lag one behind the tail, or serve one with a
gap after its checkpoint or tail (which the runtime must refuse). It keeps every record ever appended,
trimmed ones included, for oracles to replay.

**Kafka, SQS and ledgers** are mapped on paper only. What each must provide:

- P3 and read lag: a read through an offset waits until a replica has it, and
  starts from a checkpoint before it. A replica that lags is legal (P1); the
  commit loop waits for its floor (`visibilityWait`), then treats the log as
  unreadable.
- P5: no time- or size-based retention that drops records no checkpoint
  covers. Kafka trims by `deleteRecords` through a checkpointed offset.
- `putCheckpoint` is create-if-absent per position, so concurrent compactors
  write the same bytes or nothing (I11).
- `maxRecordBytes`: the largest record it stores (Kafka's `max.message.bytes`,
  SQS's message size). Such a log cannot replace itself, so the commit loop
  rejects a larger record before sending it (`TOO_LARGE`).
- A producer's own deduplication (Kafka's idempotent producer, SQS's
  deduplication id) only saves copies: replay's committed set dedups records
  whatever the producer does.
- SQS is not a log: a message received is gone, and retention is at most 14
  days. The archiver is the log: it receives in order, appends to an archive
  (an object store), and is the one compactor.

## Testing a provider

- `SharedLogContract` (intentlog-testkit) holds a provider to P1, P3, P4, P5
  and I11, to creating a log once, to `readAfter`, and to positions (a tail
  names where the log ends; a read says where its records start, after the
  position its checkpoint covers; records read after a tail say they follow
  it), as far as its capabilities make them testable. `TestAtomicFileLogContract` and
  `TestInMemorySharedLogContract` run it.
- `CommitProtocol` (intentlog-testkit) fuzzes the commit loop on a
  `LogHarness`, over `EntityVersionsMachine`, with the TLA+ models' schedules:
  writers whose requests meet faults (failures, lost replies, throttles,
  delayed appends, failed and stale reads, reads that break P3, reads with a
  gap) and inside
  whose commits other writers commit, compactors and GC records inside
  commits, and copies of any record that ever landed, which must never apply.
  It judges each commit as it ends, the state after each copy, and, once
  every delayed append has landed, the whole run: each outcome again, no
  commit applied twice, the state as the serial execution of what applied,
  and every checkpoint as the replay of the log through it; and every replica
  a read or a commit returns as a state the log held (`LogHarness.held`).
  `TestMemoryCommitProtocol` and `TestAtomicFileCommitProtocol` run it. Each
  of the shared-log model's bug switches, put back in `LogRuntime`, fails it
  on the memory log.
- `SharedLogCommit.tla` ([spec/tla/README.md](../spec/tla/README.md)) checks
  the protocol on a shared log with separate checkpoints, with lagging reads,
  compactors and every fault.
