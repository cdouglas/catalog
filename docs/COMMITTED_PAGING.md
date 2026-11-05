# Paging the committed set (design)

Status: proposed 2026-10-28, revised 2026-11-04 after review; stages 1-3
built on branch `committed-paging` (2026-11-04), stage 4 (the fuzzers and the
measurements) open.
Errata D16 tracks the problem, and D21 the stamps it leaves unbounded.

## Context

The committed set ([LOG.md](LOG.md) §"The committed set and GC")
holds every id replay applied within the retention, about 11 B an id, and every
checkpoint carries all of it. With the retention at 30 minutes (since
2026-10-28; it was 6 hours), that is 2 MB at 100 commits a second and 20 MB at
1,000. Replay builds the set in O(added) since 2db98d7, but each compaction
still writes the whole set and each fresh read still fetches it.

**What the ids are.** A record's id is its commit's id. The runtime mints it
when a commit starts (`LogRuntime.Commit`), once the writer has staged its
transaction on a read: after the job's work and Iceberg's `apply`, not when
the job starts. It is a UUIDv7 stamped max(clock, H + 1), so a writer whose
clock is behind still mints above the horizon it read (Bug 64). Every send of
the commit carries that id. A commit whose record is rejected ends there, and
Iceberg's retry is a new commit with a new id. The id is internal to the log;
Iceberg never sees it. An id therefore lands at most one commit's duration
after its stamp, usually seconds.

The set does two jobs with these ids:

- **Exactly once.** Replay applies a record at most once. A record can land
  twice: a throttled unconditional send may have landed before it was sent
  again, and a shared log may deliver a copy late. This holds at any horizon,
  since replay rejects a record at or below it.
- **The outcome.** A writer asks whether its record applied: while it
  commits, and now and then long after (LOG.md, the tri-state lookup). Only
  this job depends on how long the ids are kept.

Most of the set is never consulted. Replay asks about a record's id, which is
almost always a few seconds old. A writer asks about its own id while it
commits. The old ids are kept only for a copy of an old record that arrives
late, and for a writer that asks long after its commit. This design moves the
old ids out of the checkpoint into immutable objects, called pages, which the
checkpoint names by node and digest. Replay never reads a page, and lookups
stay exact for the whole retention.

## Two horizons

Today one horizon H (`highest_dropped_timestamp_ms`) does two jobs:

- **Admission.** Replay applies a record only if its id is above H and the set
  does not hold it (`mayApply`).
- **Knowledge.** `contains(q)` is exact above H and UNKNOWN at or below it.

**How H moves.** Only a GC record moves H (LOG.md). Every replace
(`[checkpoint][gc][record]`) and every `compact()` writes `AdvanceHorizon(w)`,
w being its writer's clock less the retention, capped below its own id's
stamp (Bug 64). Replay drops the ids at or below w and raises H to the newest
stamp it dropped. So H is always a stamp some writer minted, never a clock
reading, but how far it moves follows the compactor's clock:

- A compactor whose clock is ahead by more than the retention drops the ids
  of commits still in flight. They are rejected, or unknown if a write may
  have landed.
- Nothing bounds a stamp from above. A record stamped far ahead stays in the
  set until the clocks reach it, and can carry H along (errata D21).

Neither breaks exactly-once; both cost retention and liveness.

Paging splits them into two values, with H ≤ F:

| | Range | Where the ids are | Replay (`mayApply`) | Lookup (`contains`) |
|---|---|---|---|---|
| inline | above F, the **floor** | blocks in the checkpoint | applies if the set does not hold the id | exact, in memory |
| paged | (H, F] | pages the checkpoint names | rejects, without looking | exact, by fetching the page |
| dropped | at or below H, the **horizon** | nowhere | rejects | UNKNOWN |

Replay admits only ids above F, so it never needs a page. A record whose id is
at or below F is one of two things:

- a copy of a record that already applied, which rejecting is right for;
- a commit that took longer than the admission window to land.

The rules the horizon has today cover both, applied to F instead of H.

A writer that asks about an id in (H, F] gets an exact answer from the page:
- If its record applied before the floor passed it, the page holds the id: COMMITTED.
- If not, its record can never apply again: ABSENT, which is final.

With pages, then, a commit loop past the floor learns COMMITTED or REJECTED. Today, past the horizon, it learns BEYOND_HORIZON or unknown.

Two windows follow:

- **The admission window A = now − F.** It sizes the checkpoint: about rate × A
  ids inline. A commit slower than A has its record rejected and retries with
  a new id: a spurious failure, never a wrong answer. The commit loop bounds a
  commit's sends: 10 attempts, each with at most 8 throttled sends backing off
  up to 5 s, and a race backoff up to 1 s. A therefore needs to cover realistic commits,
  not the pathological worst case. Proposed default: 5 minutes (provisional).
  Since overrunning it costs only a retry, A may also follow the commit
  durations the runtime observes (errata D14).
- **The retention R = now − H.** It is how long a writer can learn its outcome
  exactly. Default: 30 minutes, the retention today
  (`fileio.catalog.committed.retention.ms`). Pages live in storage, not in
  checkpoints, so R can grow cheaply: at 1,000 commits a second, 6 hours of
  pages is 238 MB of objects but only 330-660 references (13-26 KB) in the
  checkpoint.

The experiments sweep both (the ingest benchmark, [Testing](#testing)).

**Closed or open admission.** The alternative keeps pages open to admission:
replay fetches the page when a record's id falls in it. That keeps today's
admission window of R, but it puts storage reads inside replay:
- every replayer of the log fetches the page;
- a read can fail on a page;
- replay's determinism comes to rest on the store.

The records it would serve are copies, which are rejected either way, and
commits slower than A, which only retry. Closed admission costs those slow commits a retry
and keeps replay a pure function of the log.

## Blocks: the index

The inline set is cut into **blocks** at deterministic index points. The same
blocks are the unit of paging.

**The grid.** A timestamp is 48 bits of ms. A node (k, a) is the aligned span
[a·2^k, (a+1)·2^k) at level k, as in a binary trie over the timestamps.

**The rule.** The blocks are the maximal nodes that hold at most N of the
inline ids, among the nodes that hold any. A block holds only the ids above F,
and its node may reach below F.

The blocks are a function of the inline ids alone, not of the order in which
replay added them or of where its builds fell. That matters: a replica that
builds after every record and one that builds after 64 must write the same
checkpoint (I11). A rule that opened blocks as ids arrived, and never merged
them, would not have that property. A decoder can check a checkpoint's blocks
against the rule. Maintaining it:

- **An add never merges blocks.** A block is maximal, so its parent holds more
  than N ids. A new id outside every block therefore opens the largest node
  that holds it and overlaps no block. Any larger node contains some block's
  parent.
- **A block that passes N ids splits.** It is replaced by the maximal nodes
  inside it that hold at most N.
- **A removal can merge blocks.** GC and paging remove ids only at the old
  end, so a merge can only join the oldest blocks with their newer neighbors,
  once their common ancestor holds N or fewer. The merged block's bytes are the
  newer block's and then the older's, with two gaps re-encoded: the first, from
  the new node's top, and the one where the two join. At a steady rate, two blocks of N/2 to N ids rarely fit
  together. Merges follow the sparse blocks a GC leaves.

**The encoding.** Each block lists its entries newest first. Each entry is its
gap in ms from the one before and its 10 random bytes, as today; the first gap
is from the node's top, (a+1)·2^k − 1. A block's bytes therefore depend on
nothing but its node and its ids:
- a change to one block leaves the others' bytes as they were;
- a checkpoint encodes the set by copying each block's cached bytes;
- a page's body is the block's bytes, verbatim.

In memory, a block keeps its entries as runs (today's `CommittedSet.Run`), so
the newest block takes replay's adds in O(added), as the set does now.

**Why this rule.**

- *Deterministic, with no clock or estimate.* The blocks are a function of the
  inline ids. Any two replicas at a position agree, and so do any two compactors
  that read the same position. They propose the same page, with the same bytes,
  at the same name.
- *Density-adaptive by count.* N bounds a block's ids, so blocks are narrow
  where ids are dense and wide where they are sparse. No rate is estimated, and
  so there is no window for two estimators to disagree over. The rule reacts within one block: a burst splits
  the block it lands in, and a lull leaves a wide one. At a steady rate r, a
  block spans the largest power of two at most N/r and holds N/2 to N ids.
  With N = 2^16:

  | Rate | Block span |
  |---|---|
  | 1,000 a second | 33-65 s |
  | 100 a second | 8.7 min |
  | 1 a second | one block: the 30-minute set is 1,800 ids, under N, so nothing pages |

- *Pivots are grid points, not ids.* A boundary is a multiple of a power of two,
  fixed by the grid, so no id's timestamp is ever a pivot. A late id lands in
  the block whose node holds it. It can split that block, but it moves no
  boundary.
- *Stable in the past.* Adds only split blocks, and removals reach only the
  oldest blocks. So a block wholly in the past changes only when a late id
  lands in it, or when a removal below it merges it.
- *Cheap.*
  - An id finds its block by a search over the blocks' starts.
  - Almost every add goes to the newest block.
  - A split costs O(N) once per N/2 or so adds.
  - A checkpoint copies blocks.

**Late ids, and sealed blocks.** Ids arrive nearly in the order of their
stamps. An id is stamped when its commit starts, so it lands at most one
commit's duration late; a job that runs for hours stamps its commit when it
commits, not when it starts. A late id lands in the block whose node holds it,
above F. A sealed block never gains an id: replay rejects every id at or below
F without looking, so the late commit fails (`BEYOND_FLOOR`) and Iceberg's
retry commits it under a new id, in the newest block. A compactor seals a
block only once its top is A old ([Compaction](#compaction-when-and-how-to-page)),
so only a commit slower than A meets a sealed block.

**N = 2^16 ids** (about 720 KB) is a constant of the envelope format; changing it
changes the reader version.
- A block holds between about N/2 and N ids once it splits, so a page is
  360-720 KB. An object-store GET of that size is dominated by latency (tens of
  ms).
- A page's reference costs about 40 B, so references stay near 0.01% of the
  ids they replace.

## Paging out: the log record

A `PageOut` record does two things:

- **It seals.** It raises the floor to the block's top, so no id at or below
  it applies again. A block's bytes are a function of its ids, so every
  compactor at one position proposes the same page, under the same name.
- **It attests that the page exists.** Its writer puts the page before it
  sends the record, as Iceberg writes a metadata file before it swaps a
  table's pointer. A landed PageOut names a page that was written, until a
  GC record drops it.

So no second record confirms a page, and no reader looks for a page or
writes one. The record carries no location: a page's name is its node and
digest, and the provider places it ([Page objects](#compaction-when-and-how-to-page)).

```protobuf
message LogRecord {
  bytes id = 1;
  oneof body {
    bytes          intention       = 2;
    AdvanceHorizon advance_horizon = 3;
    PageOut        page_out        = 4;   // no id, like a GC record
  }
}

// Seals the oldest inline block into its page, written before this record, and
// raises the floor to the block's top. Replay applies it only if the block is
// still what its writer paged; otherwise it changes nothing. Applying it twice
// changes nothing.
message PageOut {
  uint32  level  = 1;   // the block's node
  uint64  index  = 2;
  uint32  count  = 3;   // its ids
  bytes   digest = 4;   // SHA-256 of the block's bytes (node and entries), 16 bytes
}
```

Replay of `PageOut(k, a, count, digest)`:

```
function pageOut(set, p):
    b = set.oldestBlock()
    if b is None or b.node != (p.level, p.index): return        # paged since, split, or gone
    if b.top >= set.newestTimestamp(): return                  # never the newest block
    if b.count != p.count or digest(b.bytes) != p.digest: return   # a late id or a GC since
    set.blocks.remove(b)                                       # then merges, as the rule says
    set.pages.append(Page(node = b.node, count = b.count, maxTs = b.maxTs,
                          digest = p.digest))                  # its floor: b.top
    set.floor = b.top
```

Each check is a function of the set where the record lands, so every replayer
reaches the same set (I11). The checks hold the record to the page its writer
wrote:
- If the block changed between the writer's read and the record's landing
  (a late id landed in it, it split, GC cut it, or another PageOut sealed it),
  the record changes nothing. Its page is an orphan (below).
- The newest block cannot be sealed. Otherwise a writer's fast clock, or a
  bug, could raise the floor past the ids being minted now. Since every other
  block ends below the newest id, F never passes an id the log has seen.

**Ids that land between the page and its record.** A compactor reads the log
at some position p, puts the page of the oldest block as it is at p, and sends
the PageOut, which lands at a later position q. Other records land between p
and q:

- One whose id falls in the paged block (a commit slower than A) applies,
  since the floor has not risen yet. At q the block's count and digest no
  longer match, so the PageOut changes nothing: the id stays inline with the
  rest of the block, and the page is an orphan. No one re-issues it as such.
  The next compaction that takes the block pages it as it then is, under
  another digest and so another name.
- One whose id falls in the block but lands after q is at or below the floor.
  Replay rejects it, and its writer's lookup finds the page without it:
  ABSENT, which is final.
- Ids in newer blocks are not affected.

A copy of a record can also land after its id was paged, on one object too. A
writer whose append lost a race probes the object, and a replace that made it
longer reads as an append (`AtomicFileLog`: a tail continues another if the
object did not shrink). The writer then resends the same record without
reading. If that replace paged the record's id, the copy lands below the
floor, where replay rejects it without looking, and the writer's lookup finds
the original in the page. Only the floor keeps replay from judging the copy:
the TLA+ switch `floorgap` (admission above H) reports such a commit failed
although its first copy applied.

So a page holds every id that applied in its range, and none that did not.

The pages tile (H, F] in the order their records landed: page i holds the ids in (F_{i−1}, F_i].

## GC with pages

`AdvanceHorizon(w)`:
1. drops every page whose floor is at or below w, raising H to each one's `maxTs`;
2. drops or cuts the inline blocks at w, as today, then merges as the rule
   says. This is only reached when w ≥ F, for example with a retention below A,
   as the fuzzers' retention-0 writers have;
3. sets F = max(F, H).

A page that straddles w is kept whole, so the ids it holds below w stay known.
Replay never rewrites a page.

H rises to a dropped page's newest id, not to its top. An id stamped above
that id and at or below the top lies in (H, F] but in no page: nothing applied
there, and nothing can (below).

## Lookups and the commit loop

```
contains(q):                         # tri-state, as today
    if q > F:           return inline.holds(q)
    if q > H:
        p = the oldest page whose top is at or above q
        if p is None or q > p.maxTs:
            return false                                  # above a page's newest id
        throw PagedOut(p)                                 # the runtime fetches it
    throw DedupHorizonExceeded
mayApply(q) = q > F and not inline.holds(q)               # replay never fetches
```

An id in (H, F] above every page lies above the newest id of a page GC
dropped, at or below its top. No record with such an id applied, and none can,
so the set answers absent without a page (found by the TLA+ model,
spec/tla/README.md "Paging the committed set"). The same holds for an id above
the newest id of the page whose range holds it, which the reference carries
(`max_timestamp_ms`): the set answers absent without fetching it. So a page
that is missing when a lookup fetches it was dropped by a GC that raised H
past the id, and reading again finds the id unknown.

- **Minting**, after the horizon a writer read (LOG.md), mints after F: `newUuidV7After(max(H, F))`.
  A writer whose clock is behind still commits.
- **A replace's write**, whose GC record stays below its own id (LOG.md), keeps its PageOuts below its own id, as its
  GC record is.
- **`landed`** (Rule 4) catches `PagedOut`, fetches the page through the
  provider's page store, and answers from it. The runtime caches pages by name,
  since they never change. A get that fails fails the lookup; it is never read
  as "absent". The lookup then falls under the failure rules as any read does
  (LOG.md §"Failures"): unknown if a write may have landed, else rethrown. A
  page that does not exist is another case: GC dropped it and its compactor
  deleted it, so the replica is stale, and the runtime reads again
  (Deletion, below).
- **Callers outside the commit loop** (the tri-state lookup, LOG.md) ask the
  runtime, which resolves pages the same way. `CommittedSet.contains` alone
  throws `PagedOut`, since the set holds references, not pages.
- **Exceeding A.** An id at or below F whose record is not in its page, or
  that no page holds, is REJECTED (`BEYOND_FLOOR`; the catalog throws `CommitFailedException`), since
  replay rejects that record from then on.
- **P3's caveat goes away within R.** P3 notes that a view whose checkpoint
  already holds the record cannot say whether it applied once GC drops the id.
  Within R, the page now says.

**Readers that never look.** A reader that does not look ids up, such as a
query engine loading tables, holds the inline blocks (replay needs them) and
the references, and fetches no page. Paging is the mode that leaves the old
ids out. Only a service wants every id of a long window, and it keeps its own
store outside the log. The REST spec's `Idempotency-Key` is one case: a UUIDv7
reused for one operation's retries over an advertised lifetime
(`idempotency-key-lifetime`, such as 30 minutes or 24 hours). The server must
answer a retry with the operation's final response, a 409 included, which the
committed set does not record. Iceberg's own writers recover from the table
instead: Flink keeps `flink.max-committed-checkpoint-id` and Kafka Connect
`kafka.connect.commit-id` in the summaries of the snapshots they commit.

## Compaction: when and how to page

Paging is a policy of the compactor, not a protocol rule: replay accepts any
PageOut that passes its checks. Before it writes, a compactor:

1. applies its own GC to its view, so it does not page ids about to be dropped.
   It therefore pages only ids above now − R, and pages nothing unless the
   retention exceeds the admission window (R > A);
2. while the inline set exceeds T (1 MiB), takes the oldest block whose top is
   at or below its ceiling, min(now − A, timestamp(own id) − 1);
3. for each block taken, writes the page (below), then adds the
   `PageOut(node, count, digest)` to its write.

The PageOuts go into the write as follows:

- **One object** (`AtomicFileLog`): the write is `[checkpoint][gc][page-outs][record]`.
  - The checkpoint is the replay of the version it replaces, still holding the
    blocks, as I11 and `CheckpointRoundTrip` require.
  - The next compaction's checkpoint omits them, one compaction later (100
    records at the default count).
  - On CAS-only stores, every commit is such a write.
- **Checkpoints apart** (`compact()`): the steps become
  1. append the GC record;
  2. append the PageOuts;
  3. `readThrough` and replay;
  4. `putCheckpoint`, which omits the paged blocks;
  5. `trim`.

  Each step is safe to repeat or abandon, as now. A PageOut that lands twice
  changes nothing the second time.

**Page objects.**

- **Name.** A page is named by its node and digest,
  `<level>-<index>-<digest hex>`. The format carries no location: the
  provider's page store places a name, `AtomicFileLog` at
  `<log location>.committed/<name>`, so a log that moves takes its pages along
  under the same names.
  - Every compactor that seals the same block writes the same bytes to the
    same name, whatever position it read, so racing compactors' PUTs agree.
    The body is therefore a function of the log and the block alone: one
    block can be sealed from two floors (a PageOut of the block before it
    raises F to that block's top, a GC that drops the block before it raises F
    only to its newest id), so a body that recorded the floor below it would
    differ under one name (found by `TestCommittedSetBlocks` in stage 3).
  - Different contents never share a name, so a page cannot be overwritten
    with bytes its record does not describe.
- **Body.**
  `CommittedPage { min_reader_version, log_id, level, index, packed_entries }`.
  - A reader checks the log id, the node and the digest, and fails the lookup
    on a mismatch.
  - The references give the range of ids it holds: from the top of the page
    before it to its own top.
- **Deletion.**
  - A page may be deleted once a GC record that drops it has landed. A reader
    still holding an older replica then finds the page missing, reads again,
    and finds the id at or below H (UNKNOWN, as it would have been) or above
    every page (absent, as in Lookups). In a correct log only a stale replica
    names a missing page. Giving up instead would report UNKNOWN; that is not
    wrong, but it is less than reading again finds for an id above the page's
    newest (the TLA+ switch `missingfail` breaks only `UnknownExplained`).
  - The compactor whose write drops a page deletes it, best effort, after its
    write lands.
- **Orphans.** These are pages whose PageOut was refused, never sent, or lost
  in a crash. They are swept by one rule: an unreferenced page whose node ends
  at or below the current floor can never become referenced, since a PageOut
  must name the oldest block above the floor. A sweep (any compactor, now and
  then) lists the prefix and deletes those.
  - It never deletes a page whose record may still land, because such a page
    is above the floor.

**Pages hold commit ids only.** Ids and a table's snapshots differ in lifetime
and in key. Ids live the retention, catalog-wide, ordered by time. Snapshots
live until expiry, or as long as a tag holds them, per table, ordered by
sequence number. A page of both would be held by its longest-lived entry, so
they page into separate objects, and snapshots get a design of their own
(errata D18; CHECKPOINT_PLAN.md R2). Ids are not grouped by table: the log
does not know what a record touched, since intentlog-core holds nothing of
Iceberg. The table's history serves that purpose instead. A data commit's
snapshot id found in any table it touched proves the whole transaction applied
(LOG.md, the writer's second oracle), for as long as the table keeps the
snapshot.

**The SPI.** `SharedLog` gains `default PageStore pages()`, null when the
provider stores no pages; the runtime then never pages. `PageStore` has
`put(name, bytes)`, `get(name)`, `delete(name)` and `list()`, by name.
- `AtomicFileLog` implements it on its FileIO.
- `InMemorySharedLog` implements it on a map, with hooks for the fuzzers: a
  page missing, a get that fails, a put that lands but whose reply is lost.

## Why it is safe

- **No record applies twice.** `mayApply` admits only ids above F, where the set
  is exact in memory. F only rises, and it rises only by sealing ids the set
  holds.
- **No commit is misreported.**
  - A lookup answers from memory above F; from a page whose digest replay
    checked against the set it sealed, in (H, F]; or UNKNOWN at or below H.
  - A page that cannot be read is an error, never "absent".
  - An id in (H, F] that its page lacks cannot apply again, so ABSENT is final.
- **Every checkpoint is the replay of the log (I11).** The blocks are a
  function of the inline ids. Those ids, the pages, F and H are functions of the
  records, in order. PageOut's checks read only the set where it lands.
- **Pages are never wrong, only missing.** A page's name and body carry its
  digest, and replay seals only a block whose bytes have that digest.

## Wire format

```protobuf
message CommittedTransactions {
  reserved 1, 2;                               // max_timestamp_ms, packed_entries: one blob before
  fixed64 highest_dropped_timestamp_ms = 3;    // H, as now
  fixed64 floor_ms                     = 4;    // F; absent: H
  repeated CommittedPageRef pages      = 5;    // (H, F], oldest first
  repeated CommittedBlock blocks       = 6;    // above F, oldest first
}

message CommittedBlock {
  uint32 level          = 1;
  uint64 index          = 2;
  bytes  packed_entries = 3;   // newest first; the first gap from the node's top
}

message CommittedPageRef {
  uint32  level            = 1;   // the sealed block's node: its top is the floor
  uint64  index            = 2;   //   the page raised, the newest timestamp it holds
  uint32  count            = 3;
  fixed64 max_timestamp_ms = 4;   // its newest id's: the horizon when GC drops it
  bytes   digest           = 5;   // with the node, the page's name
}

message CommittedPage {           // a function of the log and the block alone
  int32   min_reader_version = 1;
  bytes   log_id             = 2;
  uint32  level              = 3;
  uint64  index              = 4;
  bytes   packed_entries     = 5;
}
```

The envelope's reader version becomes 2. There are no readers of version 1 to
keep (no active users). The format explorer decodes the new messages.

## Sizes

At a retention of 30 minutes and 11 B an id. The paged inline figures are what
the policy keeps: ids within A, up to one block more (≤ 720 KB) where the line
at A falls inside a block, and no paging below T = 1 MiB.

| Commits/s | Set | Checkpoint today | Inline, A = 5 min | Inline, A = 1 min | Pages |
|---|---|---|---|---|---|
| 1 | 1,800 ids | 20 KB | 20 KB (never pages) | 20 KB | none |
| 10 | 18,000 | 200 KB | 200 KB (never pages) | 200 KB | none |
| 100 | 180,000 | 2 MB | about 1 MB (pages down to T) | about 1 MB | 1-3 |
| 1,000 | 1.8 M | 20 MB | 3.3-4 MB | 0.7-1.4 MB | about 25-50 |
| 10,000 | 18 M | 200 MB | 33 MB | 6.6-7.3 MB | about 250-550 |

At 1,000 a second, about one page is written every 33-65 s. A compaction copies
the inline blocks' cached bytes and the references. A fresh read fetches the
checkpoint without the pages.

## What changes in the code

- **`CommittedSet`** (intentlog-core):
  - blocks on the grid, each of runs, with cached bytes and digest;
  - pages (references only), F and H;
  - `pageOut` and the new `dropThrough`;
  - `contains` throws `PagedOut`, and `mayApply` uses F.
- **`Replay`**: applies `PageOut` as it applies `AdvanceHorizon`: wherever it
  lands, with no id, not counted as an intention record.
- **`LogEnvelope`**: the record and the set's new fields, at reader version 2.
- **`LogRuntime`**:
  - minting after F, from `RuntimeConfig`'s clock (today it mints from the
    system clock, which a test cannot set: [Testing](#testing) draws stamps);
  - `landed` resolving `PagedOut` through `pages()`, with a cache;
  - the paging policy in both compactions;
  - deletion after GC, and the sweep;
  - in `RuntimeConfig`, `admissionWindowMs` (A) and `pageThresholdBytes` (T),
    which become `fileio.catalog.committed.admission.ms` and
    `fileio.catalog.committed.page.threshold` in `LogCatalogProperties`.
- **Providers**: `PageStore` for `AtomicFileLog` (FileIO) and
  `InMemorySharedLog` (a map with fault hooks).
- **The testkit** (`LogHarness`, `CommitProtocol`): page-store faults, a
  compaction split around its PageOut, drawn stamps, and the page oracles
  ([Testing](#testing)).

## Testing

The cases that matter sit at the marks: ids just above and below F and H,
ids inside the block a PageOut is sealing while it is in flight, and pages
that lose a race. Writers minting from a clock reach them only by chance, so
the tests draw stamps against the marks instead.

**Stamps drawn against the marks.** A generator draws each id's stamp from
regions defined by the log it will land in. Each region's expected outcome is
a function of the marks where the record lands, which the reference model
computes:

| Region of the stamp | Replay | The writer's lookup |
|---|---|---|
| above the newest id (the usual case) | applies | COMMITTED |
| in the same ms as another id | applies (the random bits differ) | COMMITTED |
| inside the newest block, below the newest id | applies, may split the block | COMMITTED |
| inside an older inline block, above F | applies | COMMITTED |
| inside the block an in-flight PageOut seals | applies if it lands first, and the PageOut then changes nothing | COMMITTED |
| at F, or in (H, F] | rejected | ABSENT, from the page, final |
| at H, or below it | rejected | REJECTED if no write of it may have landed, else UNKNOWN |
| a copy of an id that applied, in any region | rejected | COMMITTED, the original |
| far ahead of every clock (errata D21) | applies | COMMITTED |

A writer mints from the drawn stamp: the runtime mints from `RuntimeConfig`'s
clock, and the test sets it just before the writer stages. The generator is
there to drive the paging code through its edges, and is built with it; it
runs against today's horizon too. It does not address D21: the last row checks
only that a stamp ahead breaks no oracle.

**The set against a reference.** A walk of adds, builds, GCs, PageOuts and
decodes against a sorted set with pages, as `TestCommittedSetRuns` does for
runs. Adds draw their stamps by region, and a PageOut is computed on an
earlier version of the set than the one it is applied to, so stale PageOuts
are the norm. It checks:
- the blocks are the rule's: the maximal nodes holding at most N of the ids
  above F;
- the same bytes from any history: decoded, folded, replayed, or built after
  every record or after batches;
- a PageOut applies exactly when the reference's block is still the one its
  page holds (node, count, digest), and otherwise changes nothing;
- admission and lookups (tri-state, with pages) agree with the reference.

**The commit-protocol fuzzers** (`TestMemoryCommitProtocol`,
`TestAtomicFileCommitProtocol`) gain compactors that page with A = 0 and a
retention above it (a compactor at retention 0 pages nothing: its own GC drops
what it would page; GC writers at retention 0 still cut blocks), writers whose stamps are drawn as above, and a
compaction split around its PageOut. The compactor reads at a position p and
puts its pages. The schedule then runs other steps before the PageOut lands
at a later position q: commits with stamps drawn into the paged block, GC
records that cut it, another compactor's PageOut of the same block (the same
page, put again), and duplicates. Or the PageOut never lands: the compactor
crashes after the put. They also inject:
- copies of records whose ids are paged;
- pages that are missing, whose get fails, whose put lands with its reply
  lost, or that are deleted while a writer looks;
- sweeps at any step.

The oracles, beside today's (no copy applies, and no outcome is
misreported):
1. **A PageOut whose block changed between p and q changes nothing.** The
   state, the set, F and H are equal before and after it, as for a duplicate.
2. **A landed PageOut's page exists**, with its digest, until a GC record
   drops it, whatever sweeps ran before or after it landed, and it holds
   exactly the ids that applied in its range. A sweep cannot break this: a
   PageOut can only apply while its block is above the floor, and the sweep
   deletes nothing above it.
3. **A sweep finds every orphan it may delete, and only those.** Against the
   view it acted on, it deletes every unreferenced page whose node ends at or
   below the floor, and no other page. An orphan above the floor (a refused
   PageOut's, whose block is still inline) stays until the floor passes it.
4. **Orphans are collected.** At the drain, once every delayed append and
   PageOut has landed, a final GC at the newest stamp (retention 0) raises H
   and F past every page, and a sweep leaves the page store empty. Every
   orphan, from a refused PageOut, a split block, a GC cut or a crash after
   the put, is gone.

**TLA+** (done, stage 2). `SharedLogCommit.tla` and `CatalogCommit.tla` carry
the floor, written before the Java, as the shared log was. The floor is
admission and the horizon is knowledge. PageOut is an action, and so are the
page's put, which comes first, and the sweep. Two properties are new:
`PagesExist` (oracle 2's first half) and `PagesExact` (every page holds
exactly the ids that applied in its range). Each bug switch breaks a property
(spec/tla/README.md, "Paging the committed set"):
- replay admits ids at or below F: on a shared log, a late id applies below
  the floor and its page lacks it (`PagesExact`), and a copy of a blind append
  applies twice (`ExactlyOnce`); on one object, a copy is judged instead of
  being found in its page (`HonestFailed`);
- a missing page reads as absent: a misreport (`HonestFailed`);
- PageOut without the digest check: a page that lacks an id its record sealed
  (`PagesExact`). On one object it cannot happen, since a CAS lands only on the
  version its PageOuts were computed on;
- the sweep deletes an unreferenced page whose node ends above the floor: a
  PageOut in flight then names a missing page (`PagesExist`);
- minting below F: spurious rejections (`SoloCommits`; on one object, a CAS
  whose own record is rejected);
- on one object, a CAS's PageOuts not kept below its own record: the CAS
  rejects its own record (`CasAppliesItsRecord`).

The model's oracle matrix (spec/tla/README.md, "The oracles, with paging")
asks four things of the fuzzers in stage 4:
- `CheckpointRoundTrip` checks on every write that each page the catalog
  references exists and holds exactly the ids that applied in its range. Replay
  never reads a page, so without it the catalog fuzzers miss a sweep that
  deletes a page in use;
- its rule that a compacting commit's own record applies if it holds also
  covers writes that carry PageOuts. The catalog fuzzers' oracles miss a CAS
  whose PageOuts pass its own record: the commit was raced, so no solo rule
  applies;
- the shared log's fuzzers assert that a commit nothing raced, on a log that
  did not fail, commits. Without it, minting below the floor, which costs only
  spurious rejections, passes them;
- the race property gets schedules that page: with one operation per writer,
  nothing pages.

**The catalog fuzzers** run writers with a small A and a retention above it, as they run some with
retention 0, and writers whose clocks are offset by amounts drawn from the
same regions. `CheckpointRoundTrip` already checks that a checkpoint is the
replay.

- **The ingest benchmark.**
  - First, a mode that models paging (ids within A inline, the rest discarded
    and counted as pages) measures the gain before the store side is built, as
    `committed = discard` did for the set.
  - Then a campaign with paging on, at 1,000 a second's steady state.

## Order of work

Each stage keeps every suite and fuzzer green, and lands on the branch
`committed-paging`:

1. **The set in blocks** (reader version 2; F is written but equals H, so
   nothing pages yet): `CommittedSet` in blocks of runs, `CommittedSetCodec`
   as the whole-set reference, minting after F from the runtime's clock, and
   `TestCommittedSetBlocks` (walks at N from 1 to 2^16 with stamps drawn by
   region, against the reference and the block rule, calibrated by mutants).
   Every suite runs with N = 16 (surefire's `intentlog.committed.block.ids`),
   so the catalog's fuzzers split and merge blocks throughout. The format
   explorer decodes blocks. `CommittedSetBench` (JMH) compares decode, fold,
   encode, GC and lookups with the one-blob set, as does `IngestFoldBench`.
   No behaviour changes.

   Measured on the dev box (2026-11-04, `CommittedSetBench`, sets of 180,000
   and 1.8 million ids over 30 minutes, the one-blob set first): a lookup of an
   old id 345 µs and 5.3 ms before, 76 and 7 µs after (its block, not the
   whole set, is scanned); of an absent id 232 µs and 2.2 ms before, 6 and
   62 µs after; a GC and the checkpoint after it 1.2 and 12.4 ms before, 0.9
   and 4.7 ms after; a fold of 64 ids 6.6 and 6.4 µs before, 7.4 and 9.4 µs
   after; the checkpoint after a fold 0.28 and 4.3 ms before, 0.30 and 4.7
   ms after (within the run-to-run error); a decode 0.3 and 2.3 µs before,
   0.6 and 4.5 µs after. A fold first scanned the decoded newest block to
   count it (88-112 µs) until a block's bytes bounded its count (every entry
   takes 11 bytes or more). `IngestFoldBench` (the i1 capture at 1,000
   records a second, the set at its steady state) showed no difference
   beyond the dev box's run-to-run spread, which reached 2x between runs of
   one build.
2. **TLA+.** The floor, PageOut, the page put and the sweep in
   `SharedLogCommit.tla` and `CatalogCommit.tla`, with the bug switches
   above. Done 2026-11-04. The model refined the design in four places, now in
   the text: a lookup above a dropped page's newest id finds no page and is
   absent; a page that does not exist means the replica is stale, so the
   runtime reads again; a compactor pages nothing unless R > A, since it
   applies its own GC first; and on one object a copy can land below the
   floor, where only the floor rejects it.
3. **Paging.** The PageOut record and its replay, page references, the
   `PageStore` SPI and its providers, the runtime's policy (A, T), lookups
   through pages, deletion after GC, and the sweep. Done 2026-11-04:
   - `CommittedSet` holds the references, F and H; `pagedOut` (replay of a
     PageOut) and `sealable` (the page a compactor would write of the oldest
     block); `contains` throws `PagedOutException`.
   - `PageStore` (`SharedLog.pages()`): `AtomicFileLog` on its FileIO beside
     the object (listing needs `SupportsPrefixOperations`; the contention
     benchmark's `LatencyFileIO` lists nothing yet, so its sweeps do nothing),
     `InMemorySharedLog` on a map with page-fault hooks (`PageFault`).
   - `LogRuntime`: the policy in both compactions; a replace computes its
     payload and puts its pages before the write, whose failures are
     ambiguous; `landed` reads pages (`BEYOND_FLOOR` for an id its page lacks,
     a failed get unknown or a failed read, a missing page a fresh read);
     `committed(replica, id)` for lookups outside the commit loop. The writer
     whose GC record dropped pages deletes them once it lands, and sweeps the
     orphans whenever its write dropped or sealed a page: so a sweep follows
     every page written or dropped, at most one a minute at 1,000 commits a
     second. Pages read are cached by name (4), and pages put are remembered
     (64), since a name's bytes never change.
   - `RuntimeConfig.admissionWindowMs`, `pageThresholdBytes`; the catalog's
     `fileio.catalog.committed.admission.ms`, `.page.threshold`.
   - Tests: `TestCommittedSetBlocks` (walks with stale PageOuts against a
     reference with pages; every page referenced exists and holds exactly the
     ids that applied in its range), `TestReplay`, `TestSharedLogPaging`,
     `TestAtomicFileLogPaging`, the page store in `SharedLogContract`, and a
     format explorer scenario. Calibrated by 14 mutants, each caught: the
     TLA+ switches (`floorgap` in the set and in its builder, `absentpage`,
     `nodigest`, `sweepabove`, `mintH`, `pageown`, `missingfail`), and
     sealing the newest block, no merge after a PageOut, GC keeping pages, a
     paged id read as absent, no deletion after GC, and a ceiling without A.
     Sealing the newest block was missed until `theNewestBlockIsNeverSealed`
     (no compactor proposes it, so only a forged PageOut reaches the check).
   - The catalog suites run paged with `-Dintentlog.test.paging=true` (A = 0,
     T = 0, in `LocalCatalogMode`): `TestLocalCatalog*` and the fault and race
     fuzzers pass, but page little (15 and 44 writes carried PageOuts), since
     most of their catalogs hold fewer than two blocks of ids. Stage 4 makes
     them page.
   - Found: the page body's `floor_below` made it depend on the floor it was
     sealed from, not only on its block (Page objects, above); it is gone.
4. **The fuzzers and the measurements.** The commit-protocol fuzzers' split
   compaction, page faults and oracles; the catalog fuzzers with a small A;
   the ingest benchmark's paging mode, then a campaign; a havoc fuzz round and
   a calibration.

## Alternatives considered

- **Pivots chosen by the sealer** (any timestamp, recorded in the PageOut).
  The record would make them deterministic, but concurrent sealers would then
  propose different pages: refused PageOuts and orphans. The inline set would
  also have no stable blocks for a checkpoint to copy. The grid makes every
  sealer propose the same page.
- **Blocks opened as ids arrive and never merged.** This is simpler to
  maintain, but the blocks then depend on the order of adds and on where
  replay's builds fell, so two replicas at one position could write different
  checkpoints (I11).
- **Spans from an estimated rate** (a block's span set from the last block's
  density). This is history- and window-dependent, and a burst overfills a block
  until its span ends. Splitting by count bounds every block.
- **Seal first, then page.** One record raises the floor and freezes the ids,
  and a later one names the page, so no digest is needed. It costs two writes
  and a read between them for every page. Refusals only happen when a late id
  lands in a block the sealer has just read, which is rare, so one record with
  a digest is cheaper. Sealing first and leaving the page to any reader to
  write later is not an option: the ids would leave the checkpoint before a
  page held them, and a crash in between would lose them, unless they stayed
  inline until a second record confirmed the page, which is this alternative.
- **A tree of references** (pages of references, so the checkpoint holds a
  few). One level suffices. A page holds N/2 to N ids, so at 1,000 a second
  the references number 23-46 for 30 minutes (A = 5 minutes) and 330-660 for
  6 hours (13-26 KB). Pages never overlap, since no id lands at or below F,
  so a page never needs an overlay. Snapshot pages may: expiry removes snapshots from sealed history,
  and tags keep old ones (errata D18). A reader version costs nothing while
  there are no users, so a level is added when one is needed.
- **A lossy set** (a filter, truncated ids). It could report a record applied
  that was not, and drop it (errata D16).

## Open questions

- **A's and R's defaults** (5 minutes, provisional, and 30 minutes, as now),
  which the experiments sweep, and whether the runtime should derive A from the
  commit durations it observes (errata D14).
- **Where pages live per provider.** Beside the catalog object for
  `AtomicFileLog`; for Kafka, SQS or a ledger, in the archiver's object store
  (LOG.md §"Providers").
- **The sweep's cadence.** Stage 3 sweeps after every compaction that
  writes or drops a page. Whether a page store should list by prefix or the
  runtime should track the orphans it made is open.
- **How snapshots page** (errata D18): a design of its own,
  [SNAPSHOT_PAGING.md](SNAPSHOT_PAGING.md), under the same rule that replay
  never reads a page. It needs no PageOut: a table's blocks seal by its
  sequence numbers, which are state, not by a clock, and the root keeps each
  sealed block's live ids, so expiry's holes need no rewrite.
- **A bound on stamps** (errata D21). The floor follows the oldest block, not a
  clock, but a compactor's ceiling, now − A, does: one whose clock is far
  ahead can seal every block but the newest.
