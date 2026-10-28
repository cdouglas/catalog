# Paging the committed set (design)

Status: proposed, 2026-10-28; not built. Errata D16 tracks the problem.

## Context

The committed set ([LOG.md](LOG.md) §"The committed set and GC", [GC.md](GC.md))
holds every id replay applied within the retention, about 11 B an id, and every
checkpoint carries all of it. With the retention at 30 minutes (since
2026-10-28; it was 6 hours), that is 2 MB at 100 commits a second and 20 MB at
1,000. Replay builds the set in O(added) since 2db98d7, but each compaction
still writes the whole set and each fresh read still fetches it.

Most of the set is never consulted. Replay asks about a record's id, which is
almost always a few seconds old. A writer asks about its own id while it
commits. The old ids are kept only for a copy of an old record that arrives
late, and for a writer that asks long after its commit. This design moves the
old ids out of the checkpoint into immutable objects, called pages, which the
checkpoint names by URI. Replay never reads a page, and lookups stay exact for
the whole retention.

## Two horizons

Today one horizon H (`highest_dropped_timestamp_ms`) does two jobs:

- **Admission.** Replay applies a record only if its id is above H and the set
  does not hold it (`mayApply`).
- **Knowledge.** `contains(q)` is exact above H and UNKNOWN at or below it.

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
  not the pathological worst case. Proposed default: 5 minutes. Since overrunning
  it costs only a retry, A may also follow the commit durations the runtime
  observes (errata D14).
- **The retention R = now − H.** It is how long a writer can learn its outcome
  exactly. Pages live in storage, not in checkpoints, so R can be long cheaply:
  at 1,000 commits a second, 6 hours of pages is 238 MB of objects but only
  about 330 references (36 KB) in the checkpoint.

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

**N = 2^16 ids** (about 720 KB) is a constant of the envelope format; changing it
changes the reader version.
- A block holds between about N/2 and N ids once it splits, so a page is
  360-720 KB. An object-store GET of that size is dominated by latency (tens of
  ms).
- A page's reference costs about 110 B, so references stay under 0.03% of the
  ids they replace.

## Paging out: the log record

```protobuf
message LogRecord {
  bytes id = 1;
  oneof body {
    bytes          intention       = 2;
    AdvanceHorizon advance_horizon = 3;
    PageOut        page_out        = 4;   // no id, like a GC record
  }
}

// Seals the oldest inline block into the page at `uri`, and raises the floor to
// the block's top. Replay applies it only if the block is still what its writer
// read; otherwise it changes nothing. Applying it twice changes nothing.
message PageOut {
  uint32  level  = 1;   // the block's node
  uint64  index  = 2;
  uint32  count  = 3;   // its ids
  bytes   digest = 4;   // SHA-256 of the block's bytes (node and entries), 16 bytes
  string  uri    = 5;   // the page, written before this record
}
```

Replay of `PageOut(k, a, count, digest, uri)`:

```
function pageOut(set, p):
    b = set.oldestBlock()
    if b is None or b.node != (p.level, p.index): return        # paged since, split, or gone
    if b.top >= set.newestTimestamp(): return                  # never the newest block
    if b.count != p.count or digest(b.bytes) != p.digest: return   # a late id or a GC since
    set.blocks.remove(b)                                       # then merges, as the rule says
    set.pages.append(Page(floor = b.top, count = b.count, maxTs = b.maxTs,
                          digest = p.digest, uri = p.uri))
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

## Lookups and the commit loop

```
contains(q):                         # tri-state, as today
    if q > F:           return inline.holds(q)
    if q > H:           throw PagedOut(page holding q)    # the runtime fetches it
    throw DedupHorizonExceeded
mayApply(q) = q > F and not inline.holds(q)               # replay never fetches
```

- **Minting** (Rule 3 of GC.md) mints after F: `newUuidV7After(max(H, F))`.
  A writer whose clock is behind still commits.
- **A replace's write** (Rule 2) keeps its PageOuts below its own id, as its
  GC record is.
- **`landed`** (Rule 4) catches `PagedOut`, fetches the page through the
  provider's page store, and answers from it. The runtime caches pages by URI,
  since they never change. A page that cannot be fetched fails the lookup; it is
  never read as "absent". The lookup then falls under the failure rules as any
  read does (LOG.md §"Failures"): unknown if a write may have landed, else
  rethrown.
- **Callers outside the commit loop** (GC.md's tri-state lookup) ask the
  runtime, which resolves pages the same way. `CommittedSet.contains` alone
  throws `PagedOut`, since the set holds references, not pages.
- **Exceeding A.** An id at or below F whose record is not in its page is
  REJECTED (`BEYOND_FLOOR`; the catalog throws `CommitFailedException`), since
  replay rejects that record from then on.
- **P3's caveat goes away within R.** P3 notes that a view whose checkpoint
  already holds the record cannot say whether it applied once GC drops the id.
  Within R, the page now says.

## Compaction: when and how to page

Paging is a policy of the compactor, not a protocol rule: replay accepts any
PageOut that passes its checks. Before it writes, a compactor:

1. applies its own GC to its view, so it does not page ids about to be dropped;
2. while the inline set exceeds T (1 MiB), takes the oldest block whose top is
   at or below its ceiling, min(now − A, timestamp(own id) − 1);
3. for each block taken, writes the page (below), then adds the
   `PageOut(node, count, digest, uri)` to its write.

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

- **Name.** A page is named by its content, beside the log:
  `<log location>.committed/<level>-<index>-<digest hex>` on `AtomicFileLog`.
  - Every compactor that reads the same position writes the same bytes to the
    same name, so racing compactors' PUTs agree.
  - Different contents never share a name, so a page cannot be overwritten
    with bytes its record does not describe.
- **Body.**
  `CommittedPage { min_reader_version, log_id, level, index, floor_below, packed_entries }`.
  - A reader checks the log id, the node and the digest, and fails the lookup
    on a mismatch.
  - `floor_below` and the node give the range of ids it holds.
- **Deletion.**
  - A page may be deleted once a GC record that drops it has landed. A reader
    still holding an older replica then fails the lookup, reads again, and
    finds the id at or below H: UNKNOWN, as it would have been.
  - The compactor whose write drops a page deletes it, best effort, after its
    write lands.
- **Orphans.** These are pages whose PageOut was refused, never sent, or lost
  in a crash. They are swept by one rule: an unreferenced page whose node ends
  at or below the current floor can never become referenced, since a PageOut
  must name the oldest block above the floor. A sweep (any compactor, now and
  then) lists the prefix and deletes those.
  - It never deletes a page whose record may still land, because such a page
    is above the floor.

**The SPI.** `SharedLog` gains `default PageStore pages()`, null when the
provider stores no pages; the runtime then never pages. `PageStore` has
`put(name, bytes)` returning a URI, `get(uri)`, `delete(uri)` and `list()`.
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
  repeated CommittedPage.Ref pages     = 5;    // (H, F], oldest first
  repeated Block blocks                = 6;    // above F, oldest first
}

message Block {
  uint32 level          = 1;
  uint64 index          = 2;
  bytes  packed_entries = 3;   // newest first; the first gap from the node's top
}

message CommittedPage {
  message Ref {
    fixed64 floor_ms         = 1;   // the newest timestamp it may hold
    uint32  count            = 2;
    fixed64 max_timestamp_ms = 3;   // its newest id's: the horizon when GC drops it
    bytes   digest           = 4;
    string  uri              = 5;
  }
  int32   min_reader_version = 1;
  bytes   log_id             = 2;
  uint32  level              = 3;
  uint64  index              = 4;
  fixed64 floor_below        = 5;
  bytes   packed_entries     = 6;
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
  - minting after F;
  - `landed` resolving `PagedOut` through `pages()`, with a cache;
  - the paging policy in both compactions;
  - deletion after GC, and the sweep;
  - in `RuntimeConfig`, `admissionWindowMs` (A) and `pageThresholdBytes` (T),
    which become `fileio.catalog.committed.admission.ms` and
    `fileio.catalog.committed.page.threshold` in `LogCatalogProperties`.
- **Providers**: `PageStore` for `AtomicFileLog` (FileIO) and
  `InMemorySharedLog` (a map with fault hooks).

## Testing

- **The set against a reference.** A walk of adds, builds, GCs, PageOuts and
  decodes against a sorted set with pages, as `TestCommittedSetRuns` does for
  runs. It checks:
  - the blocks are the rule's: the maximal nodes holding at most N of the ids
    above F;
  - the same bytes from any history: decoded, folded, replayed, or built after
    every record or after batches;
  - lookups tri-state, with pages.
- **The commit-protocol fuzzers** (`TestMemoryCommitProtocol`,
  `TestAtomicFileCommitProtocol`) gain compactors that page with A = 0 (as GC
  writers run with retention 0), and they also inject:
  - duplicate and stale PageOuts;
  - copies of records whose ids are paged;
  - late ids into paged blocks, which must be rejected with an exact outcome;
  - pages that are missing, fail, or are deleted while a writer looks.

  Their oracles hold as they are: no copy applies, and no outcome is misreported.
- **TLA+.** Write the floor into `SharedLogCommit.tla` and `CatalogCommit.tla`
  before the Java, as the shared log was. The floor is admission and the
  horizon is knowledge, with PageOut as an action. Bug switches each must
  break a property:
  - replay admits ids at or below F (a copy applies);
  - a missing page reads as absent (a misreport);
  - PageOut without the digest check (a page that lacks an id its record sealed: a misreport);
  - minting below F (spurious rejections, liveness).
- **The catalog fuzzers** run writers with a small A, as they run some with
  retention 0. `CheckpointRoundTrip` already checks that a checkpoint is the replay.
- **The ingest benchmark.**
  - First, a mode that models paging (ids within A inline, the rest discarded
    and counted as pages) measures the gain before the store side is built, as
    `committed = discard` did for the set.
  - Then a campaign with paging on, at 1,000 a second's steady state.

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
  a digest is cheaper.
- **A lossy set** (a filter, truncated ids). It could report a record applied
  that was not, and drop it (errata D16).

## Open questions

- **A's default**, and whether the runtime should derive it from the commit
  durations it observes (errata D14).
- **Where pages live per provider.** Beside the catalog object for
  `AtomicFileLog`; for Kafka, SQS or a ledger, in the archiver's object store
  (LOG.md §"Providers").
- **The sweep's cadence**, and whether a page store should list by prefix or
  the runtime should track the orphans it made.
- **Whether old snapshots page the same way** (errata D18). The machinery, a
  floor raised by a record naming an immutable object, would carry over, with
  a snapshot's id in place of the ids' timestamps.
