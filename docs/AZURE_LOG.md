# The Azure log

**Status:** the catalog's log on Azure (`fileio.catalog.log` `azure-append`) is the segmented log
of [LOG_SEGMENTS.md](LOG_SEGMENTS.md), built on branch `log-segments` (2026-11-15): a catalog
object, which holds the checkpoint and installs segments, and segments that are Azure Append
Blobs, appended blind. It replaced the log v1 of 2026-11-10 (a head blob, checkpoint blobs, and
segments closed by a pad), whose measurements below it was built on. The format and protocol,
for review, are [AZURE_LOG_FORMAT.md](AZURE_LOG_FORMAT.md).

Writers append their records to an Append Blob **without a position condition** (blind
appends). The service picks where each record lands and says so in its reply. So no writer ever
loses a race to append. A commit is rejected only when replay finds that its intention no longer
holds where its record landed: the case the conflict encodings exist for ([SPEC.md](SPEC.md)
"Conflict Encodings").

To the runtime the log is a `SharedLog` ([LOG.md](LOG.md)) of blind appends and separate
checkpoints, so everything it needs is the runtime's already: the commit loop's branch for logs
without conditional appends, `compact()` for logs whose checkpoints are stored apart, and the
rules for unconditional appends (LOG.md "Unconditional appends"). This document is what is
particular to Azure: why blind appends, why a segment's end is in the segment, and how the log's
requests map onto Azure's.

## Why: the measurement

For a year the catalog took it as given that several writers can share one appendable
object only by conditioning each append on the length they last saw. That is true of the
one-object log ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)), because a CAS compaction replaces
the object, and of S3 Express, whose append must name the object's current size
(`x-amz-write-offset-bytes`; any other value fails with `InvalidWriteOffset`). It is not true
of an Azure Append Blob. Append Block's position condition (`x-ms-blob-condition-appendpos`)
is optional, and its reply names the offset where the block landed
(`x-ms-blob-append-offset`).

`AzureAppendSpike` (catalog-bench's `spikes` module, docs/AZURE_APPENDS.md; written in rmow-bench,
whose VM it ran on) measured both kinds of append on 2026-11-10:
- **Setup:** one D8as_v6 VM in westus, beside an account with a hierarchical namespace;
  4 KiB blocks; a new blob for each level; each level 2 s of warm-up and then 20 s measured.
- **Retries and connections:** the SDK's retries were off and every writer had its own
  connection.
- **The check:** after each level the blob was read back. Every acknowledged block had to be
  intact (CRC32C), appear once, and sit at the offset its reply named.
- **Where:** the run is catalog-bench's `results/spike/azure-appends-20261110T192545Z` (moved
  from rmow-bench on 2026-11-11), built
  at ee7f046.

| Writers | Blind appends landed/s | Conditional appends landed/s | Conditional appends that lost |
|---:|---:|---:|---:|
| 1 | 94 | 96 | 0% |
| 2 | 192 | 100 | 38% |
| 4 | 341 | 101 | 65% |
| 8 | 640 | 98 | 81% |
| 16 | 1,178 | 98 | 90% |
| 32 | 2,043 | 99 | 95% |
| 64 | 3,113 | 95 | 97% |

The 64-writer blind level filled its blob: it reached the 50,000-block limit 14.1 s into
its 20 s window. Its rate here is over the 14.1 s it ran. The spike's own `levels.jsonl`
says 2,199, because it divided by the full 20 s; that's fixed for later runs. The table is
computed from the run's requests in catalog-bench's lab (`append_requests`).

What the run showed:
- **Conditional appends** land at about one writer's rate (a 10 ms median append), however
  many writers there are. A lost race costs a lost append (8–11 ms median) and a HEAD
  (7 ms), not the 2 s of catalog-bench's `azure` profile.
- **Blind appends** scale with the writers up to 64, with no throttling, no errors and no
  lost appends. Their median latency rose from 10 ms to 16 ms, and their p99 reached 99 ms
  at 64 writers. The client's CPU share stayed at or below 7.4%, so the VM was not the
  limit.
- **The check passed at every level.**
- **One blob is not enough:** at 64 writers a blob fills in about 16 s, which is the case
  for moving to a new segment.

The limits of that run, which the spikes in [Open questions](#open-questions) address:
- one run of 20-second levels, the last cut short by its full blob;
- every writer on one VM;
- 4 KiB blocks only;
- one account, which has a hierarchical namespace;
- no reads while appends landed;
- no moving from one blob to the next. Closing a blob was probed separately (next section).

### Closing a segment: Seal and leases don't work

A log of several blobs must be able to end one: no append may land in its records after
the end. `AzureSealProbe` (catalog-bench's `spikes` module) tried
three ways on 2026-11-10. It ran from the laptop against the same account, and against Azurite
3.37.0 (catalog-bench's `results/spike/azure-seal-20261110T224335Z`, `azure-lease-20261110T224617Z`,
`azure-fill-20261110T224935Z` and `azure-fill-paced-20261110T225107Z`):

| Fence | Azurite 3.37.0 | The account (hierarchical namespace) |
|---|---|---|
| Append Blob Seal | works as documented: appends after it fail with `409 BlobIsSealed` | doesn't exist. Seal fails with `400 InvalidQueryParameterValue` (`comp=seal`) at service versions 2019-12-12, 2021-12-02 and 2025-05-05, and appends after it land |
| An infinite lease, whose id no appender sends | appends without the id fail with `412 LeaseIdMissing` | appends without the id land (201), on an empty blob too. Overwrite and Delete without the id do fail with `412 LeaseIdMissing`: Append Block alone ignores the lease |
| Filling the blob to its size limit | holds | holds. Each writer appends with `x-ms-blob-condition-maxsize` set below the blob's limit, and a pad that reaches the limit closes the blob |

The fill fence ran 12 times on the account, on a 256 KiB blob with 1 KiB blocks:
- **Six runs with 16 writers.** The pad lost its position race 3–4 times in the runs where
  it started at half full, and then landed once the writers had filled the blob. In every
  run, each writer's next append failed with `412 MaxBlobSizeConditionNotMet`.
- **Six runs with 2 writers, paced so the pad could win early.** In three of them the pad
  landed at 134, 142 and 194 KiB while both writers were still appending. The writers kept
  sending for a second after each pad, 37–45 appends a run, and none landed.
- **Every run** ended at exactly the limit, holding the acknowledged blocks and then the pad.

The size condition is checked by Append Block itself, as the position condition is. The log v1
closed its segments that way: writers appended with the size condition, and a pad filled a
segment to its limit. The segmented log needs no fence at all. A segment ends in its own order,
at its Kth record or at its first seal frame (LOG_SEGMENTS.md D1), so a record that lands after
the end is void for every reader alike, and appends carry no condition. Append Block's reply
says the blob's committed block count (`x-ms-blob-committed-block-count`), the frame's index, so
a writer whose record landed past the limit knows it with no read.

## The design

[LOG_SEGMENTS.md](LOG_SEGMENTS.md) is the design. In brief:
- **The catalog object** is the catalog's FileIO's object at `<warehouse>/catalog`, in format 4
  ([ATOMIC_FILE_LOG.md](ATOMIC_FILE_LOG.md)): the checkpoint, the position it covers, where the
  segments are (unless at the default), the Installs of the segments from its own on (each
  setting its segment's record limit K), and the retired segments. An Install is a conditional
  append; a checkpoint, a CAS replace.
- **The segments** are Append Blobs under the prefix the catalog object records (LOG_SEGMENTS.md
  D13): the `fileio.catalog.log.location` its creator was given, or by default, for a catalog
  object on Azure, beside it: `https://<account>.blob.<suffix>/<container>/<path>.segments` for
  the object `abfss://<container>@<account>.dfs.<suffix>/<path>`, which the object then doesn't
  record. So every client finds them, whatever it is configured with, and needs only the
  account's credentials. Each log's are under its id: `<prefix>/<log id>/seg-<ordinal>-<nonce>`
  (D12), so catalogs whose creators were given one location keep their logs apart. A segment
  ends at its Kth record or its first seal frame. Its records count only once every earlier
  segment has ended.
- **Appends** go to the first segment a writer knows open; near its end a writer installs the
  next ahead. A read through an append seals any earlier segment that hasn't ended, and an
  append past its segment's end is void, a lost race (D4).
- **Compaction** writes the checkpoint by replacing the catalog object, retiring the segments
  before its own. A retired segment is deleted by name once its TTL has passed, and its entry
  dropped only then; the sweep also lists its log's segments for unnamed blobs no Install can
  name any more (empty ones: a credential that can't list leaks only those), and never another
  log's.
- **A record** is one block, so the log bounds a record (its `maxRecordBytes`: the smaller of a
  block and a 49,999th of a blob less a seal frame, less a frame's 17 bytes: 4,194,370 B, at any
  K, since a record may land in a segment whose installer set any K up to the blob's 50,000
  blocks (LOG_SEGMENTS.md, SG8; 4,194,287 B on a service version before 2022-11-02, whose blocks
  are 4 MiB)), and
  cannot write a larger one in a replace as the one-object log does. A commit whose record is
  over the bound sends its inline table updates to metadata files, largest first, until it fits
  (SPEC_TM.md "Size Budget"); one that still does not fit fails (`RecordTooLargeException`).

A read is two requests (the catalog object, then the segment), a refresh one.

## The store's requests

`AppendBlobSegments` (intentlog-azure) is the log's `SegmentStore` on an `AppendBlobStore`:

| Segment store | Azure request |
|---|---|
| create a segment | Put Blob (Append Blob), `If-None-Match: *` |
| append a frame | Append Block with no condition; the reply's `x-ms-blob-append-offset` and `x-ms-blob-committed-block-count` |
| read from an offset | Get Blob with `Range: bytes=<from−1>-`, the first byte dropped (a read from the end is then a `206`, not a `416` and a Get Blob Properties; Azurite answers `206` either way) |
| delete | Delete Blob |
| list (the sweep, for unnamed blobs) | List Blobs under `<prefix>/<log id>/seg-` |
| limits | 50,000 blocks a blob; blocks of 4 MiB before service version 2022-11-02, 100 MiB since; a blob of 50,000 × 4 MiB |

The catalog object's requests are its FileIO's (on Azure, the fork's ADLS FileIO: conditional
appends, and CAS by rename), and its pages are under `<warehouse>/catalog.committed/`. A segment
is named by its Blob URL (`BlobUrlSegments`), and its requests go to the container that URL
names, through a store opened for it on first use, signed by the key or signature the
catalog's properties give for its account.

## Failures

Azure's responses, mapped onto the SPI's signals (LOG.md "Failures"):

| Response | From | Signal |
|---|---|---|
| 201 | Append Block | a receipt; a block count past K is a void append, sent on to the next segment |
| 409 `BlockCountExceedsLimit` | Append Block | nothing landed: the segment has ended; send to the next |
| 404 `BlobNotFound` | Append Block | nothing landed: the catalog object held was stale. Read it and send again (within `append`) |
| 503 `ServerBusy`, 500 `OperationTimedOut`, 500 `InternalError`, 429, 408 | any write | `LogThrottledException`: backpressure, or a request that did not arrive whole in time; it may have landed, and sending it again is safe |
| another 4xx (403, 400, 413) | any write | `LogRefusedException`: the service refused it and nothing landed; sending it again would not change that, so the commit fails at once (`REFUSED`, the catalog's `CommitRefusedException`, which Iceberg does not retry; Bug 118 (refused writes reported unknown)) |
| a timeout, a reset connection, any other exception | a write | unknown outcome (the exception itself) |
| any failure | a read | the log could not be read |

**The Azure SDK's own retries are off**: `RequestRetryOptions` with `maxTries` 1, for every
request (`AzureAppendBlobStore.create`; a client given to its constructor must be built the same
way, which it cannot check). An SDK that sends a timed-out Append Block again can land the
record twice and report only the second copy. The runtime would never know about the first,
which is the bug `SegmentedLog.tla`'s `sdkretry` switch puts back.

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `fileio.catalog.log` | `atomic-file` | `azure-append`: the segmented log on Azure Append Blobs |
| `fileio.catalog.log.location` | the default: beside the catalog object | where a log this catalog creates keeps its segments, `https://<account>.blob.core.windows.net/<container>/<path>`; each log's segments are under `<path>/<log id>/`, so catalogs may share one. The catalog object records it (LOG_SEGMENTS.md D13), so a client opening a log needs none, and one given another is warned that it is ignored. Its default, for a catalog object `abfs[s]://<container>@<account>.dfs.<suffix>/<path>` (or `wasb[s]://`, `.blob.`), is `https://<account>.blob.<suffix>/<container>/<path>.segments`, which the object doesn't record; a catalog object elsewhere has none, and a client creating its log needs this |
| `adls.auth.shared-key.account.name`, `.key` | one of these two: required | the account key a container's requests are signed with, if the container is of that account |
| `adls.sas-token.<account>` (or `.<account>.dfs.core.windows.net`, `.blob.`) | | a shared access signature for the account's containers, used where no key of the account is given |
| `fileio.catalog.log.segment.records` | the store's block limit (provisional) | K, the record limit each Install sets |
| `fileio.catalog.log.copies` | false | whether an append near its segment's end also sends a copy to the next (D11) |
| `fileio.catalog.log.retire.ttl.ms` | 60,000 | how long a retired segment stays before it is deleted |
| `fileio.catalog.log.checkpoint.share`, `.min.bytes`, `.records` | 1.0, 1 MiB, 10,000 | when to compact: once the records since the checkpoint reach that share of its bytes, no fewer than the floor, nor more than `max.append.size` or that many records (`CompactionPolicy.share`); each writer at a point it draws past that |

## Modules

- **intentlog-core:** the `SegmentStore` SPI, segment frames, positions (`org.intentlog.segments`).
- **intentlog-fileio:** `SegmentedLog`, over `AtomicFileLog.segmented` for the catalog object.
- **intentlog-azure:** `AppendBlobSegments`, `BlobUrlSegments` (segments named by their Blob
  URLs, in the containers those name), `AzureAppendBlobStore` (the Azure SDK, its retries off),
  and `InMemoryAppendBlobStore` (the account's semantics in memory, with fault hooks).
- **iceberg-fileio-catalog:** `AzureLogs` opens the log from the catalog's properties, and
  derives the default prefix (`AzureLogs.defaultPrefix`).

## Testing

- **The model** `spec/tla/SegmentedLog.tla` (configurations `sg-*`; spec/tla/README.md "The
  segmented log").
- **The contract** (`TestSegmentedLogContract`), the regressions (`TestSegmentedLog`), and the
  commit-protocol fuzzer (`TestSegmentedCommitProtocol`), which draws the segments' store: in
  memory, or Append Blobs in memory through `AppendBlobSegments`, whose replies say the block
  count or don't; calibrated by `fuzz/mutants/segments.sh`. A fuzz round's `azure` job runs it.
- **The catalog's suites** in the `AZLOG` modes of `TestLocalCatalog` and
  `TestLocalCatalogTransaction`, every checkpoint checked against a replay of the log's history
  (`SegmentedLogHistory`); and `TestCatalogFaultProperties` and `TestCatalogRaceProperties`,
  which draw this log.
- **Clients configured alike or not** (`TestAzureLogs`): catalogs given another location, or
  none, share the log its creator made; a catalog object on Azure keeps its segments at the
  default and records nothing; a client without credentials for the recorded account fails,
  naming it.
- **A real account:** `AzureAppendBlobStoreIT` (the store's requests), and `AzureLogCatalogIT`
  (the catalog suites with their segments on the account, its catalog objects in memory, so each
  records its prefix; run explicitly with `AZURE_LOG_IT_CONTAINER_URL` and `AZURE_STORAGE_KEY` or
  `AZURE_STORAGE_SAS`).

## Measurements

Still to run on this log, each with its predictions written down first: catalog-bench's
campaigns on `azure-append` (their earlier runs measured the log v1), against `AtomicFileLog` on
the same profile, by writers and by conflict encoding. Appends at the tail on segments, which
would isolate what blind appends buy from what segments with checkpoints apart buy, are the
segmented log's conditional ablation, not built (LOG_SEGMENTS.md "Later").

## Open questions

Each needs a spike before the code depends on it, as `AzureAppendSpike` modes run by
catalog-bench's `bin/azure-spike.sh`, on a VM of the rmow-bench deploy:

| | Question | How to answer it |
|---|---|---|
| Q2 | While appends land, does a ranged GET return only whole blocks, with its length and block count consistent with its body? | readers parsing frames during the spike's blind levels |
| Q3 | How long do appends stall when a segment ends at 32–64 writers? | the spike with a small K, from the VM |
| Q4 | Where does a blob's rate stop growing? Blind appends had not stopped at 64 writers (3,113 landed/s, the client's CPU share 7.4%) | the spike at 128 and 256 writers, from two VMs, moving on to new segments so that no level fills its blob |
| Q5 | Do the rates hold at 64 KiB and across repeated runs, and on an account without a hierarchical namespace? | `--bytes 4096,65536 --reps 3`; a second account |

## Later

- **Several records in one block** (group commit): fewer blocks, so rarer new segments, at the
  cost of latency. Each record keeps its own frame and id, so replay doesn't change.
- **The FileIO's other credentials** for the segments (a connection string, the default
  credential), beside an account key or a SAS: the store takes a `TokenCredential` already.
- **Other stores** (LOG_SEGMENTS.md "Later").

## Related work

AirDB ("Eliminating Servers in State Machine Replication", arXiv 2611.01363, Nov 2026) also
keeps a command log in Azure Append Blob segments, created if absent and listed in a meta
segment, and its writers validate after appending. Cite it as concurrent work. What this log
adds:
- segments installed by conditional writes to the catalog object, and ended in their own order,
  so that no read sees a record a later read doesn't (D1, SG3);
- positions that every read checks;
- the runtime's exactly-once commit with its proofs;
- conflict rules carried in the records, which judge each record where it lands.
