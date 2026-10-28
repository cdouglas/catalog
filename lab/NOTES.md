# What the numbers mean

Results of the multi-writer contention driver (`bench/run.sh`,
`iceberg-fileio-catalog/src/test/java/org/apache/iceberg/io/contention/`). Each run is one
configuration (`runs.params`, promoted columns in `runs`) run once: writers commit Iceberg operations
to tables of one `FileIOCatalog` over an in-memory store whose requests take a provider's latency.

## The store

- Latencies are endive's provider profiles (`StoreProfile`, from `endive/endive/providers/*.toml`):
  lognormal, floored. A GET or PUT is about a base plus a cost per MiB (endive's read cost per MiB is
  0: a large catalog read costs no more than a small one, which flatters big catalogs). A CAS is
  no faster than a PUT of its bytes. A conditional append has a landing latency and, when it loses
  its race, a failure latency (Azure's is about 2 s).
- A GET and a conditional write take effect halfway through their latency (endive's split), so
  writers race for the catalog object as they would over a network.
- `s3` and `gcp` have no conditional append: the catalog compacts (CASes the whole object) on every
  commit there.
- `time_scale` scales every latency (1 = as measured).
- The dev box's monotonic clock may run slow (WSL, about 10%); durations are in its time, sleeps and
  measurements alike.

## The writers

- Each writer is a thread with its own `FileIOCatalog` instance (as each would be a process).
- Closed load (`arrival_rate` = 0): `writers` threads, each running one operation after another
  with an exponential think time of mean `think_ms`.
- Open load (`arrival_rate` > 0, per second): Poisson arrivals wait in a queue for one of
  `max_inflight` writers; `queue_ms` is the wait, and `latency_ms` includes it.
- Each operation loads the table, waits out its job's runtime (`runtime_ms` by kind, lognormal with
  `runtime_sigma`), then commits. Fast appends refresh the table when they commit (Iceberg's
  `apply`), so their runtime only paces the writer; validated operations validate from the
  snapshot the job read, so their runtime is their conflict window.
- Iceberg's commit retry is on (`retry_num`, default 4, with its backoff from `retry_min_wait_ms`).
- A maintenance thread expires all but the last `expire_retain` snapshots every `expire_every_s`
  (kind `expire`), keeping tables at a steady size; with `expire_older_than_s` (added after the
  p5 campaign; 0, its default, expires every snapshot but those) only snapshots older than that.
  A validated operation whose starting snapshot expires during its job fails Iceberg's validation
  ("Cannot determine history"): with 50 snapshots kept, at 10+ commits/s the history is shorter
  than a 5 s compaction.

## `ops` (one row per operation)

- `measured`: started after the warmup and before the end. Count throughput over measured rows and
  `duration_s`.
- `outcome`:
  - `committed`;
  - `validation_failed`: a real conflict (Iceberg's validation found a conflicting commit);
  - `retries_exhausted`: every attempt conflicted (`CommitFailedException` after `retry_num`
    retries);
  - `unknown`;
  - `nothing_to_do` (a compaction of a partition with one file);
  - `error`;
  - `dropped` (open load: the arrival queue was full).
- `catalog_commits`, `conflicts`, `rejected`, `unknown`, `attempts`: the operation's commits to the
  catalog by outcome (`CatalogLog.counts`). A conflict is a record replay did not apply where it
  landed, which Iceberg's retry then redid on a fresh read: one more attempt.
  - Conflicts in a committed operation are false positives: the retry repaired them.
  - An operation that ends `validation_failed` after conflicts saw a real one.
- Times, in ms from the run's start: `arrived_ms`, `started_ms` and `ended_ms`. Durations:
  `queue_ms`, `runtime_ms`, `commit_ms` (from the end of the runtime to the end) and `latency_ms`
  (arrival to end).
- `cpu_ms`: the writer thread's CPU for the operation. Iceberg's worker pool (manifest writes) is
  not in it.
- Requests the writer thread made:
  - `heads`, `gets`, `puts`;
  - `cas_landed`, `cas_lost`, `appends_landed`, `appends_lost`;
  - `bytes_read`, `bytes_written`, `catalog_bytes_read`;
  - `store_wait_ms`.

  Manifest writes run on Iceberg's worker pool, so a writer's `puts` miss them; the run's totals in
  `manifest.json` (`summary.requests`) count every thread.

## `samples` (the catalog object, every `sample_every_ms`)

`catalog_bytes`, `checkpoint_bytes`, `records` (intention records after the checkpoint),
`objects` (in the store) and `heap_used`.

## The host

- `run_cpu` (views/30_flags.sql): the driver's process CPU over the run, per CPU the JVM saw.
  Above 60% (`cpu_bound`), the writers queued for the host's CPUs, which writers on their own
  machines would not: such a run measured the host as much as the protocol. havoc's shards had 4
  CPUs each in the p5 campaign; at 16 and 32 writers the merging encodings were CPU-bound there.
- A writer's CPU per operation grows with the catalog object, which grows with the table: fast
  appends never merge manifests, and an inline-ML checkpoint holds each snapshot's list of
  manifests (snapshots x manifests). A run that commits faster ends with a larger object. A
  compaction (a merging snapshot producer) merges manifests, which keeps the object small.

## Known runs

- The p5 campaign (c1-c6, 2026-10-27, havoc, 8 shards of 4 CPUs) ran fileio-catalog 2afeb46 (its
  manifests' git_sha; rebased since as d85309b): before the backoff after a lost race, with
  lamport the default. CPU-bound at 16 and 32 writers (`run_cpu`). Superseded by p6.
- The p6 campaign (c1-c7 and c2r, 2026-10-27, havoc, 6 shards of 16 CPUs) ran 8636dda: the code
  merged, manifest-window the catalog's default (the campaigns name their encodings), the backoff
  after a lost race (20 ms unless `race_min_wait_ms` says otherwise).

- `c8-fold` (2026-10-27, havoc, 3 shards of 16 CPUs per build, both builds at once) runs c1's grid
  on 4e65a65 (before replay folded a read onto a replica) and 56facd8 (after), at the defaults of
  4e65a65 (100 ms backoff); tell the builds apart by `git_sha`. Both builds' runs are canonical:
  a page that reads the campaign must split them.

- `pilot-fa`, `pilot-mix` (2026-10-27) were pilots of the driver. The first `pilot-mix` runs gave
  fast appends the validated operations' 2 s runtime too; the runtime is per kind since.
