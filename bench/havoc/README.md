# Contention campaigns on another host

The contention driver (`bench/run.sh`) runs one configuration at a time; a campaign of a few
hundred runs takes hours. This package runs one on a bigger host as several shards at once, each
a container with a hard memory limit: a JRE, a build of fileio-catalog's test classes with every
jar they need, and the campaign configurations. Nothing is built on the host.

## On the dev box: pack

    bench/havoc/pack.sh        # bench/build/bench-<stamp>.tar (a build of the tree as it is)

The build's commit (and whether the tree was dirty) is in `ctx/app/provenance.env` and in each
run's manifest.

## On the host: build and run

    tar xf bench-<stamp>.tar && cd bench-<stamp>
    ./run.sh build                                       # pulls eclipse-temurin:17-jre
    ./run.sh campaign c1 c1-fa-scaling.json              # detached, JOBS shards
    ./run.sh status c1                                   # shards, and runs finished
    ./run.sh mem                                         # each shard's memory and CPU

A shard runs every JOBS-th run of the campaign's files, in order, so shards interleave over the
axes. Results go to `./out/<name>/<label>-<hash>/<stamp>/` (manifest.json, ops.jsonl,
samples.jsonl); copy them back into the dev box's `bench/results/` and `lab ingest`. `JOBS`
(default 8) shards at once, each with `HEAP` (default 3g), a memory limit `MEM` (default HEAP and
2g, without swap, OOM score 1000: on a shared host the kernel kills this campaign's JVM first) and
`CPUS` (default 4). Runs sleep most of their time in the store's latency; a shard's CPU is its
replays and Iceberg's work, a core or two at 32 writers.

A run's timings are wall-clock: the host's other load shows in its CPU-bound parts (replay,
manifest writes), not in the latency it sleeps. Compare runs of one campaign on one host.
