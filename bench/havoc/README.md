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
    BASE=ce-fuzz:latest ./run.sh build                   # or builds on an image the host has
    ./run.sh campaign c1 c1-fa-scaling.json              # detached, JOBS shards
    ./run.sh status c1                                   # shards, and runs finished
    ./run.sh mem                                         # each shard's memory and CPU

A shard runs every JOBS-th run of the campaign's files, in order, so shards interleave over the
axes. Results go to `./out/<name>/<label>-<hash>/<stamp>-r<repetition>/` (manifest.json, ops.jsonl,
samples.jsonl); copy them back into the dev box's `bench/results/` and `lab ingest`. `JOBS`
(default 8) shards at once, each with `HEAP` (default 3g), a memory limit `MEM` (default HEAP and
2g, without swap, OOM score 1000: on a shared host the kernel kills this campaign's JVM first) and
`CPUS` (default 4). Runs sleep most of their time in the store's latency; a shard's CPU is its
replays and Iceberg's work, a core or two at 32 writers.

A run's timings are wall-clock: the host's other load shows in its CPU-bound parts (replay,
manifest writes), not in the latency it sleeps. Compare runs of one campaign on one host.

## The ingest benchmark and JMH, build against build

`run.sh ingest <name> <configs>` runs the ingest benchmark (IngestBench) in one container (16
CPUs, a 6g heap and a 12g limit unless `CPUS`, `HEAP` and `MEM` say otherwise: a capture runs 16
writers, and a JVM of 13 ingests past a 6g heap outgrew 8g), its
captures under `$OUT/<name>/captures/`. `run.sh jmh <name> <campaign...> -- <JMH args>` runs
`IngestFoldBench.fold` over every log those campaigns captured, its JSON in `$OUT/<name>/`. To
compare builds, pack each from a worktree of its commit and run them at once, one image each, so
the host's load falls on all alike:

    git worktree add ../wt-old <commit>
    ROOT=../wt-old bench/havoc/pack.sh                   # bench-<stamp>-<commit>.tar

    # on the host, per package (one OUT for all, so jmh sees every campaign's captures):
    IMAGE=ce-bench:old BASE=ce-fuzz:latest ./run.sh build
    OUT=~/data/bench-h1 IMAGE=ce-bench:old ./run.sh ingest h1-old h1-shapes.json
    OUT=~/data/bench-h1 IMAGE=ce-bench:new ./run.sh jmh j1-new h1-new h1-old -- -p batch=64 \
        -p committed=steady -f 1 -wi 3 -i 10

A build's records are in its own format, so each campaign captures its own logs, and a newer
build replays an older one's (a format change here only adds fields).
