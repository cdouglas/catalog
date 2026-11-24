# Fuzz rounds in a container

A phase-exit fuzz round runs eleven long jqwik jobs (the fault, race, multi-writer and
commit-protocol fuzzers, the last also on the segmented log as job `azure`, the rebase oracle, the conflict-encoding contracts, the inline
differential and delta properties), and a calibration
runs the oracles once per mutant. On a laptop they saturate the CPU for hours and need capped
heaps to stay clear of the OOM killer. This image runs them on a bigger host, all at once,
offline: a JDK and Maven with every dependency baked in, one or more builds of the Iceberg fork's
jars, and the source as a git bundle mounted at run time, so one image serves any commit.

## On the dev box: build and pack

    fuzz/image.sh p4c=$HOME/.m2/repository     # or several: p3=<repo> p4c=<repo>

Each `<name>=<repo>` bakes in the fork jars (`1.12.0-atomicio-SNAPSHOT`) of that local Maven
repository as fork build `<name>`, so a run names the build its commit was tested with (the
fork's commit is in `run.sh forks`). The last one, which HEAD must build with, also seeds the
base repository: an online build and test run of HEAD in the base image fetches every plugin,
dependency and surefire provider, then the fork's jars come out of it. It also builds the image here. The pack,
`fuzz/build/ce-fuzz-<date>.tar` (about 240 MB), holds `ctx/` (the build context: the
repositories, the driver, the calibration sets), `fileio.bundle` (every local branch, so unpushed
commits too), `run.sh`, `queue.sh` and this file. For new commits, only the bundle changes:

    git bundle create fileio.bundle --branches

## On the host: build and run

    tar xf ce-fuzz-<date>.tar && cd ce-fuzz-<date>
    ./run.sh build                                # pulls maven:3.9.11-eclipse-temurin-17
    SMOKE=1 ./run.sh round p4 ce-phase4 smoke     # minutes: checks the image, 3 tries each
    ./run.sh round p4 ce-phase4 p4                # the phase-exit round, detached
    ./run.sh calib p4 ce-phase4 p4-calib phase4   # the phase's calibration, detached
    ./run.sh status                               # every run: each job, running or its exit
    ./run.sh mem                                  # each run's memory and CPU, and the host's
    ./run.sh shrink p4 ce-phase4 p4-rebase p4/rebase   # shrink what failed in p4's rebase job

Results go to `./out/<name>/`: a log per job, `<job>.log.gz` (gzip: `zcat` or `zgrep -a` it,
which also read a log cut short), with its start and exit lines in `<job>.log`; `summary.txt`
(each job's exit and failures per test class; for a calibration, each mutant `caught` or `NOT
CAUGHT`, and `none`, the unmutated oracles, which must be `clean`), and `run.txt` (the fork,
commit, host). Copy `out/<name>/` back
to keep it. `JOBS` limits the jobs at once (default all), `HEAP` each test JVM's heap (default
4g; a job's resident size is up to two gigabytes more, with Maven's). `ROUND_JOBS` runs some of a
round's jobs (`ROUND_JOBS=race ./run.sh round ...` reruns one; the limits count only those).
`./run.sh plan <cmd> <args...>` prints the JVMs, memory limit and CPUs a run would take.

## Shards

jqwik runs a property's tries one after another, so a job is about one core, and a round takes
as long as its longest job: p15-r1's `azure` (1,000,000 tries) ran 13.6 hours, its next longest
6. A round therefore runs each long job as several shards (`SHARDS`; `ce-fuzz` with no arguments
gives the default): each shard its own worktree and JVM, with its own random seed and its share
of each property's tries (rounded up), as `<job>.<k>` (`<job>.<k>.log.gz`). The default makes 53
JVMs of the eleven jobs, about an hour and a quarter each on a loaded host, so a round wants 53
cores (80 at most: the CPU limit) and 320 GB of limits (about 2 GB each in use). `SHARDS=1` runs each job as one
JVM; `SHARDS="azure=16 race=8"` names counts, and a bare count is every job's that isn't named.
`summary.txt` gives each shard's line, then each sharded job's: its shards' exits, the longest's
time, each class's failures summed. jqwik sizes what it generates by a property's tries, so a
shard (thousands of tries for every job, at least a calibration's) is not quite a slice of the
whole job's run: `shrink` reruns a shard with its own share, as its start line records, and so
must a replay of its seed. A calibration margin (a round's tries at least ten times a mutant's
first failing try) counts the job's tries, all shards together. Jobs start `STAGGER` seconds
apart (default 10), as each compiles for about a minute first; a calibration's mutants too.

## Sharing the host

`run.sh` gives each run a CPU limit (`CPUS`, default one and a half per JVM) besides its memory
limit, so a burst of its JVMs' compilers and collectors leaves the host's other users their
share; `CPU_SHARES=2` makes a run yield to every other process when the CPUs are all busy.
`queue.sh <file>` (run it detached: `setsid -f nohup ./queue.sh q.txt >> out/queue.log 2>&1 <
/dev/null`) starts the runs a file lists (`calib p16 36b7e98 p16f-cal-budget budget`, after any
assignments: `SNAPSHOT_BLOCK=2 SNAPSHOT_WINDOW=1 calib ...`), in order, each once the host has room:
its 5-minute load average and the run's JVMs come to at most `LOAD_MAX` (default 120; runs it
started in the last 10 minutes count at their JVMs, as the load average lags), and our runs never
oversubscribe its memory: the host's available memory (with the ZFS ARC above its floor, which the
kernel gives back under pressure), less what every running ce-fuzz container may still grow by up
to its limit, must leave the run's limit and `MEM_RESERVE` gigabytes (default 64). Lines appended
while it runs are taken in turn, and `<file>.done` records what started. A line `soak <fork>
<commit> <prefix>` makes it, with nothing queued waiting, start rounds `<prefix>-1`, `<prefix>-2`,
... of that commit, one at a time (`SOAK_MAX`), when the load is under `SOAK_LOAD` (default 100)
and the memory has room as above, at `CPU_SHARES=2`: a soak takes only cycles nobody else wants,
and queued runs count the load of a soak round past its ramp-up as free. Memory does not yield
so: a soak round stands aside for anyone who needs it. A queued run that would fit were the soak
rounds stopped stops the newest; and when the room falls below the reserve (another user's
demand grew), the newest soak round is stopped and none starts for `SOAK_BACKOFF` seconds (default
1800). A stopped round keeps its logs, but its unfinished shards' seeds are lost. `soak off`
stops it starting more. An unattended soak runs beside `soak-guard.sh <file> <deadline> [low]
[high]`, which pauses it (`soak off`) while `/data` has under 40 GiB free and appends its soak
line again once it has 60, so soaking resumes; at the deadline it ends it. A round's logs take
about 0.7 GB.
A round's faultA2 and faultB2 page snapshots at blocks of 2 and a window of 1, and fail if too
few of their tries seal a block; the other jobs run at the catalog's default, which seals nothing
in their short schedules. `SNAPSHOT_BLOCK` (and `SNAPSHOT_WINDOW`) sets paging for every job of a
run instead: a paging round (`r0p`) runs the race, rebase and differential jobs at 2 and 1 too.

## Memory on a shared host

Each run's container has a hard memory limit, without swap (`MEM`; by default its JVMs at once
times their heap and 2 GB, and 2 GB: 320 GB for a round of 53 shards), and the highest OOM score. A run that
outgrows its limit loses one of its own JVMs to the kernel's OOM killer (the job fails, exit 137
in its log), never another user's process; and should the host itself run short, the kernel
kills this run first. `run.sh` warns if the limit did not take: rootless Docker applies it only
where systemd delegates the memory controller to the user's cgroup (`cat
/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers` lists
`memory`). Without it, keep `JOBS` times `HEAP` well within the host's free memory (`run.sh
mem`).

## Shrinking

A round runs without shrinking (`SHRINKING=OFF`): jqwik reports a failing property's original
sample and its seed at once and moves on. Shrinking in the round's JVM retains every candidate it
tries, so a long multi-writer sample could fill a small heap and thrash for hours. `run.sh shrink
<fork> <commit> <name> <run>/<job>` then shrinks each property that failed in that job, one at a
time: a worktree set up as the job's was (its tries, a shard's share, which also size jqwik's
generation; its snapshot paging), the
property pinned to the seed it failed with, run alone in a JVM of `SHRINK_HEAP` (default 16g),
shrinking for `SHRINK_SECONDS` (default 1800), with a timeout of twice that and an hour. Its log
is `out/<name>/<Class>.<method>.log.gz`; `summary.txt` gives each one's exit and its shrunk sample's
steps. Use the commit the job ran at. A round with `SHRINKING=BOUNDED` shrinks in place, for
`SHRINK_SECONDS` each, in its jobs' heaps; `JOB_TIMEOUT` (as `timeout` takes it, `12h`) bounds any
job.

On rootful Docker the results are root's. Rootless Docker: the daemon is a user service, which stops at logout unless the user lingers
(`loginctl enable-linger`); `run.sh` warns. Containers are kept after they exit (`run.sh stop
<name>` removes one), but a run deletes its worktrees, their builds and surefire's temporary
files as it ends, so an exited container holds little besides its image; a name is one run (an earlier run's results are never overwritten).

## What runs

`ce-fuzz` (the image's entrypoint) clones the bundle, checks out a worktree per job (or shard) at the
commit, sets each property's tries (as the phase-exit rounds do: `ce-fuzz` with no arguments
lists them; a job names one count per property of its class, or `each N`, and a class whose
properties don't match its job's counts stops the run at setup, so a property added to a fuzzed
class never inherits a count sized for another), and runs `mvn -o test -Dtest=...` in each, against `/opt/m2/forks/<fork>` chained
to `/opt/m2/base` (Maven 3.9's `maven.repo.local.tail`). Generation is random
(`jqwik.generation.default`) unless a property asks otherwise: jqwik multiplies its parameters'
exhaustive counts unchecked, and at a job's tries a large space whose product wraps negative was
enumerated in order (round cp4-r0's race job ran 200,000 copies of one failing namespace create). A calibration set
(`mutants/<set>.sh`) names its mutants, its oracles and their tries; each mutant is a code
change, put back by an exact-text replacement anchored in the code as the set's last commit
left it (a calibration of an older commit needs the set as it was then), and must fail an
oracle.
