# Fuzz rounds in a container

A phase-exit fuzz round runs ten long jqwik jobs (the fault, race, multi-writer and
commit-protocol fuzzers, the rebase oracle, the conflict-encoding contracts, the inline
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
commits too), `run.sh` and this file. For new commits, only the bundle changes:

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

Results go to `./out/<name>/`: a log per job, `summary.txt` (each job's exit and failures per
test class; for a calibration, each mutant `caught` or `NOT CAUGHT`, and `none`, the unmutated
oracles, which must be `clean`), and `run.txt` (the fork, commit, host). Copy `out/<name>/` back
to keep it. `JOBS` limits the jobs at once (default all), `HEAP` each test JVM's heap (default
4g; a job's resident size is up to two gigabytes more, with Maven's). A round wants ten cores
and about 62 GB; a calibration, a core and 6 GB per mutant.

## Memory on a shared host

Each run's container has a hard memory limit, without swap (`MEM`; by default its jobs at once
times their heap and 2 GB, and 2 GB: 62 GB for a round), and the highest OOM score. A run that
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
time: a worktree set up as the job's was (its tries, which also size jqwik's generation), the
property pinned to the seed it failed with, run alone in a JVM of `SHRINK_HEAP` (default 16g),
shrinking for `SHRINK_SECONDS` (default 1800), with a timeout of twice that and an hour. Its log
is `out/<name>/<Class>.<method>.log`; `summary.txt` gives each one's exit and its shrunk sample's
steps. Use the commit the job ran at. A round with `SHRINKING=BOUNDED` shrinks in place, for
`SHRINK_SECONDS` each, in its jobs' heaps; `JOB_TIMEOUT` (as `timeout` takes it, `12h`) bounds any
job.

On rootful Docker the results are root's. Rootless Docker: the daemon is a user service, which stops at logout unless the user lingers
(`loginctl enable-linger`); `run.sh` warns. Containers are kept after they exit (`run.sh stop
<name>` removes one); a name is one run (an earlier run's results are never overwritten).

## What runs

`ce-fuzz` (the image's entrypoint) clones the bundle, checks out a worktree per job at the
commit, sets each property's tries (as the phase-exit rounds do: `ce-fuzz` with no arguments
lists them), and runs `mvn -o test -Dtest=...` in each, against `/opt/m2/forks/<fork>` chained
to `/opt/m2/base` (Maven 3.9's `maven.repo.local.tail`). A calibration set
(`mutants/<set>.sh`) names its mutants, its oracles and their tries; each mutant is a code
change, put back by an exact-text replacement anchored in the code as the set's last commit
left it (a calibration of an older commit needs the set as it was then), and must fail an
oracle.
