# Fuzz rounds in a container

A phase-exit fuzz round runs nine long jqwik jobs (the fault, race, multi-writer and
commit-protocol fuzzers, the rebase oracle, the conflict-encoding contracts), and a calibration
runs the oracles once per mutant. On a laptop they saturate the CPU for hours and need capped
heaps to stay clear of the OOM killer. This image runs them on a bigger host, all at once,
offline: a JDK and Maven with every dependency baked in, one or more builds of the Iceberg fork's
jars, and the source as a git bundle mounted at run time, so one image serves any commit.

## On the dev box: build and pack

    fuzz/image.sh p12=$HOME/.m2/repository p3=$HOME/work/catalog/m2-ce p4=$HOME/work/catalog/m2-p4

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

Results go to `./out/<name>/`: a log per job, `summary.txt` (each job's exit and failures per
test class; for a calibration, each mutant `caught` or `NOT CAUGHT`, and `none`, the unmutated
oracles, which must be `clean`), and `run.txt` (the fork, commit, host). Copy `out/<name>/` back
to keep it. `JOBS` limits the jobs at once (default all), `HEAP` each test JVM's heap (default
2g; a job's resident size is about a gigabyte more). A round wants nine cores and about 30 GB;
a calibration, a core and 3 GB per mutant.

On rootful Docker the results are root's. Rootless Docker: the daemon is a user service, which stops at logout unless the user lingers
(`loginctl enable-linger`); `run.sh` warns. Containers are kept after they exit (`run.sh stop
<name>` removes one); a name is one run (an earlier run's results are never overwritten).

## What runs

`ce-fuzz` (the image's entrypoint) clones the bundle, checks out a worktree per job at the
commit, sets each property's tries (as the phase-exit rounds do: `ce-fuzz` with no arguments
lists them), and runs `mvn -o test -Dtest=...` in each, against `/opt/m2/forks/<fork>` chained
to `/opt/m2/base` (Maven 3.9's `maven.repo.local.tail`). A calibration set
(`mutants/<set>.sh`) names its mutants, its oracles and their tries; each mutant is a code
change, put back by an exact-text replacement anchored in the commit it was written for
(`phase3.sh`: 67e91f4; `phase4.sh`: the phase 4 commit), and must fail an oracle.
