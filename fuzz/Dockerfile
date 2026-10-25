# fileio-catalog's fuzz image: a JDK, Maven, and every dependency the build needs, offline, with
# one or more builds of the Iceberg fork's jars; the source comes in as a git bundle at run time.
# Built by image.sh, which seeds the repositories; run with run.sh (README.md).
FROM maven:3.9.11-eclipse-temurin-17

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 git \
    && rm -rf /var/lib/apt/lists/* \
    && git config --global --add safe.directory '*'

# base: every dependency but the fork's jars (read-only, the chained tail); forks/<name>: one
# build of the fork's jars each, the local repository a run names
COPY base /opt/m2/base
COPY forks /opt/m2/forks
COPY ce-fuzz /usr/local/bin/ce-fuzz
COPY mutants /opt/ce-fuzz/mutants
COPY provenance.txt /opt/ce-fuzz/provenance.txt

WORKDIR /work
ENTRYPOINT ["ce-fuzz"]
