# The contention driver: a JRE and a build of fileio-catalog's test classes with their jars. BASE
# is any image with Java 17 on the PATH: one the host has already (the fuzz image, ce-fuzz:latest)
# needs no pull.
ARG BASE=eclipse-temurin:17-jre
FROM ${BASE}
COPY app /app
COPY bench-entry /usr/local/bin/bench-entry
ENTRYPOINT ["bench-entry"]
