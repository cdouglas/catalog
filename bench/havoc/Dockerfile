# The contention driver: a JRE and a build of fileio-catalog's test classes with their jars.
FROM eclipse-temurin:17-jre
COPY app /app
COPY bench-entry /usr/local/bin/bench-entry
ENTRYPOINT ["bench-entry"]
