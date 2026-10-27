-- How busy each run kept its CPUs: the driver's process CPU over the run's elapsed time (preload,
-- warmup and drain included, so the measured window was busier still), per CPU the JVM saw
-- (havoc's shards: -XX:ActiveProcessorCount = the container's limit). A run that kept them more
-- than 60% busy measured the host as much as the protocol: its writers, which would each have
-- their own machine, queued for the shard's CPUs.
CREATE OR REPLACE TABLE run_cpu AS
SELECT
  run_id,
  cpus,
  CAST(json_extract(manifest::JSON, '$.summary.process_cpu_s') AS DOUBLE) AS process_cpu_s,
  CAST(json_extract(manifest::JSON, '$.elapsed_s') AS DOUBLE) AS elapsed_s,
  CAST(json_extract(manifest::JSON, '$.summary.process_cpu_s') AS DOUBLE)
    / CAST(json_extract(manifest::JSON, '$.elapsed_s') AS DOUBLE) / cpus AS busy,
  CAST(json_extract(manifest::JSON, '$.summary.process_cpu_s') AS DOUBLE)
    / CAST(json_extract(manifest::JSON, '$.elapsed_s') AS DOUBLE) / cpus > 0.6 AS cpu_bound
FROM runs
WHERE source = 'contention';
