-- Per run and kind of operation, over the measured operations (started after the warmup, before
-- the end): throughput, how operations ended, the attempts their commits took, and latency.
--
-- fp_conflicts are conflicts in operations that committed (Iceberg's retry repaired them: false
-- positives); real_conflicts are operations that ended in a validation failure.
CREATE OR REPLACE TABLE kind_metrics AS
SELECT
  o.run_id,
  o.kind,
  count(*) AS ops,
  count(*) FILTER (WHERE o.outcome = 'committed') AS committed,
  count(*) FILTER (WHERE o.outcome = 'committed') / any_value(r.duration_s) AS commits_s,
  count(*) FILTER (WHERE o.outcome = 'validation_failed') AS real_conflicts,
  count(*) FILTER (WHERE o.outcome = 'retries_exhausted') AS exhausted,
  count(*) FILTER (WHERE o.outcome IN ('unknown', 'error')) AS errors,
  count(*) FILTER (WHERE o.outcome = 'nothing_to_do') AS noops,
  sum(o.conflicts) AS conflicts,
  sum(o.conflicts) FILTER (WHERE o.outcome = 'committed') AS fp_conflicts,
  avg(o.attempts) FILTER (WHERE o.outcome = 'committed') AS attempts_per_commit,
  quantile_cont(o.latency_ms, 0.5) FILTER (WHERE o.outcome = 'committed') AS p50_ms,
  quantile_cont(o.latency_ms, 0.99) FILTER (WHERE o.outcome = 'committed') AS p99_ms,
  avg(o.latency_ms) FILTER (WHERE o.outcome = 'committed') AS mean_ms,
  avg(o.commit_ms) FILTER (WHERE o.outcome = 'committed') AS commit_ms,
  avg(o.queue_ms) AS queue_ms,
  avg(o.cpu_ms) AS cpu_ms,
  avg(o.store_wait_ms) AS store_wait_ms,
  avg(o.appends_lost) AS appends_lost,
  avg(o.cas_lost) AS cas_lost,
  avg(o.gets) AS gets,
  avg(o.catalog_bytes_read) AS catalog_bytes_read
FROM ops o JOIN runs r USING (run_id)
WHERE o.measured AND o.kind NOT IN ('arrival')
GROUP BY o.run_id, o.kind;

-- Per run, every kind but maintenance (expire) together.
CREATE OR REPLACE TABLE run_metrics AS
SELECT
  o.run_id,
  count(*) AS ops,
  count(*) FILTER (WHERE o.outcome = 'committed') / any_value(r.duration_s) AS commits_s,
  count(*) FILTER (WHERE o.outcome = 'validation_failed') AS real_conflicts,
  count(*) FILTER (WHERE o.outcome = 'retries_exhausted') AS exhausted,
  count(*) FILTER (WHERE o.outcome = 'dropped') AS dropped,
  sum(o.conflicts) AS conflicts,
  sum(o.conflicts) FILTER (WHERE o.outcome = 'committed') AS fp_conflicts,
  sum(o.conflicts) FILTER (WHERE o.outcome = 'committed') / nullif(sum(o.conflicts), 0)
    AS fp_share,
  avg(o.attempts) FILTER (WHERE o.outcome = 'committed') AS attempts_per_commit,
  quantile_cont(o.latency_ms, 0.5) FILTER (WHERE o.outcome = 'committed') AS p50_ms,
  quantile_cont(o.latency_ms, 0.99) FILTER (WHERE o.outcome = 'committed') AS p99_ms,
  avg(o.cpu_ms) AS cpu_ms,
  avg(o.store_wait_ms) AS store_wait_ms,
  avg(o.appends_lost) AS appends_lost,
  avg(o.cas_lost) AS cas_lost
FROM ops o JOIN runs r USING (run_id)
WHERE o.measured AND o.kind NOT IN ('expire')
GROUP BY o.run_id;

-- The catalog object over the measured duration.
CREATE OR REPLACE TABLE catalog_metrics AS
SELECT
  run_id,
  avg(catalog_bytes) AS catalog_bytes,
  max(catalog_bytes) AS max_catalog_bytes,
  avg(checkpoint_bytes) AS checkpoint_bytes,
  avg(records) AS records
FROM samples
WHERE measured
GROUP BY run_id;

-- endive's predictions, per run (seeds together), over its steady state: transactions submitted
-- after its first 15 minutes and before its last 15 (endive's own convention).
CREATE OR REPLACE TABLE endive_metrics AS
SELECT
  run_id,
  operation_type AS kind,
  count(DISTINCT seed) AS seeds,
  count(*) AS txns,
  count(*) FILTER (WHERE status = 'committed') / (1800.0 * count(DISTINCT seed)) AS commits_s,
  count(*) FILTER (WHERE abort_reason = 'validation_exception') AS real_conflicts,
  count(*) FILTER (WHERE abort_reason = 'max_retries_exceeded') AS exhausted,
  avg(n_retries + 1) FILTER (WHERE status = 'committed') AS attempts_per_commit,
  quantile_cont(total_latency, 0.5) FILTER (WHERE status = 'committed') AS p50_ms,
  quantile_cont(total_latency, 0.99) FILTER (WHERE status = 'committed') AS p99_ms,
  avg(commit_latency) FILTER (WHERE status = 'committed') AS commit_ms
FROM endive_txns
WHERE t_submit >= 900000 AND t_submit < 2700000
GROUP BY run_id, operation_type;
