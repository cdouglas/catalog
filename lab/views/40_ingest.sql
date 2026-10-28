-- The ingest benchmark (IngestRun). Per run: how far its measured pass got in its time, and its
-- rates in records/s: folding alone (replay, and building the state after each batch) and with its
-- compactions (GC record and checkpoint encode), over the run and over its second half, where the
-- JIT is warm and the tables have reached the snapshots they retain.
CREATE OR REPLACE TABLE ingest_metrics AS
WITH w AS (
  SELECT run_id, position, records, record_bytes, fold_ms, compact_ms, committed_bytes,
         max(position) OVER (PARTITION BY run_id) AS last
  FROM windows)
SELECT
  run_id,
  max(position) AS ingested,
  sum(record_bytes) / sum(records) AS record_bytes,
  sum(records) / (sum(fold_ms) / 1000) AS fold_records_s,
  sum(records) / ((sum(fold_ms) + sum(compact_ms)) / 1000) AS records_s,
  sum(records) FILTER (WHERE position > last / 2)
    / (sum(fold_ms) FILTER (WHERE position > last / 2) / 1000) AS fold_records_s_late,
  sum(records) FILTER (WHERE position > last / 2)
    / (sum(fold_ms + compact_ms) FILTER (WHERE position > last / 2) / 1000) AS records_s_late,
  sum(compact_ms) / (sum(fold_ms) + sum(compact_ms)) AS compact_share,
  max(committed_bytes) AS committed_bytes
FROM w
GROUP BY run_id;

-- Per run, its compactions: how many, the last checkpoint's bytes (and the committed set's share),
-- the mean time to encode one and to decode one (a cold reader), and how the checkpoint grew per
-- record ingested between the first compaction and the last.
CREATE OR REPLACE TABLE ingest_compaction_metrics AS
SELECT
  run_id,
  count(*) AS compactions,
  arg_max(checkpoint_bytes, position) AS last_checkpoint_bytes,
  arg_max(committed_bytes, position) AS last_committed_bytes,
  avg(encode_ms) AS encode_ms,
  avg(decode_ms) AS decode_ms,
  (arg_max(checkpoint_bytes, position) - arg_min(checkpoint_bytes, position))
    / nullif(max(position) - min(position), 0) AS checkpoint_bytes_per_record
FROM compactions
GROUP BY run_id;
