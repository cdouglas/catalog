-- Generated traces (LogTrace; lab/NOTES.md "The log benchmark") and their ingest. One row per
-- ingest run of a trace: the trace's workload, encoding, oracle or contended, writers; what the
-- generator counted (entries landed, rejected in the log, refused by the oracle before landing,
-- the mean window in entries); and what the ingest measured (records/s, goodput/s: entries that
-- applied per second). A contended trace's rejected entries land and replay rejects them; an
-- oracle trace's refused entries never land, so all its entries apply.
CREATE OR REPLACE VIEW trace_ingest AS
SELECT r.run_id, r.trace_dir, r.trace_label, r.trace_encoding, r.trace_oracle, r.trace_writers,
       r.trace_entries AS entries,
       CAST(json_extract(r.manifest, '$.capture.manifest.summary.rejected') AS BIGINT) AS rejected,
       CAST(json_extract(r.manifest, '$.capture.manifest.summary.refused') AS BIGINT) AS refused,
       CAST(json_extract(r.manifest, '$.capture.manifest.summary.mean_window') AS DOUBLE)
           AS mean_window,
       CAST(json_extract(r.manifest, '$.summary.records_s') AS DOUBLE) AS records_s,
       CAST(json_extract(r.manifest, '$.summary.goodput_s') AS DOUBLE) AS goodput_s
FROM runs r
WHERE r.source = 'ingest' AND r.trace_dir LIKE 'traces/%';

-- JMH IngestFoldBench.fold over a trace (its log param names traces/<label>-<hash>/log.bin), per
-- run: the fold's records/s with its error. The run's label says the build (j-g1a-8d9277a, ...).
CREATE OR REPLACE VIEW trace_fold AS
SELECT j.run_id, regexp_extract(j.params['log'], 'traces/[^/]+') AS trace_dir,
       CAST(j.params['batch'] AS INTEGER) AS batch, j.params['committed'] AS committed,
       j.score AS records_s, j.score_error AS records_s_error
FROM jmh j
WHERE j.metric = 'records' AND j.params['log'] LIKE '%traces/%';
