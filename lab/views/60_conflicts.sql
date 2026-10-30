-- Conflicts by their reason (since 2026-10-30; earlier runs have none). A reason is
-- "<category>: <detail>": the first thing the record read that changed since its writer's read
-- ("concern: properties", "ref: advanced", "partition: data files added", "window: ..."), why its
-- witness was the table's whole version ("whole: lamport", "whole: removes snapshots",
-- "whole: commit once: a schema change", "whole: pinned (a catalog transaction's table)", ...),
-- or why a record the encoding admitted could not merge ("merge: ..."). Read through to_json so
-- that runs without the fields still ingest.

-- One row per (operation, reason): the operation's catalog conflicts with that reason.
CREATE OR REPLACE VIEW conflict_reasons AS
WITH r AS (
  SELECT run_id, op_id, kind, outcome, measured,
         json_extract(to_json(o), '$.conflict_reasons') AS reasons
  FROM ops o
)
SELECT run_id, op_id, kind, outcome, measured, j.key AS reason,
       split_part(j.key, ':', 1) AS category, CAST(j.value AS BIGINT) AS conflicts
FROM r, json_each(r.reasons) j
WHERE r.reasons IS NOT NULL;

-- One row per (catalog transaction, cause): why its attempts failed. "catalog: its record
-- conflicted" (the reason is in conflict_reasons), or a check before the catalog wrote it:
-- "isolation: ..." (its serializable read set), "pin: ..." (a written table's version, errata
-- D12), "requirement: ..." (the commits' requirements).
CREATE OR REPLACE VIEW txn_failures AS
WITH r AS (
  SELECT run_id, op_id, outcome, measured,
         json_extract(to_json(o), '$.txn_failures') AS failures
  FROM ops o
  WHERE kind = 'catalog_txn'
)
SELECT run_id, op_id, outcome, measured, j.key AS cause,
       split_part(j.key, ':', 1) AS category, CAST(j.value AS BIGINT) AS attempts
FROM r, json_each(r.failures) j
WHERE r.failures IS NOT NULL;

-- Per ingest run: the records of its capture that replay rejected, by reason (the writers'
-- conflicts that landed in the log; on a store without conditional append, the commit loop
-- rejects before writing and none land).
CREATE OR REPLACE VIEW ingest_rejections AS
SELECT run_id, j.key AS reason, split_part(j.key, ':', 1) AS category,
       CAST(j.value AS BIGINT) AS records
FROM runs_raw r,
     json_each(json_extract(r.manifest, '$.summary.rejection_reasons')) j
WHERE r.source = 'ingest'
  AND json_extract(r.manifest, '$.summary.rejection_reasons') IS NOT NULL;
