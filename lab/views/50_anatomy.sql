-- Record bytes by what they carry (source anatomy, fact record_fields), per capture: the groups
-- docs/FORMAT.md ranks. A snapshot id counts once per field it appears in: the new snapshot's own
-- id, its parent's, its ref's, and each added or removed manifest's (the snapshot whose list it
-- changes); expiry's ids (RemoveSnapshots) are other. A summary's extra entries are split by key
-- (path{key}), and what it takes of its parent's (inherits_parent_extra, dropped_extra) counts
-- with them. Bytes include each field's tag and length.
CREATE OR REPLACE TABLE record_groups AS
SELECT
  run_id,
  CASE
    WHEN path = 'CompactSummary.extra{iceberg-version}' THEN 'summary: iceberg-version'
    WHEN path LIKE 'CompactSummary.extra%' OR path IN (
        'CompactSummary.inherits_parent_extra', 'CompactSummary.dropped_extra')
      THEN 'summary: other entries'
    WHEN path LIKE 'CompactSummary.%' THEN 'summary counts'
    WHEN path IN ('AddSnapshot.snapshot_id', 'AddSnapshot.parent_snapshot_id',
                  'SetSnapshotRef.snapshot_id', 'AddManifestDelta.snapshot_id',
                  'RemoveManifestDelta.snapshot_id', 'ManifestFileEntry.added_snapshot_id')
      THEN 'snapshot ids'
    WHEN path LIKE 'RemoveManifestDelta.%' OR path = 'TableMetadataUpdate.remove_manifest'
      THEN 'removed manifests'
    WHEN path IN ('ManifestFileEntry.manifest_path_suffix', 'ManifestFileEntry.manifest_path',
                  'ManifestFileEntry.manifest_uuid', 'ManifestFileEntry.manifest_number')
      THEN 'new manifest paths'
    WHEN path LIKE 'ManifestFileEntry.%' OR path LIKE 'AddManifestDelta.%'
      OR path LIKE 'PartitionFieldSummaryEntry.%' OR path = 'TableMetadataUpdate.add_manifest'
      THEN 'new manifests, other fields'
    WHEN path LIKE 'LogRecord.%' OR path = 'txn_length' THEN 'envelope'
    ELSE 'other'
  END AS part,
  sum(bytes_per_record) AS bytes_per_record
FROM record_fields
GROUP BY ALL;
