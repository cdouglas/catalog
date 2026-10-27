-- A name for each run's operation mix, as endive's labels name it: fa (fast appends only),
-- favo<N> (fast appends with N% validated overwrites), else the mix's kinds joined by '+'.
CREATE OR REPLACE VIEW run_workload AS
SELECT run_id,
  CASE
    WHEN source = 'endive' THEN workload
    WHEN json_keys(mix::JSON) = ['fast_append'] THEN 'fa'
    WHEN list_sort(json_keys(mix::JSON)) = ['fast_append', 'validated_overwrite']
      THEN 'favo' || CAST(round(100 * CAST(json_extract(mix::JSON, '$.validated_overwrite') AS DOUBLE)) AS INTEGER)
    ELSE array_to_string(list_sort(json_keys(mix::JSON)), '+')
  END AS workload
FROM runs;
