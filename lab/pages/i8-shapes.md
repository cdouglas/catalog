---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Ingest across workload shapes
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme
theme()

SHAPES = {
    "cap-i1": "baseline, 16 tables",
    "cap-s-onetable": "one table",
    "cap-s-zipf": "zipf over tables",
    "cap-s-fast": "fast appends only",
    "cap-s-merge2": "merge at 2 manifests",
    "cap-s-txn": "catalog txns, schema",
    "cap-s-txn4": "4-table catalog txns",
}
ORDER = list(SHAPES.values())

runs = q("""
    SELECT r.run_id, p.value AS capture, r.capture_hash, r.capture_records, r.compact_every,
           m.ingested, m.record_bytes, m.fold_records_s, m.fold_records_s_late, m.records_s,
           m.records_s_late, m.compact_share
    FROM canonical_runs r JOIN ingest_metrics m USING (run_id)
    JOIN run_params p ON p.run_id = r.run_id AND p.key = 'capture.label'
    WHERE list_contains(r.campaigns, 'i8-shapes')""")
runs["shape"] = runs.capture.map(SHAPES)
fold = runs[runs.compact_every == 0].copy()
compacting = runs[runs.compact_every == 100].copy()

windows = q("""
    SELECT p.value AS capture, w.position, w.records / (w.fold_ms / 1000) AS records_s
    FROM windows w JOIN canonical_runs r USING (run_id)
    JOIN run_params p ON p.run_id = r.run_id AND p.key = 'capture.label'
    WHERE list_contains(r.campaigns, 'i8-shapes') AND r.compact_every = 0""")
windows["shape"] = windows.capture.map(SHAPES)

checkpoints = q("""
    WITH last AS (
      SELECT * FROM compactions
      QUALIFY row_number() OVER (PARTITION BY run_id ORDER BY position DESC) = 1)
    SELECT p.value AS capture, l.checkpoint_bytes, l.committed_bytes, l.state_bytes,
           l.inline_pool, l.inline_lists, l.inline_metadata,
           l.state_bytes - l.inline_pool - l.inline_lists - l.inline_metadata AS state_other,
           l.checkpoint_bytes - l.committed_bytes - l.state_bytes AS envelope, l.pool_entries,
           l.lists, k.encode_ms, k.decode_ms
    FROM last l JOIN ingest_compaction_metrics k USING (run_id) JOIN canonical_runs r USING (run_id)
    JOIN run_params p ON p.run_id = r.run_id AND p.key = 'capture.label'
    WHERE list_contains(r.campaigns, 'i8-shapes') AND r.compact_every = 100""")
checkpoints["shape"] = checkpoints.capture.map(SHAPES)
parts = checkpoints.melt(
    id_vars=["shape"], value_name="bytes", var_name="part",
    value_vars=["committed_bytes", "inline_pool", "inline_lists", "inline_metadata",
                "state_other", "envelope"])
parts["part"] = parts.part.map({
    "committed_bytes": "committed set", "inline_pool": "manifest pool",
    "inline_lists": "manifest lists", "inline_metadata": "table metadata",
    "state_other": "rest of the state", "envelope": "envelope and tail"})
parts["MB"] = parts.bytes / 1e6

# record anatomy: the anatomy run of each shape's capture (its config hash is the capture's; the
# baseline's runs name the hash before new default keys changed it, NOTES.md "Record anatomy")
ANATOMY = """
    SELECT run_id FROM canonical_runs
    WHERE source = 'anatomy' AND (config_hash IN (
      SELECT capture_hash FROM canonical_runs WHERE list_contains(campaigns, 'i8-shapes'))
       OR run_id = 'anatomy/cap-i1-b26a3a9fc131')"""
records = q(f"""
    SELECT a.label AS capture, g.part, g.bytes_per_record
    FROM record_groups g JOIN canonical_runs a USING (run_id)
    WHERE run_id IN ({ANATOMY})""")
records["shape"] = records.capture.map(SHAPES)
FIELDS = ["removed manifests", "new manifest paths", "new manifests, other fields", "snapshot ids",
          "summary: iceberg-version", "summary: other entries", "summary counts", "envelope",
          "other"]
records["order"] = records.part.map(FIELDS.index)
counts = q(f"""
    SELECT a.label AS capture,
      json_extract(a.manifest, '$.summary.sampled')::INT AS sampled,
      json_extract(a.manifest, '$.summary.p99')::INT AS p99,
      sum(f.count) FILTER (WHERE f.path = 'AddSnapshot.snapshot_id') AS snapshots,
      sum(f.count) FILTER (WHERE f.path = 'RemoveManifestDelta.manifest_path_suffix') AS removed,
      sum(f.bytes) FILTER (WHERE f.path LIKE 'CompactSummary.extra%') AS extra_bytes,
      sum(f.bytes) FILTER (WHERE f.path = 'CompactSummary.extra{{iceberg-version}}') AS version_bytes,
      sum(f.bytes) AS bytes
    FROM record_fields f JOIN canonical_runs a USING (run_id)
    WHERE run_id IN ({ANATOMY}) GROUP BY ALL""").set_index("capture")
removed_bytes = records[records.part == "removed manifests"].set_index("capture").bytes_per_record

def fold_rate(capture, col="fold_records_s"):
    return float(fold[fold.capture == capture][col].iloc[0])

def compact_rate(capture, col="records_s"):
    return float(compacting[compacting.capture == capture][col].iloc[0])

def ckpt(capture, col):
    return float(checkpoints[checkpoints.capture == capture][col].iloc[0])

def part(capture, name):
    row = records[(records.capture == capture) & (records.part == name)]
    return float(row.bytes_per_record.iloc[0]) if len(row) else 0.0

def share(capture, *names):
    c = counts.loc[capture]
    return 100 * sum(part(capture, n) for n in names) * c.sampled / c.bytes

SUMMARY = ("summary: iceberg-version", "summary: other entries")

def per_snapshot(capture):
    c = counts.loc[capture]
    return c.extra_bytes / c.snapshots

def version_per_snapshot(capture):
    c = counts.loc[capture]
    return c.version_bytes / c.snapshots

def per_removal(capture):
    c = counts.loc[capture]
    return removed_bytes[capture] * c.sampled / c.removed

committed_share = 100 * (checkpoints.committed_bytes / checkpoints.checkpoint_bytes)
others = checkpoints[checkpoints.capture != "cap-s-fast"]
```

How one thread's ingest of the intention log, its checkpoints and its records depend on the
workload that wrote the log: campaign `i8-shapes` replays seven captures (each 16 writers for
10 minutes under manifest-window, with no store latency; NOTES.md, "The ingest benchmark"), in
batches of 64 records at 1,000 records/s with the committed set at its 30-minute steady state,
without compactions and with one every 100 records. The baseline is the capture every earlier
ingest campaign replayed; each other shape changes one thing about it.

```{code-cell} ipython3
:tags: [remove-input]

table(fold.assign(order=fold["shape"].map(ORDER.index)).sort_values("order")[
          ["shape", "capture", "capture_records", "record_bytes"]],
      caption="The captures: records each writer landed, and their mean bytes")
```

## Without compactions

```{code-cell} ipython3
:label: shapes-fold
:tags: [remove-input]
:class: col-page-right

base = alt.Chart(fold).encode(y=alt.Y("shape:N", sort=ORDER, title=None))
(base.mark_bar().encode(x=alt.X("fold_records_s:Q", title="records/s"))
 + base.mark_tick(thickness=2, size=18, color="black").encode(x="fold_records_s_late:Q")
).properties(width=360, height=200)
```

Folding the baseline's log ingests {eval}`f"{fold_rate('cap-i1'):,.0f}"` records/s (bars: the whole
log; ticks: its second half). The rate tracks how long the tables' manifest lists grow, since
replay builds each new snapshot's list from its parent's: merging at 2 manifests, which keeps
every list short, ingests {eval}`f"{fold_rate('cap-s-merge2'):,.0f}"`, while fast appends, which
never merge, ingest {eval}`f"{fold_rate('cap-s-fast'):,.0f}"` and slow as the lists grow, to
{eval}`f"{fold_rate('cap-s-fast', 'fold_records_s_late'):,.0f}"` over the second half of a log a
third as long as the others. One table ingests {eval}`f"{fold_rate('cap-s-onetable'):,.0f}"`, and
skewed tables {eval}`f"{fold_rate('cap-s-zipf'):,.0f}"`.

```{code-cell} ipython3
:label: shapes-over-log
:tags: [remove-input]
:class: col-page-right

alt.Chart(windows).mark_line().encode(
    x=alt.X("position:Q", title="records ingested"),
    y=alt.Y("records_s:Q", scale=alt.Scale(type="log"), title="records/s (window of 2,000)"),
    color=alt.Color("shape:N", sort=ORDER, title="shape"),
).properties(width=420, height=220)
```

```{code-cell} ipython3
:tags: [remove-input]

table(fold.assign(order=fold["shape"].map(ORDER.index)).sort_values("order")[
          ["shape", "ingested", "fold_records_s", "fold_records_s_late"]],
      caption="Per shape: records/s folding the whole log, and its second half")
```

## With a compaction every 100 records

```{code-cell} ipython3
:label: shapes-compaction
:tags: [remove-input]
:class: col-page-right

alt.Chart(parts).mark_bar().encode(
    x=alt.X("MB:Q", title="last checkpoint (MB)"),
    y=alt.Y("shape:N", sort=ORDER, title=None),
    color=alt.Color("part:N", title="part"),
    order=alt.Order("part:N"),
).properties(width=360, height=200)
```

A compaction encodes the whole checkpoint on the ingest thread, and with one every 100 records it
takes {eval}`f"{100 * compacting.compact_share.min():.0f}"`-{eval}`f"{100 * compacting.compact_share.max():.0f}"`%
of the time in every shape: ingest falls to
{eval}`f"{compacting.records_s.min():,.0f}"`-{eval}`f"{compacting.records_s.max():,.0f}"` records/s.
The checkpoints are the committed set: {eval}`f"{others.committed_bytes.min() / 1e6:.1f}"`-{eval}`f"{others.committed_bytes.max() / 1e6:.1f}"` MB
of every shape's but fast appends' (the 1.8 million ids of 30 minutes at 1,000 records/s), against
a state of {eval}`f"{others.state_bytes.min() / 1e3:,.0f}"`-{eval}`f"{others.state_bytes.max() / 1e3:,.0f}"` KB.
Fast appends' state is {eval}`f"{ckpt('cap-s-fast', 'state_bytes') / 1e6:.1f}"` MB, of which
{eval}`f"{ckpt('cap-s-fast', 'inline_lists') / 1e6:.1f}"` MB is the
{eval}`f"{ckpt('cap-s-fast', 'lists'):,.0f}"` snapshots' manifest lists, each written in full
though it is its parent's with one manifest more. Paging the set out
([COMMITTED_PAGING.md](../../docs/COMMITTED_PAGING.md)) is the larger cut for every shape, and
lists encoded against their parents' (errata D18) the one fast appends need.

```{code-cell} ipython3
:tags: [remove-input]

table(checkpoints.assign(order=checkpoints["shape"].map(ORDER.index)).sort_values("order")[
          ["shape", "checkpoint_bytes", "committed_bytes", "state_bytes", "inline_pool",
           "inline_lists", "pool_entries", "lists", "encode_ms", "decode_ms"]]
      .merge(compacting[["shape", "records_s", "compact_share"]], on="shape"),
      caption="Per shape: the last checkpoint's bytes by part, the mean encode and decode (ms), "
              "and records/s with compactions")
```

## Records

```{code-cell} ipython3
:label: shapes-records
:tags: [remove-input]
:class: col-page-right

alt.Chart(records).mark_bar().encode(
    x=alt.X("bytes_per_record:Q", title="bytes per record"),
    y=alt.Y("shape:N", sort=ORDER, title=None),
    color=alt.Color("part:N", title="field", sort=FIELDS,
                    scale=alt.Scale(domain=FIELDS, scheme="tableau10")),
    order=alt.Order("order:Q"),
).properties(width=360, height=200)
```

Every 50th record of each capture, decoded by the format explorer
(`docs/format-explorer/anatomy.mjs`; a field's bytes include its tag and length), in the format
before records referred to their parents (campaign `i9-format` captures the same workloads in the
format after; docs/FORMAT.md, "What a record holds"). Two fields carry
most of the bytes. A snapshot's summary entries beyond the counts the format keeps as numbers,
which it sends as Iceberg's strings, cost {eval}`f"{per_snapshot('cap-i1'):.0f}"` bytes per
snapshot in the baseline and as much in every shape:
{eval}`f"{share('cap-i1', *SUMMARY):.0f}"`% of the baseline's record bytes and
{eval}`f"{share('cap-s-fast', *SUMMARY):.0f}"`% of fast appends'. Of them,
`iceberg-version`, the same in every snapshot a writer makes, is
{eval}`f"{version_per_snapshot('cap-i1'):.0f}"` bytes; the rest are small counts under their
names (`changed-partition-count`, `manifests-created`, `manifests-replaced`, `manifests-kept`). A
removed manifest is
named by its path, {eval}`f"{per_removal('cap-i1'):.0f}"` bytes each besides its snapshot's id, and
merges on hot tables remove many: {eval}`f"{share('cap-s-onetable', 'removed manifests'):.0f}"`%
of one table's bytes, whose largest records (p99 {eval}`f"{int(counts.loc['cap-s-onetable'].p99):,}"`
bytes) are merges, {eval}`f"{share('cap-s-zipf', 'removed manifests'):.0f}"`% of skewed tables'
and {eval}`f"{share('cap-i1', 'removed manifests'):.0f}"`% of the baseline's. Snapshot ids, written
once per field that names the snapshot, add {eval}`f"{part('cap-i1', 'snapshot ids'):.0f}"` bytes to
the baseline's records and {eval}`f"{part('cap-s-onetable', 'snapshot ids'):.0f}"` to one table's.

```{code-cell} ipython3
:tags: [remove-input]

wide = records.pivot_table(index="shape", columns="part", values="bytes_per_record", aggfunc="sum")
table(wide.reindex(ORDER).round(0).reset_index(),
      caption="Per shape: mean bytes per record by field (snapshot ids: every field naming the "
              "new snapshot, its parent or its ref; removed manifests: their paths)")
```
