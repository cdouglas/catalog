---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Log format and conflict encodings: preliminary results'
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import q, table, theme, figures_module
theme()
f = figures_module()

MERGING = ["blind-append", "partition-vector", "manifest-window"]
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
FORMATS = {1: "format 1", 2: "format 2", 3: "format 3 (current)"}
CAMPAIGNS = ["c1-fa-scaling", "c2r-expiry", "c5-tables", "c6-openloop", "c8-fold", "c9-txn",
             "h1-shapes", "j1-fold", "g1-witness"]
# JMH runs carry no git_sha of their own; their builds are in NOTES.md "Known runs"
JMH_BUILDS = {"j1-fold": "1e86cf5, 343f37b, 03ab333 (one per format)"}

prov = q("""
    SELECT c.campaign, count(*) AS runs,
           string_agg(DISTINCT left(r.git_sha, 7), ', ' ORDER BY left(r.git_sha, 7)) AS builds,
           min(r.started_at)::DATE AS started
    FROM campaign_runs c JOIN canonical_runs r USING (run_id)
    WHERE list_contains($c, c.campaign) GROUP BY 1 ORDER BY started, campaign""", c=CAMPAIGNS)
prov["builds"] = [JMH_BUILDS.get(c, b) for c, b in zip(prov.campaign, prov.builds)]

# Record bytes: each workload captured anew in each record format; every 50th record decoded alone
# by the format explorer (source anatomy). An anatomy run's config hash is its capture's; the
# baseline's format-1 runs name the hash before new default keys changed it (NOTES.md).
records = q("""
    WITH caps AS (
      SELECT DISTINCT capture_hash,
             CASE WHEN list_contains(campaigns, 'i10-format') THEN 3
                  WHEN list_contains(campaigns, 'i9-format') THEN 2 ELSE 1 END AS format
      FROM canonical_runs
      WHERE list_has_any(campaigns, ['i8-shapes', 'i9-format', 'i10-format'])),
    anat AS (
      SELECT a.run_id, a.label, c.format
      FROM canonical_runs a JOIN caps c ON a.config_hash = c.capture_hash
      WHERE a.source = 'anatomy'
      UNION SELECT 'anatomy/cap-i1-b26a3a9fc131', 'cap-i1', 1)
    SELECT a.label, a.format, sum(g.bytes_per_record) AS bytes
    FROM anat a JOIN record_groups g USING (run_id) GROUP BY ALL""")
records["shape"] = records.label.map(SHAPES)
records["format_name"] = records["format"].map(FORMATS)
rec = records.pivot(index="shape", columns="format", values="bytes").reindex(ORDER)
rec["smaller_by"] = rec[1] / rec[3]
rec_table = rec.reset_index().rename(columns={1: "format 1 (B)", 2: "format 2 (B)",
                                              3: "format 3 (B)", "smaller_by": "1 / 3"})

def rec_at(shape, fmt):
    return float(rec.loc[shape, fmt])

# Replay: JMH IngestFoldBench.fold (batches of 64, the committed set at its 30-minute steady state
# for 1,000 records/s) over each build's own captures of the workloads (j1-fold)
fold = q("""
    SELECT CASE j.run_id WHEN 'jmh/j1-i8' THEN 1 WHEN 'jmh/j1-i9' THEN 2 ELSE 3 END AS format,
           'cap-' || regexp_extract(j.params['log'], 'cap-([a-z0-9-]+)-[0-9a-f]{12}', 1) AS label,
           j.score AS records_s, j.score_error AS error
    FROM jmh j JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'j1-fold') AND j.metric = 'records'""")
fold["shape"] = fold.label.map(SHAPES)
fold_wide = fold.pivot(index="shape", columns="format", values="records_s").reindex(ORDER)
fold_err = fold.pivot(index="shape", columns="format", values="error").reindex(ORDER)
fold_table = pd.DataFrame({
    FORMATS[k] + " (records/s)": [f"{v:,.0f} ± {e:,.0f}" for v, e in zip(fold_wide[k], fold_err[k])]
    for k in (1, 2, 3)}, index=fold_wide.index).reset_index()
fold3 = fold_wide[3]

# Checkpoints: h1-shapes, the same workloads ingested by each format's build on havoc, with a
# compaction every 100 records; the last checkpoint of each run
ckpt = q("""
    WITH last AS (
      SELECT * FROM compactions
      QUALIFY row_number() OVER (PARTITION BY run_id ORDER BY position DESC) = 1)
    SELECT CASE WHEN r.git_sha LIKE '1e86cf5%' THEN 1 WHEN r.git_sha LIKE '343f37b%' THEN 2
                ELSE 3 END AS format,
           p.value AS label, l.checkpoint_bytes, l.committed_bytes, l.state_bytes, l.inline_lists,
           k.encode_ms, k.decode_ms, m.compact_share
    FROM last l JOIN canonical_runs r USING (run_id)
    JOIN ingest_compaction_metrics k USING (run_id) JOIN ingest_metrics m USING (run_id)
    JOIN run_params p ON p.run_id = r.run_id AND p.key = 'capture.label'
    WHERE list_contains(r.campaigns, 'h1-shapes') AND r.compact_every = 100""")
ckpt["shape"] = ckpt.label.map(SHAPES)
state = ckpt.pivot(index="shape", columns="format", values="state_bytes").reindex(ORDER) / 1024
c3 = ckpt[ckpt["format"] == 3].set_index("shape").reindex(ORDER)
ckpt_table = pd.DataFrame({
    "state, format 1 (KB)": state[1].round(), "state, format 3 (KB)": state[3].round(),
    "committed set (MB)": (c3.committed_bytes / 2**20).round(1),
    "checkpoint (MB)": (c3.checkpoint_bytes / 2**20).round(1),
    "committed share": c3.committed_bytes / c3.checkpoint_bytes,
    "encode (ms)": c3.encode_ms.round(), "decode (ms)": c3.decode_ms.round(),
    "time compacting": c3.compact_share}).reset_index()
not_fast = [s for s in ORDER if s != "fast appends only"]
state_cut = 1 - state.loc[not_fast, 3] / state.loc[not_fast, 1]
committed_share_nf = (c3.committed_bytes / c3.checkpoint_bytes).loc[not_fast]
fast3 = c3.loc["fast appends only"]
```

What the contention, ingest and log-generator campaigns of October 2026 say about the catalog's
intention-log format and its conflict encodings, gathered for a progress report. No run here
touched real storage: the contention driver runs writers over an in-memory store whose requests
take S3 Express's (`s3x`) or S3's (`s3`) latencies from endive's provider profiles, and the ingest,
fold and log-generator runs have no storage at all. The catalog benchmark, next, measures real
stores and other catalogs. Each section names the campaign it reads; the builds are below.

```{code-cell} ipython3
:tags: [remove-input]

table(prov, caption="Campaigns this page reads (canonical runs)")
```

## The log format

### Records

Each workload was captured three times, once in each record format (16 writers under
manifest-window, 10 minutes, no store latency), and every 50th record decoded alone by the format
explorer. Format 2 keeps a snapshot's parent's summary entries instead of resending them as strings
and names removed manifests by index; format 3 writes a delta's new snapshot id once and names
manifests by UUID and number under their table's prefix (docs/FORMAT.md).

```{code-cell} ipython3
:label: progress-record-bytes
:tags: [remove-input]
:class: col-page-right

alt.Chart(records).mark_bar().encode(
    x=alt.X("bytes:Q", title="bytes per record (mean of every 50th record)"),
    y=alt.Y("shape:N", sort=ORDER, title=None),
    color=alt.Color("format_name:N", title="record format", sort=list(FORMATS.values())),
    yOffset=alt.YOffset("format_name:N", sort=list(FORMATS.values())),
).properties(width=420)
```

```{code-cell} ipython3
:tags: [remove-input]

table(rec_table, caption="Bytes per record by format; 1 / 3 is format 1's size over format 3's")
```

A record of the baseline workload (mostly merge appends to 16 tables) went from
{eval}`f"{rec_at('baseline, 16 tables', 1):.0f}"` bytes to
{eval}`f"{rec_at('baseline, 16 tables', 3):.0f}"`, and across the seven workloads records are
{eval}`f"{rec.smaller_by.min():.1f}"`-{eval}`f"{rec.smaller_by.max():.1f}"` times smaller in
format 3; most for one hot table ({eval}`f"{rec_at('one table', 1):,.0f}"` to
{eval}`f"{rec_at('one table', 3):.0f}"` bytes), whose merges removed many manifests, each named by
its path in format 1.

### Replay

```{code-cell} ipython3
:tags: [remove-input]

table(fold_table, caption="One thread's fold rate (JMH IngestFoldBench.fold, j1-fold; ± is a 99.9% half-interval)")
```

Smaller records did not make replay slower. One thread folds the baseline at
{eval}`f"{fold_wide.loc['baseline, 16 tables', 1]:,.0f}"` records/s in format 1 and
{eval}`f"{fold_wide.loc['baseline, 16 tables', 3]:,.0f}"` in format 3 (batches of 64, the committed
set at its steady state for 1,000 records/s and a 30-minute retention), and the workloads fold
alike in every format within their errors but frequent merges
({eval}`f"{fold_wide.loc['merge at 2 manifests', 1]:,.0f}"` to
{eval}`f"{fold_wide.loc['merge at 2 manifests', 3]:,.0f}"`, with format 1's error
{eval}`f"{fold_err.loc['merge at 2 manifests', 1]:,.0f}"`). In format 3 the rate runs from
{eval}`f"{fold3.min():,.0f}"` records/s (fast appends, whose manifest lists grow with every
snapshot) to {eval}`f"{fold3.max():,.0f}"` (merges at 2 manifests, which keep every list short):
{eval}`f"{1e6 / fold3.loc['baseline, 16 tables']:.0f}"` µs a record on the baseline.

### Checkpoints

```{code-cell} ipython3
:tags: [remove-input]

table(ckpt_table, caption="The last checkpoint of each h1-shapes run with a compaction every 100 records (format 3 unless named)")
```

The catalog's state (its tables, without the committed set) is
{eval}`f"{100 * state_cut.min():.0f}"`-{eval}`f"{100 * state_cut.max():.0f}"`% smaller in format 3
for every workload but fast appends, at {eval}`f"{state.loc[not_fast, 3].min():.0f}"`-{eval}`f"{state.loc[not_fast, 3].max():.0f}"`
KB. Two parts of a checkpoint are not yet in shape, and both are measure-first items for the
catalog benchmark:

- The committed set (the ids of recent commits, which make a duplicate append harmless) is
  {eval}`f"{c3.committed_bytes.median() / 2**20:.1f}"` MB at 1,000 records/s and a 30-minute
  retention: {eval}`f"{100 * committed_share_nf.min():.0f}"`-{eval}`f"{100 * committed_share_nf.max():.0f}"`%
  of every checkpoint but fast appends'. With a compaction every 100 records, encoding checkpoints
  took {eval}`f"{100 * c3.compact_share.min():.0f}"`-{eval}`f"{100 * c3.compact_share.max():.0f}"`%
  of ingest time. Paging it out of the checkpoint is errata D16.
- Under fast appends the state is {eval}`f"{fast3.state_bytes / 2**20:.1f}"` MB, of which
  {eval}`f"{fast3.inline_lists / 2**20:.1f}"` MB is the snapshots' manifest lists, each stored
  whole though it is its parent's with one manifest more (errata D18).

```{code-cell} ipython3
:tags: [remove-cell]

# Contention on the newest build that ran the scaling grid: c8-fold's after build (56facd8)
c8 = q("""
    SELECT r.store, r.encoding, r.writers, count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.p50_ms) AS p50_ms, avg(m.cpu_ms) AS cpu_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM run_metrics m JOIN canonical_runs r USING (run_id) JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c8-fold') AND r.git_sha LIKE '56facd8%'
    GROUP BY ALL ORDER BY r.store, r.writers, r.encoding""")

def c8_at(store, encoding, writers, col="commits_s"):
    row = c8[(c8.store == store) & (c8.encoding == encoding) & (c8.writers == writers)]
    return float(row[col].iloc[0])

c8m = c8[c8.encoding.isin(MERGING)]
one_writer = c8[(c8.store == "s3x") & (c8.writers == 1)].commits_s
merge_s3x = c8m[(c8m.store == "s3x") & (c8m.writers >= 4)]
merge_exh = c8m[(c8m.store == "s3x")].exhausted_share.max()
merge_exh_text = "none" if merge_exh == 0 else f"at most {100 * merge_exh:.1f}%"
merge_s3_32 = c8m[(c8m.store == "s3") & (c8m.writers == 32)].commits_s.mean()

mix = q("""
    SELECT r.encoding, m.kind, count(*) AS runs,
           sum(m.committed) / sum(m.ops) AS committed_share,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.commits_s) AS commits_s
    FROM kind_metrics m JOIN canonical_runs r USING (run_id) JOIN run_workload w USING (run_id)
    WHERE list_contains(r.campaigns, 'c2r-expiry') AND r.writers = 16
      AND w.workload = 'compaction+fast_append' AND m.kind IN ('compaction', 'fast_append')
    GROUP BY ALL""")

def mix_at(encoding, kind, col):
    row = mix[(mix.encoding == encoding) & (mix.kind == kind)]
    return float(row[col].iloc[0])

tables = q("""
    SELECT r.tables, r.encoding, count(*) AS runs, avg(m.commits_s) AS commits_s
    FROM run_metrics m JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c5-tables') GROUP BY ALL ORDER BY r.encoding, r.tables""")

def tab(n, encoding):
    return float(tables[(tables.tables == n) & (tables.encoding == encoding)].commits_s.iloc[0])

endive = q("""
    SELECT r.arrival_rate, 'fileio-catalog (measured)' AS source, avg(m.commits_s) AS commits_s
    FROM kind_metrics m JOIN canonical_runs r USING (run_id) JOIN run_workload w USING (run_id)
    WHERE list_contains(r.campaigns, 'c6-openloop') AND r.source = 'contention' AND r.store = 's3x'
      AND r.encoding = 'partition-vector' AND w.workload = 'fa' AND m.kind = 'fast_append'
    GROUP BY ALL
    UNION ALL
    SELECT r.arrival_rate, 'endive (predicted)', avg(e.commits_s)
    FROM endive_metrics e JOIN canonical_runs r USING (run_id)
    WHERE r.store = 's3x' AND r.encoding = 'partition-vector' AND r.workload = 'fa'
      AND e.kind = 'fast_append'
    GROUP BY ALL ORDER BY 1, 2""")

def endive_at(rate, source):
    row = endive[(endive.arrival_rate == rate) & (endive.source.str.startswith(source))]
    return float(row.commits_s.iloc[0])

# The log generator (g1-witness): the share of landed entries that apply, 16 writers, contended
eff = q("""
    SELECT t.trace_label AS workload, t.trace_encoding AS encoding, t.trace_writers AS writers,
           t.entries, t.rejected, (t.entries - t.rejected) / t.entries AS applied_share
    FROM trace_ingest t JOIN canonical_runs USING (run_id)
    WHERE list_contains(campaigns, 'g1-witness') AND NOT t.trace_oracle""")
WORKLOADS = {"t1-fa": "fast appends", "t1-mix": "mix", "t1-txn": "catalog transactions"}
eff["workload_name"] = eff.workload.map(WORKLOADS)
eff16 = eff[eff.writers == 16]

def applied(workload, encoding):
    return float(eff16[(eff16.workload == workload) & (eff16.encoding == encoding)].applied_share.iloc[0])

fa_lamport_family = eff16[(eff16.workload == "t1-fa") & ~eff16.encoding.isin(MERGING)]
fa_merging = eff16[(eff16.workload == "t1-fa") & eff16.encoding.isin(MERGING)]
eff_table = eff16.pivot(index="encoding", columns="workload_name", values="applied_share") \
    .reindex(f.ENCODINGS)[list(WORKLOADS.values())].reset_index()

witness = q("""
    SELECT t.trace_encoding AS encoding, f.records_s,
           CASE WHEN f.run_id LIKE '%proto' THEN '9446029' ELSE '8d9277a' END AS build
    FROM trace_fold f JOIN trace_ingest t USING (trace_dir) JOIN canonical_runs c ON c.run_id = f.run_id
    WHERE list_contains(c.campaigns, 'g1-witness') AND t.trace_label = 't1-fa'
      AND t.trace_writers = 16 AND t.trace_oracle""")

def wfold(encoding, build):
    row = witness[(witness.encoding == encoding) & (witness.build == build)]
    return float(row.records_s.iloc[0])

# Catalog transactions: g1's client-side failures and c9-txn by width
txn_g1 = q("""
    SELECT t.trace_encoding AS encoding,
           CAST(json_extract(r.manifest, '$.capture.manifest.summary.outcomes.catalog_txn.committed') AS BIGINT) AS committed,
           CAST(json_extract(r.manifest, '$.capture.manifest.summary.outcomes.catalog_txn.validation_failed') AS BIGINT) AS failed_client_side,
           CAST(json_extract(r.manifest, '$.capture.manifest.summary.outcomes.catalog_txn.retries_exhausted') AS BIGINT) AS retries_exhausted
    FROM trace_ingest t JOIN runs_raw r USING (run_id) JOIN canonical_runs USING (run_id)
    WHERE list_contains(campaigns, 'g1-witness') AND t.trace_label = 't1-txn'
      AND t.trace_writers = 16 AND NOT t.trace_oracle""")
txn_g1["client_share"] = txn_g1.failed_client_side / (
    txn_g1.committed + txn_g1.failed_client_side + txn_g1.retries_exhausted)

c9 = q("""
    SELECT CAST(json_extract_string(r.params, '$.txn_tables') AS INT) AS txn_tables,
           json_extract_string(r.params, '$.txn_isolation') AS isolation, r.encoding,
           count(*) AS runs,
           sum(m.committed) / sum(m.ops) AS committed_share,
           sum(m.real_conflicts) / sum(m.ops) AS failed_validation,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.commits_s) AS commits_s
    FROM kind_metrics m JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c9-txn') AND m.kind = 'catalog_txn'
      AND json_extract_string(r.params, '$.table_dist') = 'uniform'
    GROUP BY ALL ORDER BY txn_tables, isolation, r.encoding""")
c9_8 = c9[c9.txn_tables == 8]
c9_1 = c9[c9.txn_tables == 1]
```

## Conflict encodings

Every encoding commits what Iceberg's own retry would commit (design.md I12, checked by the
conflict-encoding oracle); they differ in how many records conflict, and so in attempts, exhausted
retries and throughput. `lamport` conflicts on the table's whole version; `blind-append`,
`partition-vector` and `manifest-window` (the default) merge a record onto commits that landed
since its writer read the table when Iceberg's retry would commit the same.

### What each encoding lets apply

The log generator (campaign `g1-witness`) runs writers over one in-memory shared log, taking turns
as a seeded scheduler picks them, so a log depends on its configuration and seed alone: no store,
no clock. A record that loses lands in the log, replay rejects it, and its writer retries. The
share of landed entries that apply is the log's efficiency.

```{code-cell} ipython3
:label: progress-applied
:tags: [remove-input]
:class: col-page-right

alt.Chart(eff16).mark_bar().encode(
    x=alt.X("applied_share:Q", title="entries that applied (share of landed), 16 writers",
            axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("encoding:N", sort=f.ENCODINGS, title=None),
    color=f.encoding_color(),
    row=alt.Row("workload_name:N", title=None, sort=list(WORKLOADS.values())))
```

```{code-cell} ipython3
:tags: [remove-input]

table(eff_table, caption="Share of landed entries that applied, 16 writers (g1-witness, contended traces)")
```

Under fast appends at 16 writers, `lamport` and the two encodings that relax only properties and
other refs reject {eval}`f"{100 * (1 - fa_lamport_family.applied_share.mean()):.0f}"`% of the entries
that land; the merging encodings reject
{eval}`f"{100 * (1 - fa_merging.applied_share.mean()):.1f}"`%, only those that race a snapshot
expiry. In the mix of fast appends, validated overwrites, compactions, property changes and catalog
transactions, `manifest-window` applies {eval}`f"{100 * applied('t1-mix', 'manifest-window'):.1f}"`%
of entries against {eval}`f"{100 * applied('t1-mix', 'lamport'):.1f}"`% for `lamport`.

The finer witness costs replay little once implemented with care: one thread folds the fast-append
log (16 writers, every entry applied) at {eval}`f"{wfold('manifest-window', '9446029'):,.0f}"`
entries/s under `manifest-window` against `lamport`'s {eval}`f"{wfold('lamport', '9446029'):,.0f}"`
(JMH on a laptop, 10-20% errors); before the fixes profiling these folds found (9446029) it was
{eval}`f"{wfold('manifest-window', '8d9277a'):,.0f}"`.

### Throughput under contention

```{code-cell} ipython3
:label: progress-commits
:tags: [remove-input]
:class: col-page-right

base = alt.Chart(c8).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    color=f.encoding_color())
(base.mark_line(point=True).encode(y=alt.Y("commits_s:Q", title="fast appends committed/s"))
 .properties(width=260, height=200).facet(column=alt.Column("store:N", title=None)))
```

```{code-cell} ipython3
:tags: [remove-input]

table(c8, caption="Fast appends to one table, closed load (c8-fold, build 56facd8; means over two runs)")
```

Fast appends to one table of 32 partitions, closed load, two 60 s runs per point (campaign
`c8-fold`, its newer build). On `s3x`, whose conditional append lets a commit append its record, one
writer commits {eval}`f"{one_writer.min():.1f}"`-{eval}`f"{one_writer.max():.1f}"`/s under any
encoding (`manifest-window`: a median commit of
{eval}`f"{c8_at('s3x', 'manifest-window', 1, 'p50_ms'):.0f}"` ms). As writers are added, `lamport`
falls to {eval}`f"{c8_at('s3x', 'lamport', 32):.1f}"` commits/s at 32, and
{eval}`f"{100 * c8_at('s3x', 'lamport', 16, 'exhausted_share'):.0f}"`% of its operations exhaust
Iceberg's retries at 16 writers ({eval}`f"{100 * c8_at('s3x', 'lamport', 32, 'exhausted_share'):.0f}"`%
at 32). The merging encodings rise to {eval}`f"{merge_s3x.commits_s.min():.1f}"`-{eval}`f"{merge_s3x.commits_s.max():.1f}"`/s
from 4 to 32 writers, exhausting {eval}`merge_exh_text`; `manifest-window` commits
{eval}`f"{c8_at('s3x', 'manifest-window', 16):.1f}"`/s at 16 writers. On `s3`, where every commit
replaces the whole catalog object, races for the object bound every encoding: the merging ones
commit {eval}`f"{merge_s3_32:.1f}"`/s at 32 writers against `lamport`'s
{eval}`f"{c8_at('s3', 'lamport', 32):.1f}"`.

Other operations racing fast appends, 16 writers on `s3x` (campaign `c2r-expiry`, keeping 30 s of
snapshot history so that long jobs can validate): a compaction (a 5 s job rewriting one partition)
commits {eval}`f"{100 * mix_at('manifest-window', 'compaction', 'committed_share'):.0f}"`% of the
time under `manifest-window` and
{eval}`f"{100 * mix_at('lamport', 'compaction', 'committed_share'):.0f}"`% under `lamport`, where
{eval}`f"{100 * mix_at('lamport', 'compaction', 'exhausted_share'):.0f}"`% exhaust their retries;
and since a committed compaction keeps the table's manifest lists short, the fast appends beside
them commit {eval}`f"{mix_at('manifest-window', 'fast_append', 'commits_s'):.1f}"`/s against
{eval}`f"{mix_at('lamport', 'fast_append', 'commits_s'):.1f}"`.

Several tables in one catalog object (campaign `c5-tables`, 16 writers, fast appends): commits to
different tables never conflict in replay, but race for the object's append. `blind-append` commits
{eval}`f"{tab(1, 'blind-append'):.1f}"`/s on one table, {eval}`f"{tab(4, 'blind-append'):.1f}"` on 4
and {eval}`f"{tab(16, 'blind-append'):.1f}"` on 16; `lamport` {eval}`f"{tab(1, 'lamport'):.1f}"`,
{eval}`f"{tab(4, 'lamport'):.1f}"` and {eval}`f"{tab(16, 'lamport'):.1f}"`.

Against endive, a discrete-event model of Iceberg's optimistic commits on these stores (campaign
`c6-openloop`, open load, 128 writers): under `partition-vector` on `s3x` fileio-catalog keeps up
with endive's prediction at 20 transactions/s ({eval}`f"{endive_at(20, 'fileio'):.1f}"` commits/s
against {eval}`f"{endive_at(20, 'endive'):.1f}"`) and falls behind at 40
({eval}`f"{endive_at(40, 'fileio'):.1f}"` against {eval}`f"{endive_at(40, 'endive'):.1f}"`): endive
models a commit as a few requests of fixed latency, while each of ours also reads and replays a
catalog object that grows with the table.

### Catalog transactions

```{code-cell} ipython3
:tags: [remove-input]

table(c9, caption="Catalog transactions racing fast appends on 16 tables, 16 writers on s3x, uniform tables (c9-txn)")
```

In the log generator's transaction workload (4-table transactions racing fast appends, 16
writers), {eval}`f"{100 * txn_g1.client_share.min():.0f}"`-{eval}`f"{100 * txn_g1.client_share.max():.0f}"`%
of catalog transactions fail a check before their writer appends anything (their serializable read
set, or an Iceberg requirement), whatever the encoding: no encoding sees them. On `s3x` (campaign
`c9-txn`), single-table transactions commit
{eval}`f"{100 * c9_1.committed_share.min():.0f}"`-{eval}`f"{100 * c9_1.committed_share.max():.0f}"`%
of the time and 8-table ones {eval}`f"{100 * c9_8.committed_share.min():.0f}"`-{eval}`f"{100 * c9_8.committed_share.max():.0f}"`%,
under either encoding and either isolation level. A transaction's width, not the encoding, decides
it; the next lever is judging the transaction's read set in the log, at a finer grain than a
table's version.

## Caveats

- No storage: the contention runs' latencies are endive's profiles of S3 Express and S3 over an
  in-memory store; the ingest, fold and generator runs have none.
- The contention campaigns ran on builds of 2026-10-27, before the replay-cost fixes since; at 16
  and 32 writers the merging encodings kept their havoc shards' CPUs busy, reading a catalog object
  that grows with the table (`cpu_bound` in the table above).
- Each contention point is two runs of 60 s; the JMH folds are one fork each (errors in their
  tables).
- Correctness is not measured on this page: the protocol is checked by TLA+ models, fuzzers
  calibrated against mutants, invariants I1-I12 and the real-cloud suites (docs/design.md,
  spec/tla/README.md).
