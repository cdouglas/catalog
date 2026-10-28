---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Folding a read onto a replica
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()

df = q("""
    SELECT CASE WHEN r.git_sha LIKE '56facd8%' THEN 'after' ELSE 'before' END AS build,
           r.store, r.encoding, r.writers,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.cpu_ms) AS cpu_ms,
           avg(m.p50_ms) AS p50_ms,
           avg(m.p99_ms) AS p99_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c8-fold')
    GROUP BY ALL ORDER BY r.store, r.encoding, r.writers, build""")

def at(build, store, encoding, writers, col):
    row = df[(df.build == build) & (df.store == store) & (df.encoding == encoding)
             & (df.writers == writers)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

ratio = df.pivot_table(index=["store", "encoding", "writers"], columns="build",
                       values=["commits_s", "cpu_ms"]).reset_index()
ratio.columns = ["_".join(c).strip("_") for c in ratio.columns]
ratio["commits_gain"] = ratio.commits_s_after / ratio.commits_s_before
ratio["cpu_ratio"] = ratio.cpu_ms_after / ratio.cpu_ms_before
s3x16 = ratio[(ratio.store == "s3x") & (ratio.writers >= 16)]
merging = ["blind-append", "partition-vector", "manifest-window"]
s3x_merge = s3x16[s3x16.encoding.isin(merging)]
s3x_lamport = s3x16[~s3x16.encoding.isin(merging)]
s3x_all = ratio[ratio.store == "s3x"]
s3_all = ratio[ratio.store == "s3"]
s3_one = s3_all[s3_all.writers == 1]
```

Since 56facd8 a replica keeps the view it was replayed from, and a later read that continues
it byte for byte replays only the records after it (docs/LOG.md "Replay"); before, every read
decoded the checkpoint and replayed every record after it. Campaign `c8-fold` runs c1's grid (fast
appends; `s3x` and `s3`; every encoding; 1, 4, 16 and 32 writers; two runs each) on both builds,
at once on one host (havoc, 3 shards of 16 CPUs per build), with the defaults of 4e65a65:
`max.append.count` 100 and a 100 ms backoff after a lost race.

On `s3x` every point commits more after: by a factor of {eval}`f"{s3x_all.commits_gain.min():.2f}"`
to {eval}`f"{s3x_all.commits_gain.max():.2f}"`. A single writer under `manifest-window` goes from
{eval}`f"{at('before', 's3x', 'manifest-window', 1, 'commits_s'):.1f}"` to
{eval}`f"{at('after', 's3x', 'manifest-window', 1, 'commits_s'):.1f}"` commits/s (median latency
{eval}`f"{at('before', 's3x', 'manifest-window', 1, 'p50_ms'):.0f}"` to
{eval}`f"{at('after', 's3x', 'manifest-window', 1, 'p50_ms'):.0f}"` ms): its read back of its own
append replays one record. At 16 and 32 writers the merging encodings (`blind-append`,
`partition-vector`, `manifest-window`) gain a factor of {eval}`f"{s3x_merge.commits_gain.min():.2f}"`
to {eval}`f"{s3x_merge.commits_gain.max():.2f}"` (`manifest-window`:
{eval}`f"{at('before', 's3x', 'manifest-window', 16, 'commits_s'):.1f}"` to
{eval}`f"{at('after', 's3x', 'manifest-window', 16, 'commits_s'):.1f}"` at 16 writers,
{eval}`f"{at('before', 's3x', 'manifest-window', 32, 'commits_s'):.1f}"` to
{eval}`f"{at('after', 's3x', 'manifest-window', 32, 'commits_s'):.1f}"` at 32); the others, bound
by their conflicts (about a quarter of operations exhaust their retries at 16 writers and 40% at
32, before and after), gain {eval}`f"{s3x_lamport.commits_gain.min():.2f}"` to
{eval}`f"{s3x_lamport.commits_gain.max():.2f}"` while their CPU an operation falls to
{eval}`f"{s3x_lamport.cpu_ratio.min():.2f}"`-{eval}`f"{s3x_lamport.cpu_ratio.max():.2f}"` of what it
was.

The merging encodings at 16 and 32 writers still spend
{eval}`f"{s3x_merge.cpu_ms_after.min():.0f}"` to {eval}`f"{s3x_merge.cpu_ms_after.max():.0f}"` ms of
CPU an operation and most of their shards' CPUs at 32 writers (`run_cpu`): the fold removed the
checkpoint from most reads, not the rest of the work, which this campaign does not break down.
Under fast appends a table's inline lists grow with its manifests, which fast appends never merge,
and a load builds every retained snapshot's list (docs/REPLAY_BENCH_RESULTS.md).

On `s3` every commit replaces the object, so a commit's read back of its own write starts from
the checkpoint it wrote. Throughput moves by a factor of {eval}`f"{s3_all.commits_gain.min():.2f}"`
to {eval}`f"{s3_all.commits_gain.max():.2f}"`, and CPU an operation is
{eval}`f"{s3_all.cpu_ratio.min():.2f}"` to {eval}`f"{s3_all.cpu_ratio.max():.2f}"` times what it was,
lowest for one writer ({eval}`f"{s3_one.cpu_ratio.min():.2f}"` to
{eval}`f"{s3_one.cpu_ratio.max():.2f}"`), whose reads between its commits find nothing new since the
replica it holds.

```{code-cell} ipython3
:label: c8-commits
:tags: [remove-input]
:class: col-page-right

alt.Chart(df).mark_line(point=True).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    y=alt.Y("commits_s:Q", title="commits/s"),
    color=f.encoding_color(),
    strokeDash=alt.StrokeDash("build:N", sort=["before", "after"], title="build"),
).properties(width=220, height=180).facet(column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:label: c8-cpu
:tags: [remove-input]
:class: col-page-right

alt.Chart(df).mark_line(point=True).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    y=alt.Y("cpu_ms:Q", title="writer CPU per operation (ms)"),
    color=f.encoding_color(),
    strokeDash=alt.StrokeDash("build:N", sort=["before", "after"], title="build"),
).properties(width=220, height=180).facet(column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df, caption="Per build, store, encoding and writer count (means over two runs)")
```
