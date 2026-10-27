---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Fast appends as writers scale
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()

df = q("""
    SELECT r.store, r.encoding, r.writers,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           avg(m.attempts_per_commit) AS attempts,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.fp_share) AS fp_share,
           avg(m.p50_ms) AS p50_ms,
           avg(m.p99_ms) AS p99_ms,
           avg(m.cpu_ms) AS cpu_ms,
           avg(c.busy) AS busy,
           bool_or(c.cpu_bound) AS cpu_bound,
           avg(k.max_catalog_bytes) / 1e6 AS catalog_mb
    FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_cpu c USING (run_id)
    JOIN catalog_metrics k USING (run_id)
    WHERE list_contains(r.campaigns, 'c1-fa-scaling')
    GROUP BY ALL ORDER BY r.store, r.writers, r.encoding""")
df["bound"] = df.cpu_bound.map({True: "CPU-bound", False: "not CPU-bound"})

def at(store, encoding, writers, col):
    row = df[(df.store == store) & (df.encoding == encoding) & (df.writers == writers)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

MERGING = ["blind-append", "partition-vector", "manifest-window"]
merging = df[df.encoding.isin(MERGING)]
lamport_peak = df[(df.store == "s3x") & (df.encoding == "lamport")].commits_s.max()
lamport_exh_16 = at("s3x", "lamport", 16, "exhausted_share")
lamport_exh_32 = at("s3x", "lamport", 32, "exhausted_share")
merge_s3x_4 = merging[(merging.store == "s3x") & (merging.writers == 4)].commits_s.mean()
merge_s3x_32 = merging[(merging.store == "s3x") & (merging.writers == 32)].commits_s.mean()
high = merging[(merging.store == "s3x") & (merging.writers >= 16)]
merge_busy_high = high.busy.mean()
merge_cpu_high = high.cpu_ms.mean()
merge_mb_16 = merging[(merging.store == "s3x") & (merging.writers == 16)].catalog_mb.mean()
lamport_mb_16 = at("s3x", "lamport", 16, "catalog_mb")
merge_cpu_4 = merging[(merging.store == "s3x") & (merging.writers == 4)].cpu_ms.mean()
merge_s3_32 = merging[(merging.store == "s3") & (merging.writers == 32)].commits_s.mean()
lamport_s3_32 = at("s3", "lamport", 32, "commits_s")

races = q("""
    SELECT avg(o.cas_lost) AS cas_lost
    FROM ops o JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c1-fa-scaling') AND r.store = 's3' AND r.writers = 32
      AND r.encoding IN ('blind-append', 'partition-vector', 'manifest-window')
      AND o.measured AND o.kind = 'fast_append' AND o.outcome = 'committed'""")
merge_cas_lost = float(races.cas_lost[0])
# p5, superseded: the same configurations before the backoff, on 4-CPU shards
p5 = q("""
    SELECT avg(m.commits_s) AS commits_s FROM run_metrics m JOIN runs r USING (run_id)
    WHERE r.label = 'c1-fa-scaling' AND r.git_sha LIKE '2afeb46%' AND r.store = 's3'
      AND r.writers = 32
      AND r.encoding IN ('blind-append', 'partition-vector', 'manifest-window')""")
p5_merge_s3_32 = float(p5.commits_s[0])
```

Fast appends only, one table, closed load: each writer appends one file to a random one of 32
partitions, thinks, and appends again (campaign `c1-fa-scaling`, p6: two runs per point, 60 s
measured after a 10 s warmup, shards of 16 CPUs). `s3x` is S3 Express, whose conditional append
lets a commit append its record; `s3` is a general-purpose bucket, where every commit replaces the
catalog object (CAS).

Under `lamport` (and the two encodings that relax only properties and other branches, which fast
appends to one branch don't touch), throughput on `s3x` peaks at
{eval}`f"{lamport_peak:.1f}"` commits/s, and from 16 writers many operations exhaust Iceberg's
retries: {eval}`f"{100 * lamport_exh_16:.0f}"`% at 16 writers, {eval}`f"{100 * lamport_exh_32:.0f}"`%
at 32. The merging encodings (`blind-append`, `partition-vector`, `manifest-window`) merge
concurrent appends, so their commits take one attempt: on `s3x` they reach
{eval}`f"{merge_s3x_4:.1f}"` commits/s at 4 writers and stay near it to 32
({eval}`f"{merge_s3x_32:.1f}"` at 32), exhausting none.

What bounds them there is CPU: at 16 and 32 writers their runs kept the shard's CPUs
{eval}`f"{100 * merge_busy_high:.0f}"`% busy over the whole run, at
{eval}`f"{merge_cpu_high:.0f}"` ms of writer CPU per operation (against
{eval}`f"{merge_cpu_4:.0f}"` at 4 writers). The CPU goes to reading the catalog, and the catalog
grows with throughput: fast appends never merge manifests, so a faster run ends with a table of
more manifests, and every read decodes a checkpoint whose inline manifest lists grow with
snapshots × manifests. At 16 writers the object reached {eval}`f"{merge_mb_16:.1f}"` MB under the
merging encodings, {eval}`f"{lamport_mb_16:.1f}"` MB under `lamport`. Writers on their own
machines would not queue for CPUs, but each read would still cost that much: the read path
(docs/REPLAY_BENCH_RESULTS.md, "What remains") is the next bound. Hollow points mark runs whose
CPUs were more than 60% busy (`run_cpu`).

On `s3`, every commit replaces the object: one CAS wins per round, and a writer that loses reads
all of it again. With the commit loop's backoff after a lost race (see the backoff page), the
merging encodings commit {eval}`f"{merge_s3_32:.1f}"`/s at 32 writers against `lamport`'s
{eval}`f"{lamport_s3_32:.1f}"`, a committed operation losing {eval}`f"{merge_cas_lost:.1f}"` races
on average; in p5, without the backoff and on 4-CPU shards, they fell to
{eval}`f"{p5_merge_s3_32:.1f}"`.

```{code-cell} ipython3
:label: c1-commits
:tags: [remove-input]
:class: col-page-right

base = alt.Chart(df).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    color=f.encoding_color())
lines = base.mark_line().encode(y=alt.Y("commits_s:Q", title="commits/s"))
tooltip = ["store", "encoding", "writers", alt.Tooltip("commits_s:Q", format=".1f"),
           alt.Tooltip("busy:Q", format=".0%", title="CPU busy")]
free = base.mark_point(size=60, filled=True).encode(y="commits_s:Q", tooltip=tooltip) \
    .transform_filter("!datum.cpu_bound")
bound = base.mark_point(size=60, filled=False, strokeWidth=2).encode(
    y="commits_s:Q", tooltip=tooltip).transform_filter("datum.cpu_bound")
(lines + free + bound).properties(width=280, height=220).facet(
    column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:label: c1-exhausted
:tags: [remove-input]
:class: col-page-right

alt.Chart(df).mark_line(point=True).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    y=alt.Y("exhausted_share:Q", axis=alt.Axis(format="%"), title="operations that exhausted retries"),
    color=f.encoding_color()).properties(width=280, height=200).facet(
        column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:label: c1-latency
:tags: [remove-input]
:class: col-page-right

alt.Chart(df).mark_line(point=True).encode(
    x=alt.X("writers:Q", scale=alt.Scale(type="log", base=2), title="writers"),
    y=alt.Y("p99_ms:Q", scale=alt.Scale(type="log"), title="p99 latency of commits (ms)"),
    color=f.encoding_color()).properties(width=280, height=200).facet(
        column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df.drop(columns=["bound"]), caption="Per store, encoding and writer count (means over runs)")
```
