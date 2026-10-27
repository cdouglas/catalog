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

lamport_peak = df[(df.store == "s3x") & (df.encoding == "lamport")].commits_s.max()
blind_4 = at("s3x", "blind-append", 4, "commits_s")
lamport_exh_32 = at("s3x", "lamport", 32, "exhausted_share")
merge_busy_16 = df[(df.store == "s3x") & (df.writers == 16)
                   & df.encoding.isin(["blind-append", "partition-vector", "manifest-window"])].busy.mean()
s3_lamport_peak = df[(df.store == "s3") & (df.encoding == "lamport")].commits_s.max()
lamport_exh_16 = at("s3x", "lamport", 16, "exhausted_share")

races = q("""
    SELECT r.encoding, avg(o.cas_lost) AS cas_lost, avg(o.rejected) AS rejected,
           avg(o.conflicts) AS conflicts
    FROM ops o JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c1-fa-scaling') AND r.store = 's3' AND r.writers = 32
      AND o.measured AND o.kind = 'fast_append' AND o.outcome = 'committed'
    GROUP BY ALL""").set_index("encoding")
blind_cas_lost = races.loc["blind-append", "cas_lost"]
blind_rejected = races.loc["blind-append", "rejected"]
lamport_cas_lost = races.loc["lamport", "cas_lost"]
```

Fast appends only, one table, closed load: each writer appends one file to a random one of 32
partitions, thinks, and appends again (campaign `c1-fa-scaling`, two runs per point, 60 s
measured after a 10 s warmup). `s3x` is S3 Express, whose conditional append lets a commit append
its record; `s3` is a general-purpose bucket, where every commit replaces the catalog object (CAS).

Under `lamport` (and the two encodings that relax only properties and other branches, which fast
appends to one branch don't touch), throughput on `s3x` peaks at
{eval}`f"{lamport_peak:.1f}"` commits/s, and from 16 writers many operations exhaust Iceberg's
retries: {eval}`f"{100 * lamport_exh_16:.0f}"`% at 16 writers, {eval}`f"{100 * lamport_exh_32:.0f}"`%
at 32. The merging encodings
(`blind-append`, `partition-vector`, `manifest-window`) merge concurrent appends, so their commits
take one attempt; at 4 writers they reach {eval}`f"{blind_4:.1f}"` commits/s.

Past 4 writers the merging encodings' throughput falls, but this is the benchmark's host, not the
protocol: each shard had 4 CPUs, and at 16 writers those runs kept them
{eval}`f"{100 * merge_busy_16:.0f}"`% busy over the whole run (warmup and drain included). Hollow
points mark runs above 60% (`run_cpu`). Their CPU per operation also grows with throughput: fast
appends never merge manifests, so a faster run ends with a table of more manifests, and every
read decodes a checkpoint whose inline manifest lists grow with snapshots × manifests
(`catalog_mb`, the catalog object's largest size).

On `s3`, where every commit replaces the object, the merging encodings fall at 32 writers for a
reason of the protocol's: one CAS wins per round, and a writer that loses reads the object again
and resends at once, with no backoff, up to 10 writes before its commit fails back to Iceberg's
retry. A committed `blind-append` operation lost {eval}`f"{blind_cas_lost:.1f}"` CAS races and
failed {eval}`f"{blind_rejected:.2f}"` commits that way, on average; a `lamport` one lost
{eval}`f"{lamport_cas_lost:.1f}"`, since its conflicts go back to Iceberg's retry, whose backoff
throttles the writers. A randomized backoff after a lost race would do the same for every
encoding.

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

On `s3`, where every commit is a CAS of the whole object, `lamport` peaks at
{eval}`f"{s3_lamport_peak:.1f}"` commits/s.

```{code-cell} ipython3
:tags: [remove-input]

table(df.drop(columns=["bound"]), caption="Per store, encoding and writer count (means over runs)")
```
