---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Backoff after a lost race
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()
ENC = ["lamport", "manifest-window"]

df = q("""
    SELECT r.store, r.encoding, r.writers,
           CAST(json_extract(r.params::JSON, '$.race_min_wait_ms') AS INTEGER) AS race_min_wait_ms,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.cas_lost) AS cas_lost,
           avg(m.appends_lost) AS appends_lost,
           avg(m.p50_ms) AS p50_ms,
           avg(m.p99_ms) AS p99_ms,
           avg(m.cpu_ms) AS cpu_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c7-backoff')
    GROUP BY ALL ORDER BY r.store, r.writers, r.encoding, race_min_wait_ms""")
df["backoff"] = df.race_min_wait_ms.map(lambda ms: "none" if ms == 0 else f"{ms} ms")

def at(store, writers, encoding, ms, col):
    row = df[(df.store == store) & (df.writers == writers) & (df.encoding == encoding)
             & (df.race_min_wait_ms == ms)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

mw = lambda store, writers, ms, col: at(store, writers, "manifest-window", ms, col)
lp = lambda store, writers, ms, col: at(store, writers, "lamport", ms, col)
```

A writer that loses a race for the catalog object (another writer's append or replace landed
first) reads it again and resends. Since 2026-10-27 the commit loop first waits a random time
below a bound that starts at `fileio.catalog.race.retry.min-wait-ms` and doubles with each race
the commit loses, up to 1 s (campaign `c7-backoff`: fast appends, 16 and 32 writers, no backoff
and bounds of 20 and 100 ms, two runs per point, p6). On `s3` every commit replaces the object,
so every lost race costs a read of all of it.

Under `manifest-window`, the backoff pays on both stores, more with the larger bound. On `s3x` at 32
writers it commits {eval}`f"{mw('s3x', 32, 0, 'commits_s'):.1f}"`/s with none,
{eval}`f"{mw('s3x', 32, 20, 'commits_s'):.1f}"` at 20 ms and
{eval}`f"{mw('s3x', 32, 100, 'commits_s'):.1f}"` at 100 ms. On `s3` at 32 writers its throughput
barely moves ({eval}`f"{mw('s3', 32, 0, 'commits_s'):.1f}"`,
{eval}`f"{mw('s3', 32, 20, 'commits_s'):.1f}"`, {eval}`f"{mw('s3', 32, 100, 'commits_s'):.1f}"`), but
the races an operation loses fall from {eval}`f"{mw('s3', 32, 0, 'cas_lost'):.1f}"` to
{eval}`f"{mw('s3', 32, 20, 'cas_lost'):.1f}"` and {eval}`f"{mw('s3', 32, 100, 'cas_lost'):.1f}"`, its
CPU from {eval}`f"{mw('s3', 32, 0, 'cpu_ms'):.0f}"` ms to {eval}`f"{mw('s3', 32, 20, 'cpu_ms'):.0f}"`
and {eval}`f"{mw('s3', 32, 100, 'cpu_ms'):.0f}"`, and the operations that exhaust Iceberg's retries
from {eval}`f"{100 * mw('s3', 32, 0, 'exhausted_share'):.1f}"`% to
{eval}`f"{100 * mw('s3', 32, 20, 'exhausted_share'):.1f}"`% and
{eval}`f"{100 * mw('s3', 32, 100, 'exhausted_share'):.1f}"`%; its p99 latency rises.

Under `lamport` the backoff costs throughput on `s3x`
({eval}`f"{lp('s3x', 16, 0, 'commits_s'):.1f}"`, {eval}`f"{lp('s3x', 16, 20, 'commits_s'):.1f}"` and
{eval}`f"{lp('s3x', 16, 100, 'commits_s'):.1f}"` commits/s at 16 writers), so a `lamport` catalog
on an appending store may do better without it (`fileio.catalog.race.retry.min-wait-ms` 0).

```{code-cell} ipython3
:label: c7-commits
:tags: [remove-input]
:class: col-page-right

alt.Chart(df).mark_bar().encode(
    x=alt.X("backoff:N", sort=["none", "20 ms", "100 ms"], title="backoff bound"),
    xOffset=alt.XOffset("encoding:N", sort=f.ENCODINGS),
    y=alt.Y("commits_s:Q", title="commits/s"),
    color=f.encoding_color(only=ENC),
).properties(width=160, height=160).facet(
    column=alt.Column("writers:O", title="writers"), row=alt.Row("store:N", title=None))
```

```{code-cell} ipython3
:label: c7-lost
:tags: [remove-input]
:class: col-page-right

df["lost"] = df.cas_lost + df.appends_lost
alt.Chart(df).mark_bar().encode(
    x=alt.X("backoff:N", sort=["none", "20 ms", "100 ms"], title="backoff bound"),
    xOffset=alt.XOffset("encoding:N", sort=f.ENCODINGS),
    y=alt.Y("lost:Q", title="races lost per operation"),
    color=f.encoding_color(only=ENC),
).properties(width=160, height=160).facet(
    column=alt.Column("writers:O", title="writers"), row=alt.Row("store:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df.drop(columns=["backoff", "lost"]), caption="Per store, writer count, encoding and backoff")
```
