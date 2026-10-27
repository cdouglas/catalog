---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Append count and tables
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()
ENC = ["lamport", "blind-append"]

knobs = q("""
    SELECT r.max_append_count, r.writers, r.encoding,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.p50_ms) AS p50_ms,
           avg(m.p99_ms) AS p99_ms,
           avg(m.cpu_ms) AS cpu_ms,
           avg(m.appends_lost) AS appends_lost,
           avg(m.cas_lost) AS cas_lost,
           avg(k.catalog_bytes) / 1e6 AS catalog_mb,
           avg(k.records) AS records,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN catalog_metrics k USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c4-knobs')
    GROUP BY ALL ORDER BY r.writers, r.max_append_count, r.encoding""")
knobs["append count"] = knobs.max_append_count.map(lambda n: "CAS every commit" if n == 0 else str(n))

tables = q("""
    SELECT r.tables, r.encoding,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.p50_ms) AS p50_ms,
           avg(m.cpu_ms) AS cpu_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c5-tables')
    GROUP BY ALL ORDER BY r.tables, r.encoding""")

def knob(writers, count, encoding, col="commits_s"):
    row = knobs[(knobs.writers == writers) & (knobs.max_append_count == count)
                & (knobs.encoding == encoding)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

def tab(n, encoding, col="commits_s"):
    row = tables[(tables.tables == n) & (tables.encoding == encoding)]
    return float(row[col].iloc[0]) if len(row) else float("nan")
```

`fileio.catalog.max.append.count` (campaign `c4-knobs`, p6): how many records a writer appends to the
catalog object before it compacts it (CAS of a checkpoint and its record). 0 compacts on every
commit, as on a store without conditional append. Fast appends on `s3x`, 4 and 16 writers. A
smaller count keeps the object small, so a read replays fewer records; a larger one compacts
less often, and every compaction races the appends of the others.

At 4 writers, `blind-append` commits {eval}`f"{knob(4, 0, 'blind-append'):.1f}"`/s replacing the
object on every commit, {eval}`f"{knob(4, 100, 'blind-append'):.1f}"`/s at 100 and
{eval}`f"{knob(4, 1000, 'blind-append'):.1f}"`/s at 1000; at 16,
{eval}`f"{knob(16, 0, 'blind-append'):.1f}"`, {eval}`f"{knob(16, 100, 'blind-append'):.1f}"` and
{eval}`f"{knob(16, 1000, 'blind-append'):.1f}"`. `lamport` moves less: its conflicts, not the
object, bound it, and at 16 writers it does best replacing on every commit
({eval}`f"{knob(16, 0, 'lamport'):.1f}"`/s against {eval}`f"{knob(16, 100, 'lamport'):.1f}"`).
100 is the catalog's default since 2026-10-27; the best count depends on the store and the catalog
(errata D14).

```{code-cell} ipython3
:label: c4-commits
:tags: [remove-input]
:class: col-page-right

alt.Chart(knobs).mark_bar().encode(
    x=alt.X("append count:N", sort=["CAS every commit", "100", "1000"], title="max.append.count"),
    xOffset=alt.XOffset("encoding:N", sort=f.ENCODINGS),
    y=alt.Y("commits_s:Q", title="commits/s"),
    color=f.encoding_color(only=ENC),
).properties(width=240, height=180).facet(column=alt.Column("writers:O", title="writers"))
```

```{code-cell} ipython3
:label: c4-cpu
:tags: [remove-input]
:class: col-page-right

alt.Chart(knobs).mark_bar().encode(
    x=alt.X("append count:N", sort=["CAS every commit", "100", "1000"], title="max.append.count"),
    xOffset=alt.XOffset("encoding:N", sort=f.ENCODINGS),
    y=alt.Y("cpu_ms:Q", title="writer CPU per operation (ms)"),
    color=f.encoding_color(only=ENC),
).properties(width=240, height=180).facet(column=alt.Column("writers:O", title="writers"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(knobs.drop(columns=["append count"]), caption="Per writer count, append count and encoding")
```

Tables on one catalog object (campaign `c5-tables`, p6): 16 writers append to 1, 4 or 16 tables, all
recorded in the one catalog object. Commits to different tables never conflict in replay, but
they still race for the object's append. `blind-append` goes from
{eval}`f"{tab(1, 'blind-append'):.1f}"` commits/s on one table to {eval}`f"{tab(4, 'blind-append'):.1f}"`
on 4 and {eval}`f"{tab(16, 'blind-append'):.1f}"` on 16 (past 4 tables an operation takes as
long whatever the count, so 16 writers in a closed loop commit as many);
`lamport` from {eval}`f"{tab(1, 'lamport'):.1f}"` to {eval}`f"{tab(4, 'lamport'):.1f}"` and
{eval}`f"{tab(16, 'lamport'):.1f}"`, as spreading the writers over tables spreads its conflicts.

```{code-cell} ipython3
:label: c5-commits
:tags: [remove-input]

alt.Chart(tables).mark_line(point=True).encode(
    x=alt.X("tables:Q", scale=alt.Scale(type="log", base=2), title="tables"),
    y=alt.Y("commits_s:Q", title="commits/s, all tables"),
    color=f.encoding_color(only=ENC),
).properties(width=300, height=200)
```

```{code-cell} ipython3
:tags: [remove-input]

table(tables, caption="Per table count and encoding")
```
