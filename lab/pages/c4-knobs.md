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
```

`fileio.catalog.max.append.count` (campaign `c4-knobs`): how many records a writer appends to the
catalog object before it compacts it (CAS of a checkpoint and its record). 0 compacts on every
commit, as on a store without conditional append. Fast appends on `s3x`, 4 and 16 writers. A
smaller count keeps the object small, so a read replays fewer records; a larger one compacts
less often, and every compaction races the appends of the others.

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

Tables on one catalog object (campaign `c5-tables`): 16 writers append to 1, 4 or 16 tables, all
recorded in the one catalog object. Commits to different tables never conflict in replay, but
they still race for the object's append.

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
