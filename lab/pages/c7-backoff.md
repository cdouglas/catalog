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
```

A writer that loses a race for the catalog object (another writer's append or replace landed
first) reads it again and resends. Since 2026-10-27 the commit loop first waits a random time
below a bound that starts at `fileio.catalog.race.retry.min-wait-ms` and doubles with each race
the commit loses, up to 1 s (campaign `c7-backoff`: fast appends, 16 and 32 writers, no backoff
and bounds of 20 and 100 ms, two runs per point). On `s3` every commit replaces the object, so
every lost race costs a read of all of it.

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
