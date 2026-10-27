---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Partitions per operation
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()
ENC = ["lamport", "blind-append", "partition-vector", "manifest-window"]

df = q("""
    SELECT r.partitions_per_txn, r.partition_dist, r.encoding, m.kind,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.committed) / sum(m.ops) AS committed_share,
           sum(m.real_conflicts) / sum(m.ops) AS failed_share,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.p50_ms) AS p50_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c3-partitions')
      AND m.kind IN ('fast_append', 'validated_overwrite')
    GROUP BY ALL ORDER BY m.kind, r.partition_dist, r.partitions_per_txn, r.encoding""")
vo = df[df.kind == "validated_overwrite"]
fa = df[df.kind == "fast_append"]
```

Fast appends with 20% validated overwrites, 16 writers on `s3x`, each operation writing 1, 4 or
16 of the table's 32 partitions, chosen uniformly or by a Zipf distribution (campaign
`c3-partitions`, two runs per point). An overwrite validates, from the snapshot its job read 2 s
(median) earlier, that no data or deletes landed in its partitions; the encodings that judge it
by its partitions (`partition-vector`, `manifest-window`) merge it onto commits elsewhere, and
`lamport` and `blind-append` (which judges only blind records by partition) make Iceberg retry it.
Its validation fails, whatever the encoding, when a commit landed in one of its partitions.

```{code-cell} ipython3
:label: c3-overwrites
:tags: [remove-input]
:class: col-page-right

alt.Chart(vo).mark_line(point=True).encode(
    x=alt.X("partitions_per_txn:Q", scale=alt.Scale(type="log", base=2), title="partitions per operation"),
    y=alt.Y("committed_share:Q", axis=alt.Axis(format="%"), title="overwrites committed"),
    color=f.encoding_color(only=ENC),
).properties(width=260, height=200).facet(column=alt.Column("partition_dist:N", title=None))
```

```{code-cell} ipython3
:label: c3-fast-appends
:tags: [remove-input]
:class: col-page-right

alt.Chart(fa).mark_line(point=True).encode(
    x=alt.X("partitions_per_txn:Q", scale=alt.Scale(type="log", base=2), title="partitions per operation"),
    y=alt.Y("commits_s:Q", title="fast appends committed/s"),
    color=f.encoding_color(only=ENC),
).properties(width=260, height=200).facet(column=alt.Column("partition_dist:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df, caption="Per partition count, distribution, encoding and kind (means over runs)")
```
