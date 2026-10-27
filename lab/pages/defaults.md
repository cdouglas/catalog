---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Choosing the defaults
---

```{code-cell} ipython3
:tags: [remove-cell]

import pandas as pd
from labbook import q, table, figures_module
f = figures_module()

def per_encoding(sql, **params):
    return q(sql, **params).set_index("encoding").reindex(f.ENCODINGS)

fa4 = per_encoding("""
    SELECT r.encoding, avg(m.commits_s) AS v FROM run_metrics m JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c1-fa-scaling') AND r.store = 's3x' AND r.writers = 4
    GROUP BY ALL""")
exh = per_encoding("""
    SELECT r.encoding, sum(m.exhausted) / sum(m.ops) AS v FROM run_metrics m
    JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c1-fa-scaling') AND r.writers >= 16 GROUP BY ALL""")
kinds = """
    SELECT r.encoding, sum(m.committed) / sum(m.ops) AS v FROM kind_metrics m
    JOIN canonical_runs r USING (run_id) JOIN run_workload w USING (run_id)
    WHERE list_contains(r.campaigns, $c) AND m.kind = $k AND w.workload = $w AND r.writers = 16
    GROUP BY ALL"""
compaction = per_encoding(kinds, c="c2-mixes", k="compaction", w="compaction+fast_append")
overwrite = per_encoding(kinds, c="c2-mixes", k="validated_overwrite", w="favo10")
c3 = per_encoding("""
    SELECT r.encoding, sum(m.committed) / sum(m.ops) AS v FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c3-partitions') AND m.kind = 'validated_overwrite'
    GROUP BY ALL""")
summary = pd.DataFrame({
    "fast appends/s, 4 writers (s3x)": fa4.v,
    "operations exhausting retries, 16-32 writers": exh.v,
    "compactions committed, 16 writers": compaction.v,
    "overwrites committed, 16 writers": overwrite.v,
    "overwrites committed, partitions campaign": c3.v,
}).reset_index()

knobs = q("""
    SELECT r.max_append_count, r.encoding, r.writers, avg(m.commits_s) AS commits_s,
           avg(m.p50_ms) AS p50_ms, avg(m.p99_ms) AS p99_ms, avg(m.cpu_ms) AS cpu_ms,
           avg(k.catalog_bytes) / 1e6 AS catalog_mb
    FROM run_metrics m JOIN canonical_runs r USING (run_id) JOIN catalog_metrics k USING (run_id)
    WHERE list_contains(r.campaigns, 'c4-knobs') GROUP BY ALL
    ORDER BY r.writers, r.encoding, r.max_append_count""")
```

What the campaigns say about `fileio.catalog.conflict.encoding` and
`fileio.catalog.max.append.count`, from the pages before this one. Every encoding commits what
Iceberg's retry would (design.md I12); they differ in how many records conflict, and so in how
many attempts, how many exhausted retries, and how much throughput a table gets.

```{code-cell} ipython3
:label: defaults-encodings
:tags: [remove-input]

table(summary, caption="Per encoding (means over the runs named; shares of operations)")
```

The caveats of the pages apply: on havoc's 4-CPU shards, the merging encodings were CPU-bound
at 16 and 32 writers (so their throughput there is a floor), and the driver's maintenance kept
only 50 snapshots, which failed long compactions and overwrites whatever the encoding.

`max.append.count` (fast appends on `s3x`):

```{code-cell} ipython3
:label: defaults-append-count
:tags: [remove-input]

table(knobs, caption="Per writer count, encoding and max.append.count")
```
