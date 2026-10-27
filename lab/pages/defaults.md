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
compaction = per_encoding(kinds, c="c2r-expiry", k="compaction", w="compaction+fast_append")
overwrite = per_encoding(kinds, c="c2r-expiry", k="validated_overwrite", w="favo10")
c3 = per_encoding("""
    SELECT r.encoding, sum(m.committed) / sum(m.ops) AS v FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c3-partitions') AND m.kind = 'validated_overwrite'
    GROUP BY ALL""")
summary = pd.DataFrame({
    "fast appends/s, 4 writers (s3x)": fa4.v,
    "operations exhausting retries, 16-32 writers": exh.v,
    "compactions committed, 16 writers (30 s history)": compaction.v,
    "overwrites committed, 16 writers (30 s history)": overwrite.v,
    "overwrites committed, partitions campaign": c3.v,
}).reset_index()

knobs = q("""
    SELECT r.max_append_count, r.encoding, r.writers, avg(m.commits_s) AS commits_s,
           avg(m.p50_ms) AS p50_ms, avg(m.p99_ms) AS p99_ms, avg(m.cpu_ms) AS cpu_ms,
           avg(k.catalog_bytes) / 1e6 AS catalog_mb
    FROM run_metrics m JOIN canonical_runs r USING (run_id) JOIN catalog_metrics k USING (run_id)
    WHERE list_contains(r.campaigns, 'c4-knobs') GROUP BY ALL
    ORDER BY r.writers, r.encoding, r.max_append_count""")

backoff = q("""
    SELECT r.store, r.writers, r.encoding,
           CAST(json_extract(r.params::JSON, '$.race_min_wait_ms') AS INTEGER) AS race_min_wait_ms,
           avg(m.commits_s) AS commits_s, avg(m.cas_lost + m.appends_lost) AS races_lost,
           avg(m.cpu_ms) AS cpu_ms, sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.p99_ms) AS p99_ms
    FROM run_metrics m JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'c7-backoff') GROUP BY ALL
    ORDER BY r.store, r.writers, r.encoding, race_min_wait_ms""")
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

These are the p6 runs (fileio-catalog 8636dda, havoc shards of 16 CPUs). The caveats of the
pages apply: at 16 and 32 writers the merging encodings still kept their shards' CPUs busy,
reading a catalog object that grows with the table (the read path is the next bound); the
compactions and overwrites are those of `c2r-expiry`, which keeps 30 s of history, since keeping
50 snapshots failed long jobs whatever the encoding.

`max.append.count` (fast appends on `s3x`):

```{code-cell} ipython3
:label: defaults-append-count
:tags: [remove-input]

table(knobs, caption="Per writer count, encoding and max.append.count")
```

The backoff after a lost race (`fileio.catalog.race.retry.min-wait-ms`, fast appends):

```{code-cell} ipython3
:label: defaults-backoff
:tags: [remove-input]

table(backoff, caption="Per store, writer count, encoding and backoff bound (ms)")
```
