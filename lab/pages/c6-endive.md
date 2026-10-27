---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Open load, against endive's predictions
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import q, table, theme, figures_module
theme()
f = figures_module()

measured = q("""
    SELECT r.store, w.workload, r.arrival_rate, r.encoding, m.kind,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           sum(m.real_conflicts) / sum(m.ops) AS failed_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.commit_ms) AS commit_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_workload w USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c6-openloop') AND r.source = 'contention'
      AND m.kind IN ('fast_append', 'validated_overwrite')
    GROUP BY ALL""")
measured["source"] = "fileio-catalog (measured)"
predicted = q("""
    SELECT r.store, r.workload, r.arrival_rate, r.encoding, e.kind,
           e.seeds AS runs,
           e.commits_s,
           e.exhausted / e.txns AS exhausted_share,
           e.real_conflicts / e.txns AS failed_share,
           e.attempts_per_commit AS attempts,
           e.commit_ms,
           false AS cpu_bound
    FROM endive_metrics e JOIN canonical_runs r USING (run_id)""")
predicted["source"] = "endive (predicted)"
df = pd.concat([measured, predicted], ignore_index=True)
df = df.sort_values(["store", "workload", "kind", "encoding", "arrival_rate", "source"])
fa = df[df.kind == "fast_append"]
vo = df[df.kind == "validated_overwrite"]

def get(frame, source, store, workload, rate, encoding, col):
    row = frame[(frame.source.str.startswith(source)) & (frame.store == store)
                & (frame.workload == workload) & (frame.arrival_rate == rate)
                & (frame.encoding == encoding)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

m_s3x_40 = get(fa, "fileio", "s3x", "fa", 40, "lamport", "commits_s")
e_s3x_40 = get(fa, "endive", "s3x", "fa", 40, "lamport", "commits_s")
m_s3_40 = get(fa, "fileio", "s3", "fa", 40, "lamport", "commits_s")
e_s3_40 = get(fa, "endive", "s3", "fa", 40, "lamport", "commits_s")
m_pv_s3x_20 = get(fa, "fileio", "s3x", "fa", 20, "partition-vector", "commits_s")
e_pv_s3x_20 = get(fa, "endive", "s3x", "fa", 20, "partition-vector", "commits_s")
m_pv_s3x_40 = get(fa, "fileio", "s3x", "fa", 40, "partition-vector", "commits_s")
e_pv_s3x_40 = get(fa, "endive", "s3x", "fa", 40, "partition-vector", "commits_s")
m_l_commit = get(fa, "fileio", "s3x", "fa", 20, "lamport", "commit_ms")
e_l_commit = get(fa, "endive", "s3x", "fa", 20, "lamport", "commit_ms")
m_pv_s3_20 = get(fa, "fileio", "s3", "fa", 20, "partition-vector", "commits_s")
m_l_s3_20 = get(fa, "fileio", "s3", "fa", 20, "lamport", "commits_s")
pv_s3_cpu = float(q("""
    SELECT avg(m.cpu_ms) AS cpu FROM kind_metrics m JOIN canonical_runs r USING (run_id)
    JOIN run_workload w USING (run_id)
    WHERE list_contains(r.campaigns, 'c6-openloop') AND r.source = 'contention'
      AND r.store = 's3' AND r.encoding = 'partition-vector' AND r.arrival_rate = 20
      AND w.workload = 'fa' AND m.kind = 'fast_append'""").cpu[0])
```

Open load: transactions arrive at a fixed rate (lognormal inter-arrival times, as endive draws
them) and wait for one of 128 writers (campaign `c6-openloop`, p6). The same configurations went to
endive, a discrete-event simulator of Iceberg's optimistic commits on these stores
(`bench/endive/gen.py` maps each run's store, workload, rate and encoding to endive's: S3 Express
commits by conditional append, S3 by CAS; `lamport` is endive's one-partition table,
`partition-vector` its 32 partitions). endive's rows are its steady state over an hour (two seeds);
fileio-catalog's are 60 s measured (two runs).

Under `partition-vector`, fileio-catalog keeps up with endive's prediction on `s3x` at 20
transactions/s ({eval}`f"{m_pv_s3x_20:.1f}"` commits/s against {eval}`f"{e_pv_s3x_20:.1f}"`), and
falls behind at 40 ({eval}`f"{m_pv_s3x_40:.1f}"` against {eval}`f"{e_pv_s3x_40:.1f}"`). Under
`lamport` it falls behind sooner: at 40/s it commits {eval}`f"{m_s3x_40:.1f}"`/s on `s3x` where
endive predicts {eval}`f"{e_s3x_40:.1f}"`, and {eval}`f"{m_s3_40:.1f}"`/s on `s3` where endive
predicts {eval}`f"{e_s3_40:.1f}"`. endive models a commit as a few requests of fixed latency;
fileio-catalog's commits also read and replay the catalog object, which grows with the table (see
the fast-append scaling page), so a commit takes longer (at 20/s on `s3x` under `lamport`,
{eval}`f"{m_l_commit:.0f}"` ms against endive's {eval}`f"{e_l_commit:.0f}"`), more commits overlap,
and under `lamport` each overlap is a conflict.

On `s3`, where every commit replaces the object, `partition-vector` does worse than `lamport` under
overload ({eval}`f"{m_pv_s3_20:.1f}"` against {eval}`f"{m_l_s3_20:.1f}"` commits/s at 20/s), even
with the backoff after a lost race: with 128 writers waiting, every lost race is a read of the
whole object, {eval}`f"{pv_s3_cpu:.0f}"` ms of CPU per operation. `lamport`'s conflicts go back to
Iceberg's retry, whose longer backoff throttles the writers. (The backoff page measures larger
bounds; errata D14.)

```{code-cell} ipython3
:label: c6-commits
:tags: [remove-input]
:class: col-page-right

alt.Chart(fa).mark_line(point=True).encode(
    x=alt.X("arrival_rate:Q", title="arrivals/s"),
    y=alt.Y("commits_s:Q", title="fast appends committed/s"),
    color=f.encoding_color(only=["lamport", "partition-vector"]),
    strokeDash=alt.StrokeDash("source:N", title=None),
    tooltip=["source", "store", "workload", "encoding", "arrival_rate",
             alt.Tooltip("commits_s:Q", format=".1f")]
).properties(width=220, height=180).facet(
    column=alt.Column("workload:N", title=None), row=alt.Row("store:N", title=None))
```

```{code-cell} ipython3
:label: c6-commit-latency
:tags: [remove-input]
:class: col-page-right

alt.Chart(fa).mark_line(point=True).encode(
    x=alt.X("arrival_rate:Q", title="arrivals/s"),
    y=alt.Y("commit_ms:Q", scale=alt.Scale(type="log"), title="mean commit latency of fast appends (ms)"),
    color=f.encoding_color(only=["lamport", "partition-vector"]),
    strokeDash=alt.StrokeDash("source:N", title=None),
).properties(width=220, height=180).facet(
    column=alt.Column("workload:N", title=None), row=alt.Row("store:N", title=None))
```

Validated overwrites (`favo10`): endive's one-partition `lamport` table aborts every overwrite that
overlaps another commit, which overstates the conflicts of a partitioned table; under
`partition-vector` both judge an overwrite by its partition.

```{code-cell} ipython3
:label: c6-overwrites
:tags: [remove-input]
:class: col-page-right

alt.Chart(vo).mark_line(point=True).encode(
    x=alt.X("arrival_rate:Q", title="arrivals/s"),
    y=alt.Y("commits_s:Q", title="validated overwrites committed/s"),
    color=f.encoding_color(only=["lamport", "partition-vector"]),
    strokeDash=alt.StrokeDash("source:N", title=None),
).properties(width=220, height=180).facet(column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df, caption="Measured and predicted, per store, workload, rate, encoding and kind")
```
