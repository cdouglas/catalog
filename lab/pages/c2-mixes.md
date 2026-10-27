---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Operation mixes
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, figures_module
theme()
f = figures_module()

WORKLOADS = {
    "favo10": "10% validated overwrites",
    "compaction+fast_append": "5% compactions",
    "fast_append+property": "10% property sets",
    "fast_append+wap": "90% WAP inserts to own branch",
}
df = q("""
    SELECT w.workload, r.writers, r.encoding, m.kind,
           count(*) AS runs,
           avg(m.commits_s) AS commits_s,
           sum(m.committed) / sum(m.ops) AS committed_share,
           sum(m.real_conflicts) / sum(m.ops) AS failed_share,
           sum(m.exhausted) / sum(m.ops) AS exhausted_share,
           avg(m.attempts_per_commit) AS attempts,
           avg(m.p50_ms) AS p50_ms,
           avg(m.p99_ms) AS p99_ms,
           bool_or(c.cpu_bound) AS cpu_bound
    FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_workload w USING (run_id)
    JOIN run_cpu c USING (run_id)
    WHERE list_contains(r.campaigns, 'c2-mixes') AND m.kind <> 'expire'
    GROUP BY ALL ORDER BY w.workload, r.writers, m.kind, r.encoding""")
df["mix"] = df.workload.map(WORKLOADS).fillna(df.workload)
minority = df[df.kind != "fast_append"].copy()
fa = df[df.kind == "fast_append"].copy()
shares = minority.melt(
    id_vars=["mix", "writers", "encoding", "kind"],
    value_vars=["committed_share", "failed_share", "exhausted_share"],
    var_name="outcome", value_name="share")
shares["outcome"] = shares.outcome.map({
    "committed_share": "committed", "failed_share": "failed validation",
    "exhausted_share": "exhausted retries"})

def val(workload, writers, encoding, kind, col):
    row = df[(df.workload == workload) & (df.writers == writers) & (df.encoding == encoding)
             & (df.kind == kind)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

pv_compaction_16 = val("compaction+fast_append", 16, "partition-vector", "compaction", "committed_share")
lamport_compaction_16 = val("compaction+fast_append", 16, "lamport", "compaction", "committed_share")
pv_fa_16 = val("compaction+fast_append", 16, "partition-vector", "fast_append", "commits_s")
lamport_fa_16 = val("compaction+fast_append", 16, "lamport", "fast_append", "commits_s")
```

Fast appends with a minority of another kind of operation, on `s3x`, at 4 and 16 writers
(campaign `c2-mixes`, two runs per point). A validated overwrite is Spark's copy-on-write
overwrite of one partition under serializable isolation, validated from the snapshot its job read
2 s (median) earlier; a compaction rewrites one partition's files, its job taking 5 s; a property
set changes one table property; a WAP insert appends to the writer's own branch.

The validated operations are the ones whose job runs long enough for others to commit under
them. Under `partition-vector`, {eval}`f"{100 * pv_compaction_16:.0f}"`% of compactions commit at
16 writers, against {eval}`f"{100 * lamport_compaction_16:.0f}"`% under `lamport`, and since a
committed compaction merges the table's manifests, the catalog stays small and fast appends reach
{eval}`f"{pv_fa_16:.1f}"` commits/s, against {eval}`f"{lamport_fa_16:.1f}"`. Most failed overwrites
fail Iceberg's own validation, whatever the encoding: a fast append landed in their partition
while their job ran, which a serializable overwrite must not miss.

```{code-cell} ipython3
:label: c2-fast-appends
:tags: [remove-input]
:class: col-page-right

alt.Chart(fa).mark_bar().encode(
    x=alt.X("encoding:N", sort=f.ENCODINGS, title=None, axis=alt.Axis(labels=False, ticks=False)),
    y=alt.Y("commits_s:Q", title="fast appends committed/s"),
    color=f.encoding_color(),
    tooltip=["mix", "writers", "encoding", alt.Tooltip("commits_s:Q", format=".1f")]
).properties(width=150, height=160).facet(
    column=alt.Column("mix:N", title=None), row=alt.Row("writers:O", title="writers"))
```

```{code-cell} ipython3
:label: c2-minority-outcomes
:tags: [remove-input]
:class: col-page-right

alt.Chart(shares).mark_bar().encode(
    x=alt.X("encoding:N", sort=f.ENCODINGS, title=None),
    y=alt.Y("share:Q", axis=alt.Axis(format="%"), title="operations of the minority kind"),
    color=alt.Color("outcome:N", sort=["committed", "failed validation", "exhausted retries"],
                    title="outcome"),
    tooltip=["mix", "writers", "encoding", "outcome", alt.Tooltip("share:Q", format=".0%")]
).properties(width=150, height=160).facet(
    column=alt.Column("mix:N", title=None), row=alt.Row("writers:O", title="writers"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df.drop(columns=["mix"]), caption="Per mix, writer count, encoding and kind (means over runs)")
```

## Keeping 30 s of history

```{code-cell} ipython3
:tags: [remove-cell]

history = q("""
    SELECT w.workload, r.writers, r.encoding, m.kind,
           CASE WHEN list_contains(r.campaigns, 'c2r-expiry') THEN '30 s' ELSE '50 snapshots' END
             AS history,
           count(*) AS runs,
           sum(m.committed) / sum(m.ops) AS committed_share,
           sum(m.real_conflicts) / sum(m.ops) AS failed_share,
           avg(m.commits_s) AS commits_s
    FROM kind_metrics m
    JOIN canonical_runs r USING (run_id)
    JOIN run_workload w USING (run_id)
    WHERE (list_contains(r.campaigns, 'c2r-expiry') OR list_contains(r.campaigns, 'c2-mixes'))
      AND m.kind IN ('compaction', 'validated_overwrite')
    GROUP BY ALL ORDER BY w.workload, r.writers, m.kind, r.encoding, history""")
history["mix"] = history.workload.map(WORKLOADS).fillna(history.workload)
```

The driver's maintenance expires all but the last 50 snapshots every 5 s, which at 10 or more
commits/s keeps less history than a 5 s compaction needs: Iceberg fails a validated operation
whose starting snapshot expired ("Cannot determine history"), whatever the encoding. Campaign
`c2r-expiry` runs the compaction and overwrite mixes again, keeping every snapshot younger than
30 s.

```{code-cell} ipython3
:label: c2r-history
:tags: [remove-input]
:class: col-page-right

alt.Chart(history).mark_bar().encode(
    x=alt.X("history:N", title="history kept"),
    xOffset=alt.XOffset("encoding:N", sort=f.ENCODINGS),
    y=alt.Y("committed_share:Q", axis=alt.Axis(format="%"), title="committed"),
    color=f.encoding_color(),
).properties(width=160, height=160).facet(
    column=alt.Column("mix:N", title=None), row=alt.Row("writers:O", title="writers"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(history.drop(columns=["mix"]), caption="Validated operations, by the history the maintenance kept")
```
