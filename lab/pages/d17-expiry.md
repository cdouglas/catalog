---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Expiries that merge (D17)
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, scale, figures_module
theme()
f = figures_module()
MERGING = ["concern-vector", "blind-append", "partition-vector", "manifest-window"]
ENC = ["lamport"] + MERGING
ARMS = scale(domain=["off", "on"])

# the campaign's runs, with the arm (expiry_merge) and the workload each ran
RUNS = """
  WITH r AS (
    SELECT run_id, encoding, writers,
           CASE WHEN json_extract_string(params::JSON, '$.expiry_merge') = 'true'
                THEN 'on' ELSE 'off' END AS arm,
           CASE WHEN json_extract_string(params::JSON, '$.expire_older_than_s') = '30'
                THEN 'appends + compactions' ELSE 'appends' END AS wl
    FROM canonical_runs WHERE list_contains(campaigns, 'd17-expiry'))"""

runs = q(RUNS + """
  SELECT count(*) AS runs, count(DISTINCT git_sha) AS shas, left(any_value(git_sha), 7) AS sha,
         strftime(min(started_at), '%Y-%m-%d') AS day,
         count(*) FILTER (WHERE c.cpu_bound) AS cpu_bound,
         coalesce(list_distinct(list(r.encoding || ' ' || CAST(r.writers AS VARCHAR) || ', '
                                     || r.wl) FILTER (WHERE c.cpu_bound)), []) AS cpu_bound_cells
  FROM r JOIN canonical_runs USING (run_id) JOIN run_cpu c USING (run_id)""").iloc[0]

# every expiry the maintenance client ran in the measured window, per cell (two runs each)
exp = q(RUNS + """
  SELECT r.wl, r.encoding, r.writers, r.arm,
         count(*) AS expiries,
         count(*) FILTER (WHERE o.outcome = 'retries_exhausted') AS exhausted,
         count(*) FILTER (WHERE o.outcome NOT IN ('committed', 'retries_exhausted')) AS other,
         avg(o.conflicts) AS conflicts,
         quantile_cont(o.latency_ms, 0.5) AS p50_ms,
         quantile_cont(o.latency_ms, 0.9) AS p90_ms
  FROM ops o JOIN r USING (run_id)
  WHERE o.measured AND o.kind = 'expire'
  GROUP BY ALL ORDER BY r.wl, r.writers, r.encoding, r.arm""")
exp["failed_share"] = exp.exhausted / exp.expiries

# the same by class: the four merging encodings together, and lamport (the control)
cls = q(RUNS + """
  SELECT CASE WHEN r.encoding = 'lamport' THEN 'lamport' ELSE 'merging' END AS cls,
         r.writers, r.arm,
         count(*) AS expiries,
         count(*) FILTER (WHERE o.outcome = 'retries_exhausted') AS exhausted,
         avg(o.conflicts) AS conflicts,
         quantile_cont(o.latency_ms, 0.5) AS p50_ms,
         quantile_cont(o.latency_ms, 0.9) AS p90_ms
  FROM ops o JOIN r USING (run_id)
  WHERE o.measured AND o.kind = 'expire'
  GROUP BY ALL ORDER BY cls, r.writers, r.arm""")

def c(klass, writers, merge, col):
    row = cls[(cls.cls == klass) & (cls.writers == writers) & (cls.arm == merge)]
    return float(row[col].iloc[0])

def tot(klass, merge, col):
    return int(cls[(cls.cls == klass) & (cls.arm == merge)][col].sum())

# each expiry's latency under the merging encodings, as an empirical CDF per writers and arm
ecdf = q(RUNS + """
  SELECT r.writers, r.arm, o.latency_ms / 1000.0 AS latency_s,
         cume_dist() OVER (PARTITION BY r.writers, r.arm ORDER BY o.latency_ms) AS share
  FROM ops o JOIN r USING (run_id)
  WHERE o.measured AND o.kind = 'expire' AND r.encoding <> 'lamport'""")

# the writers' appends, per cell
fa = q(RUNS + """
  SELECT r.wl, r.encoding, r.writers, r.arm,
         avg(k.commits_s) AS commits_s,
         sum(k.exhausted) / sum(k.ops) AS exhausted_share,
         avg(k.p50_ms) AS p50_ms, avg(k.p99_ms) AS p99_ms
  FROM kind_metrics k JOIN r USING (run_id)
  WHERE k.kind = 'fast_append'
  GROUP BY ALL ORDER BY r.wl, r.writers, r.encoding, r.arm""")
wide = fa.pivot_table(index=["wl", "encoding", "writers"], columns="arm",
                      values="commits_s").reset_index()
wide["ratio"] = wide["on"] / wide["off"]
merging_ratio = wide[wide.encoding.isin(MERGING)].ratio
lamport_ratio = wide[wide.encoding == "lamport"].ratio

# before the fix: c8-fold (fast appends on s3x under manifest-window, as this campaign's first
# workload, on the code of its day)
old = q("""
  SELECT r.writers, sum(k.committed) / sum(k.ops) AS committed,
         strftime(min(r.started_at), '%Y-%m-%d') AS day,
         left(any_value(r.git_sha), 7) AS sha
  FROM kind_metrics k JOIN canonical_runs r USING (run_id)
  WHERE list_contains(r.campaigns, 'c8-fold') AND k.kind = 'expire' AND r.store = 's3x'
    AND r.encoding = 'manifest-window' AND r.writers IN (16, 32)
  GROUP BY ALL ORDER BY r.writers""")
old_at = lambda w, col: old[old.writers == w][col].iloc[0]
mw_off = lambda w: 1 - float(exp[(exp.wl == "appends") & (exp.encoding == "manifest-window")
                                 & (exp.writers == w) & (exp.arm == "off")].failed_share.iloc[0])
```

An expiry's record removes the snapshots it found unused on the table it read. Before errata
D17's fix, replay merged no record that removes snapshots, so an expiry failed whenever anything
had changed its table since its read, and Iceberg retried it. Since the fix (fileio 2a9f5a4), an
expiry lands on a changed table under `concern-vector` and the encodings built on it, removing what
it chose on its base, if no ref was reset since in any ref bucket and the table, schema and spec
concerns are unchanged. `fileio.catalog.expiry.merge` false (the driver's `expiry_merge`) holds an
expiry to the version it read again, for ablation.

Campaign `d17-expiry` compares the two on `s3x`. Its maintenance client expires every 5 s, keeping
the last 50 snapshots, while the writers commit fast appends, or fast appends with 5% compactions
(and expiries only of snapshots older than 30 s). It runs `lamport` as the control, where an expiry
conflicts on the version either way, and the four merging encodings, at 16 and 32 writers, two runs
each: {eval}`int(runs.runs)` runs on havoc on {eval}`str(runs.day)`, fileio
{eval}`runs.sha`.

## Expiries

Under the merging encodings, {eval}`tot('merging', 'off', 'exhausted')` of
{eval}`tot('merging', 'off', 'expiries')` expiries exhausted Iceberg's retries with the merge off
({eval}`f"{100 * tot('merging', 'off', 'exhausted') / tot('merging', 'off', 'expiries'):.0f}"`%),
and {eval}`tot('merging', 'on', 'exhausted')` of {eval}`tot('merging', 'on', 'expiries')` with it on.
An expiry met {eval}`f"{c('merging', 16, 'off', 'conflicts'):.2f}"` conflicts on average at 16
writers and {eval}`f"{c('merging', 32, 'off', 'conflicts'):.2f}"` at 32 with the merge off, and
{eval}`f"{c('merging', 32, 'on', 'conflicts'):.0f}"` with it on. Under `lamport`, where the switch
changes nothing an expiry does,
{eval}`tot('lamport', 'off', 'exhausted')` of {eval}`tot('lamport', 'off', 'expiries')` failed with
it off and {eval}`tot('lamport', 'on', 'exhausted')` of {eval}`tot('lamport', 'on', 'expiries')`
with it on: the spread two runs a cell leave.

```{code-cell} ipython3
:label: d17-failed
:tags: [remove-input]
:class: col-page-right

alt.Chart(exp).mark_bar().encode(
    x=alt.X("encoding:N", sort=ENC, title=None, axis=alt.Axis(labelAngle=-30)),
    xOffset=alt.XOffset("arm:N", sort=["off", "on"]),
    y=alt.Y("failed_share:Q", title="expiries that exhausted retries", axis=alt.Axis(format="%")),
    color=alt.Color("arm:N", title="expiry merge", sort=["off", "on"], scale=ARMS),
    tooltip=["wl", "encoding", "writers", "arm", "expiries", "exhausted"],
).properties(width=220, height=150).facet(
    column=alt.Column("writers:O", title="writers"), row=alt.Row("wl:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(exp.drop(columns=["failed_share"]),
      caption="Expiries per workload, encoding, writer count and arm (two runs each; latency of "
              "every expiry, failed ones included)")
```

An expiry also finishes sooner. Under the merging encodings its 90th-percentile latency (failed
expiries included) falls from {eval}`f"{c('merging', 16, 'off', 'p90_ms') / 1000:.1f}"` s to
{eval}`f"{c('merging', 16, 'on', 'p90_ms') / 1000:.1f}"` s at 16 writers and from
{eval}`f"{c('merging', 32, 'off', 'p90_ms') / 1000:.1f}"` s to
{eval}`f"{c('merging', 32, 'on', 'p90_ms') / 1000:.1f}"` s at 32: the tail the merge removes is
Iceberg's retries, each after a backoff from 100 ms.

```{code-cell} ipython3
:label: d17-latency
:tags: [remove-input]
:class: col-page-right

alt.Chart(ecdf).mark_line(interpolate="step-after").encode(
    x=alt.X("latency_s:Q", scale=alt.Scale(type="log", domain=[0.005, 30]),
            axis=alt.Axis(values=[0.01, 0.1, 1, 10]), title="expiry latency (s, log)"),
    y=alt.Y("share:Q", title="share of expiries", axis=alt.Axis(format="%")),
    color=alt.Color("arm:N", title="expiry merge", sort=["off", "on"], scale=ARMS),
).properties(width=260, height=180).facet(column=alt.Column("writers:O", title="writers"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(cls, caption="Expiries by class (the four merging encodings together; lamport, the control), "
                   "writer count and arm")
```

## The writers' appends

Merging expiries costs the writers nothing visible: under the merging encodings, a cell's append
throughput with the merge on is {eval}`f"{merging_ratio.min():.2f}"` to
{eval}`f"{merging_ratio.max():.2f}"` times its throughput with it off (median
{eval}`f"{merging_ratio.median():.2f}"`). Under `lamport`, where the switch changes nothing, the
ratio is {eval}`f"{lamport_ratio.min():.2f}"` to {eval}`f"{lamport_ratio.max():.2f}"`: the arms ran
in consecutive blocks of a few minutes (merge on first) on havoc, a shared host that was heavily
loaded that night, and the change in its load between blocks moves throughput as much as the
switch does. This comparison is weaker than the expiries', and
{eval}`int(runs.cpu_bound)` runs kept their shard's CPUs more than 60% busy
({eval}`", ".join(runs.cpu_bound_cells)`, in both arms).

```{code-cell} ipython3
:label: d17-appends
:tags: [remove-input]
:class: col-page-right

alt.Chart(fa).mark_bar().encode(
    x=alt.X("encoding:N", sort=ENC, title=None, axis=alt.Axis(labelAngle=-30)),
    xOffset=alt.XOffset("arm:N", sort=["off", "on"]),
    y=alt.Y("commits_s:Q", title="fast appends committed/s"),
    color=alt.Color("arm:N", title="expiry merge", sort=["off", "on"], scale=ARMS),
    tooltip=["wl", "encoding", "writers", "arm", alt.Tooltip("commits_s:Q", format=".1f")],
).properties(width=220, height=150).facet(
    column=alt.Column("writers:O", title="writers"), row=alt.Row("wl:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(fa, caption="Fast appends per workload, encoding, writer count and arm (two runs each)")
```

## Against the earlier campaigns

Campaign `c8-fold` ({eval}`str(old_at(16, 'day'))`, fileio {eval}`old_at(16, 'sha')`) ran this
campaign's first workload under `manifest-window` on `s3x`; its expiries committed
{eval}`f"{100 * old_at(16, 'committed'):.0f}"`% at 16 writers and
{eval}`f"{100 * old_at(32, 'committed'):.0f}"`% at 32, where this campaign's arm with the merge off
commits {eval}`f"{100 * mw_off(16):.0f}"`% and {eval}`f"{100 * mw_off(32):.0f}"`%. Both hold an
expiry to the version it read, but the catalog changed between them in more than its expiries:
among other things, the backoff after a lost race scales with the store's latency (errata D14),
reads are tail reads, and a writer compacts after its commit. Which of these relieved most of the
starvation D17 recorded is not measured here; this campaign's arm with the merge off, not
`c8-fold`, is the baseline for the merge, which removes what remained, failures and conflicts both.
