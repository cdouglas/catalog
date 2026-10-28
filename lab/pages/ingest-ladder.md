---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Ingest throughput, step by step
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme
theme()

STEPS = {
    "i1-ingest": "1 before", "i2-compaction": "1 before",
    "i3-committed": "2 O(added) set", "i4-compaction": "2 O(added) set",
    "i5-fold": "3 persistent state, encode", "i6-compaction": "3 persistent state, encode",
}

runs = q("""
    SELECT r.campaigns[1] AS campaign, r.rate, r.prefill, coalesce(r.committed, 'keep') AS committed,
           r.batch, r.compact_every, m.ingested, m.records_s, m.records_s_late, m.fold_records_s,
           m.compact_share
    FROM canonical_runs r JOIN ingest_metrics m USING (run_id)
    WHERE r.source = 'ingest' AND r.campaigns[1] IN ('i1-ingest', 'i2-compaction', 'i3-committed',
          'i4-compaction', 'i5-fold', 'i6-compaction')
    ORDER BY campaign, r.rate, r.prefill, committed, r.batch""")
runs["step"] = runs.campaign.map(STEPS)
runs["load"] = runs.apply(
    lambda r: f"{r.rate:,.0f}/s, " + ("ids discarded" if r.committed == "discard"
                                      else "steady state" if r.prefill else "from empty"), axis=1)
# The loads the last step ran, which every step but the first ran too
last = set(runs[runs.step == "3 persistent state, encode"].load)
fold = runs[(runs.compact_every == 0) & runs.load.isin(last)]
last_compacting = set(runs[(runs.step == "3 persistent state, encode")
                           & (runs.compact_every > 0)].load)
compacting = runs[(runs.compact_every > 0) & runs.load.isin(last_compacting)]

def rate(step, rate, load, batch=64, frame=None):
    frame = fold if frame is None else frame
    row = frame[(frame.step == step) & (frame.rate == rate) & (frame.load == load)
                & (frame.batch == batch)]
    return float(row.records_s.iloc[0]) if len(row) else float("nan")

def compact(step, load):
    return rate(step, 1000, load, 64, compacting)

steady = "1,000/s, steady state"
empty = "1,000/s, from empty"
discarded = "1,000/s, ids discarded"
first, second, third = "1 before", "2 O(added) set", "3 persistent state, encode"
```

How far one thread's ingest of the intention log rose with each change to replay and to the
checkpoint encode, on the ingest benchmark's capture (records/s; NOTES.md, "The ingest
benchmark"). Each step is a pair of campaigns on the same capture: without compactions
(`i1-ingest`, `i3-committed`, `i5-fold`) and with one every 100 records (`i2-compaction`,
`i4-compaction`, `i6-compaction`). JMH measured the changes between the steps one at a time
(IngestFoldBench; the commits say how much each bought).

## Without compactions

```{code-cell} ipython3
:label: ladder-fold
:tags: [remove-input]
:class: col-page-right

alt.Chart(fold).mark_line(point=True).encode(
    x=alt.X("batch:Q", scale=alt.Scale(type="log", base=2), title="batch (records per state)"),
    y=alt.Y("records_s:Q", scale=alt.Scale(type="log"), title="records/s"),
    color=alt.Color("step:N", title="step"),
).properties(width=220, height=160).facet(facet=alt.Facet("load:N", title=None), columns=2)
```

With the committed set at its steady state for 1,000 records/s (21.6 million ids), building
the state after every 64 records ingested {eval}`f"{rate(first, 1000, steady):,.0f}"` records/s
before; {eval}`f"{rate(second, 1000, steady):,.0f}"` once building the set cost only the ids it
adds; and {eval}`f"{rate(third, 1000, steady):,.0f}"` once a batch stopped copying the tables it
leaves alone. After every record, {eval}`f"{rate(first, 1000, steady, 1):,.1f}"`,
{eval}`f"{rate(second, 1000, steady, 1):,.0f}"` and {eval}`f"{rate(third, 1000, steady, 1):,.0f}"`.
With the ids discarded, an upper bound for keeping the set out of the replica, it ingests
{eval}`f"{rate(third, 1000, discarded):,.0f}"` in batches of 64, so keeping the steady-state set
costs the fold {eval}`f"{100 * (1 - rate(third, 1000, steady) / rate(third, 1000, discarded)):.0f}"`%
against discarding it, and an empty one
{eval}`f"{100 * (1 - rate(third, 1000, empty) / rate(third, 1000, discarded)):.0f}"`%.

```{code-cell} ipython3
:tags: [remove-input]

table(fold[["step", "load", "batch", "ingested", "records_s", "records_s_late"]],
      caption="Per step, load and batch: records/s over the log, and over its second half")
```

## With a compaction every 100 records

```{code-cell} ipython3
:label: ladder-compaction
:tags: [remove-input]
:class: col-page-right

alt.Chart(compacting).mark_bar().encode(
    x=alt.X("records_s:Q", title="records/s"),
    y=alt.Y("load:N", title=None),
    yOffset=alt.YOffset("step:N"),
    color=alt.Color("step:N", title="step"),
).properties(width=360, height=180)
```

A compaction encodes the whole checkpoint on the ingest thread. From an empty set, ingest with a
compaction every 100 records went from {eval}`f"{compact(first, empty):,.0f}"` records/s to
{eval}`f"{compact(second, empty):,.0f}"` and then {eval}`f"{compact(third, empty):,.0f}"`, the
last with the inline tables encoded without re-parsing their JSON and gzipped at the fastest
level; at the set's steady state, {eval}`f"{compact(first, steady):,.0f}"`,
{eval}`f"{compact(second, steady):,.0f}"` and {eval}`f"{compact(third, steady):,.0f}"`, whose
checkpoints still carry the whole 238 MB set (errata D16). Discarding the ids, it ingests
{eval}`f"{compact(third, discarded):,.0f}"`.

```{code-cell} ipython3
:tags: [remove-input]

table(compacting[["step", "load", "records_s", "fold_records_s", "compact_share"]],
      caption="Per step and load: records/s with compactions, folding alone, and the share compacting")
```
