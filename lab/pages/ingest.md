---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Ingest without storage
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import q, table, theme
theme()

RETENTION_S = 6 * 3600

capture = q("""
    SELECT any_value(r.capture_records) AS records,
           any_value(m.record_bytes) AS record_bytes
    FROM canonical_runs r JOIN ingest_metrics m USING (run_id)
    WHERE list_contains(r.campaigns, 'i1-ingest')""").iloc[0]

runs = q("""
    SELECT r.run_id, r.rate, r.prefill, r.batch, m.ingested, m.fold_records_s,
           m.records_s, m.records_s_late, m.committed_bytes / 1e6 AS committed_mb
    FROM canonical_runs r JOIN ingest_metrics m USING (run_id)
    WHERE list_contains(r.campaigns, 'i1-ingest')
    ORDER BY r.rate, r.prefill, r.batch""")
runs["committed set"] = runs.prefill.map(lambda p: "steady state" if p else "from empty")
runs["falls behind"] = runs.records_s_late < runs.rate

windows = q("""
    SELECT r.run_id, r.rate, r.prefill, r.batch, w.position, w.records,
           w.fold_ms + w.compact_ms AS ms, w.records / ((w.fold_ms + w.compact_ms) / 1000) AS records_s,
           w.committed_bytes / 1e6 AS committed_mb
    FROM windows w JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'i1-ingest')
    ORDER BY r.run_id, w.position""")
windows["committed set"] = windows.prefill.map(lambda p: "steady state" if p else "from empty")
windows["load"] = windows.apply(lambda w: f"{w.rate:,.0f}/s, {w['committed set']}", axis=1)

def lags(group):
    """Lag behind a log produced at the run's rate: a window's records arrive from
    (position - records)/rate to position/rate, and its ingest starts once its first record has
    arrived and the windows before it are done. Ingest that keeps up has no lag."""
    rate = group.rate.iloc[0]
    done = 0.0
    rows = []
    for w in group.itertuples():
        done = max(done, (w.position - w.records) / rate) + w.ms / 1000
        rows.append({"t_s": w.position / rate, "lag_s": max(0.0, done - w.position / rate)})
    return pd.DataFrame(rows)

lag = (windows.groupby(["load", "batch"], group_keys=True).apply(lags)
       .reset_index(level=[0, 1]).reset_index(drop=True))
lag_summary = (lag.groupby(["load", "batch"])
               .agg(log_s=("t_s", "max"), final_lag_s=("lag_s", "last")).reset_index())

def run(r, prefill, batch, col="records_s"):
    row = runs[(runs.rate == r) & (runs.prefill == prefill) & (runs.batch == batch)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

def late(r, prefill, batch):
    return run(r, prefill, batch, "records_s_late")

def build_s(r, prefill):
    """A build's cost, from a batch of 1 and a batch of 64: 1/r1 = f + b, 1/r64 = f + b/64."""
    return (1 / run(r, prefill, 1) - 1 / run(r, prefill, 64)) / (1 - 1 / 64)

def ids(r):
    return r * RETENTION_S

fold_us = 1e6 / run(1000, False, 1024)
single = windows[(windows.rate == 1000) & (~windows.prefill) & (windows.batch == 1)]
ms_per_mb = 1e3 * build_s(1000, True) / run(1000, True, 1, "committed_mb")

behind = runs[runs["falls behind"]]
behind_list = "; ".join(
    f"{b.rate:,.0f}/s {b['committed set']}, batch {b.batch:,}" for _, b in behind.iterrows())

growth = q("""
    SELECT r.rate, r.prefill, c.position, c.checkpoint_bytes, c.committed_bytes,
           c.inline_metadata, c.inline_pool, c.inline_lists, c.inline_conflict,
           c.state_bytes - c.inline_metadata - c.inline_pool - c.inline_lists - c.inline_conflict
             AS other, c.encode_ms, c.decode_ms, c.tail_bytes
    FROM compactions c JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'i2-compaction')
    ORDER BY r.rate, r.prefill, c.position""")
growth["committed set"] = growth.prefill.map(lambda p: "steady state" if p else "from empty")
growth["load"] = growth.apply(lambda g: f"{g.rate:,.0f}/s, {g['committed set']}", axis=1)
parts = ["committed_bytes", "inline_metadata", "inline_pool", "inline_lists", "inline_conflict",
         "other"]
part_names = {"committed_bytes": "committed set", "inline_metadata": "inline metadata",
              "inline_pool": "manifest pool", "inline_lists": "manifest lists",
              "inline_conflict": "conflict state", "other": "other"}
stacked = growth[growth.position % 1000 == 0].melt(
    id_vars=["load", "position"], value_vars=parts, var_name="part", value_name="bytes")
stacked["part"] = stacked.part.map(part_names)
stacked["mb"] = stacked.bytes / 1e6

compaction = q("""
    SELECT r.rate, r.prefill, k.compactions, k.last_checkpoint_bytes / 1e6 AS checkpoint_mb,
           k.last_committed_bytes / 1e6 AS committed_mb, k.encode_ms, k.decode_ms,
           k.checkpoint_bytes_per_record, m.fold_records_s, m.records_s, m.compact_share
    FROM canonical_runs r JOIN ingest_compaction_metrics k USING (run_id)
    JOIN ingest_metrics m USING (run_id)
    WHERE list_contains(r.campaigns, 'i2-compaction')
    ORDER BY r.rate, r.prefill""")

def comp(r, prefill, col):
    row = compaction[(compaction.rate == r) & (compaction.prefill == prefill)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

empty = growth[(growth.rate == 1000) & (~growth.prefill)]
end = empty.iloc[-1]
state_mb = float(end.checkpoint_bytes - end.committed_bytes) / 1e6
early = empty[empty.position == 10000].iloc[0]
bytes_per_id_empty = float(end.committed_bytes) / float(end.position)
bytes_per_id_steady = comp(1000, True, "committed_mb") * 1e6 / ids(1000)
set_share = {r: comp(r, True, "committed_mb") / comp(r, True, "checkpoint_mb") for r in (100, 1000)}
```

How fast one thread can consume the intention log as the catalog grows, with no storage at all,
and how fast the catalog grows in bytes: an upper bound for an ingest service that tails the log.
The log is one capture of real writers (16 writers on 16 tables, mostly merge appends with some
fast appends, validated overwrites and compactions, each table's snapshots expired to the last
100; {eval}`f"{capture.records:,.0f}"` records of {eval}`f"{capture.record_bytes:,.0f}"` B on
average), replayed from its first checkpoint with its ids retimed to a production rate
(campaigns `i1-ingest`, without compactions, and `i2-compaction`, with one every 100 records;
NOTES.md, "The ingest benchmark"). A run stops after about 120 s of ingest (the slowest overran
it; NOTES.md).

## Consuming the log

```{code-cell} ipython3
:label: ingest-rate
:tags: [remove-input]
:class: col-page-right

alt.Chart(windows).mark_line(point=True).encode(
    x=alt.X("position:Q", title="records ingested"),
    y=alt.Y("records_s:Q", scale=alt.Scale(type="log"), title="records/s (window)"),
    color=alt.Color("batch:O", title="batch"),
).properties(width=220, height=160).facet(facet=alt.Facet("load:N", title=None), columns=3)
```

From an empty committed set, one thread ingests {eval}`f"{run(1000, False, 64):,.0f}"` records/s in
batches of 64 and {eval}`f"{run(1000, False, 1024):,.0f}"` in batches of 1,024 (ids retimed to 1,000
a second): about {eval}`f"{fold_us:.0f}"` µs a record. The production rate changes only the ids'
spacing, so 100 and 10,000 a second ingest alike ({eval}`f"{run(100, False, 64):,.0f}"` and
{eval}`f"{run(10000, False, 64):,.0f}"` in batches of 64). Building the state after every record
(`batch` 1) ingests {eval}`f"{run(1000, False, 1):,.0f}"` records/s over the log, falling from
{eval}`f"{single.records_s.iloc[0]:,.0f}"` in its first window to
{eval}`f"{single.records_s.iloc[-1]:,.0f}"` in its last as the committed set grows to
{eval}`f"{single.committed_mb.iloc[-1]:.2f}"` MB: each build rewrites the whole set (errata D16).

Prefilled to its steady state (every id within the 6 h retention:
{eval}`f"{ids(100) / 1e6:.2f}"` million at 100 records/s, {eval}`f"{ids(1000) / 1e6:.1f}"` million at
1,000), batch 1 ingests {eval}`f"{run(100, True, 1):,.0f}"` and {eval}`f"{run(1000, True, 1):,.1f}"`
records/s, batches of 64 {eval}`f"{run(100, True, 64):,.0f}"` and
{eval}`f"{run(1000, True, 64):,.0f}"`, and batches of 1,024 {eval}`f"{run(100, True, 1024):,.0f}"`
and {eval}`f"{run(1000, True, 1024):,.0f}"`. The set is the cost: replay unpacks it entry by entry
to merge each batch's ids and packs it again, about
{eval}`f"{1e3 * build_s(100, True):.0f}"` ms a build at 100 records/s's set and
{eval}`f"{1e3 * build_s(1000, True):.0f}"` ms at 1,000's, {eval}`f"{ms_per_mb:.1f}"` ms per MB of
set. (10,000 a second was not prefilled: {eval}`f"{ids(10000) / 1e6:.0f}"` million ids.)

```{code-cell} ipython3
:tags: [remove-input]

table(runs.drop(columns=["run_id", "prefill"]),
      caption="Per production rate, committed set and batch: records/s (all, and the second half)")
```

## When ingest falls behind

A log produced at the run's rate falls behind where a window's records take longer to ingest than
they took to arrive; the lag is how far ingest trails the log's head, in seconds of log time.

```{code-cell} ipython3
:label: ingest-lag
:tags: [remove-input]
:class: col-page-right

alt.Chart(lag).mark_line(point=True).encode(
    x=alt.X("t_s:Q", title="log time (s)"),
    y=alt.Y("lag_s:Q", title="lag behind the log (s)"),
    color=alt.Color("batch:O", title="batch"),
).properties(width=220, height=160).facet(
    facet=alt.Facet("load:N", title=None), columns=3).resolve_scale(x="independent",
                                                                    y="independent")
```

Over the second half of the log, ingest is slower than its production rate in
{eval}`f"{len(behind)}"` of the {eval}`f"{len(runs)}"` runs: {eval}`behind_list`. From an empty
set, batches of 64 keep up with 10,000 records a second
({eval}`f"{late(10000, False, 64):,.0f}"`), while building after every record keeps up only with
100 ({eval}`f"{late(100, False, 1):,.0f}"`). At the set's steady state only a batch amortizes the
build: 64 records keep up with 100 a second ({eval}`f"{late(100, True, 64):,.0f}"`), and at 1,000 a
second only batches of 1,024 do, barely ({eval}`f"{late(1000, True, 1024):,.0f}"`), publishing a
state about every {eval}`f"{1024 / 1000:.0f}"` s of log.

```{code-cell} ipython3
:tags: [remove-input]

table(lag_summary, caption="Per load and batch: the log time a run covered and its lag at the end (s)")
```

## How the catalog grows

The checkpoint at each compaction, by what its bytes hold (`i2-compaction`, 64-record batches, every
1,000th record shown; each facet its own scales); a compaction's cost is the encode, which a
compacting writer pays, and a reader that starts from the checkpoint pays the decode.

```{code-cell} ipython3
:label: ingest-growth
:tags: [remove-input]
:class: col-page-right

# A concat, not a facet: a faceted area keeps one x domain whatever the resolve
area = alt.Chart(stacked).mark_area().encode(
    x=alt.X("position:Q", title="records ingested"),
    y=alt.Y("mb:Q", stack="zero", title="checkpoint (MB)"),
    color=alt.Color("part:N", title="part"),
).properties(width=220, height=160)
alt.concat(*[area.transform_filter(alt.datum.load == load).properties(title=load)
             for load in sorted(stacked.load.unique())], columns=2).resolve_scale(
    x="independent", y="independent")
```

Without the committed set, the catalog's state settles near {eval}`f"{state_mb:.2f}"` MB: the
tables keep their last 100 snapshots, and the merge appends merge their manifests. The inline
manifest pool ({eval}`f"{end.inline_pool / 1e6:.2f}"` MB at the end of the log) and the snapshots'
manifest lists ({eval}`f"{end.inline_lists / 1e6:.2f}"` MB) are most of it, the tables' metadata
{eval}`f"{end.inline_metadata / 1e6:.2f}"` MB. The committed set grows by about
{eval}`f"{bytes_per_id_empty:.1f}"` B a record from empty
({eval}`f"{comp(1000, False, 'committed_mb'):.2f}"` MB by the end of the log) until its retention.
At its steady state it holds {eval}`f"{comp(100, True, 'committed_mb'):.1f}"` MB at 100 records/s
and {eval}`f"{comp(1000, True, 'committed_mb'):.0f}"` MB at 1,000
({eval}`f"{bytes_per_id_steady:.1f}"` B an id): {eval}`f"{set_share[100]:.0%}"` and
{eval}`f"{set_share[1000]:.1%}"` of the checkpoint that every compaction writes and every fresh read
fetches.

Encoding a checkpoint takes {eval}`f"{comp(1000, False, 'encode_ms'):.0f}"` ms on average from empty,
mostly the state's encode ({eval}`f"{early.encode_ms:.0f}"` ms at 10,000 records, when the set is
{eval}`f"{early.committed_bytes / 1e6:.1f}"` MB); at steady state it takes
{eval}`f"{comp(100, True, 'encode_ms'):.0f}"` and {eval}`f"{comp(1000, True, 'encode_ms'):,.0f}"`
ms, since the compaction's GC record rewrites the set too. Decoding one takes
{eval}`f"{comp(1000, False, 'decode_ms'):.0f}"`, {eval}`f"{comp(100, True, 'decode_ms'):.0f}"` and
{eval}`f"{comp(1000, True, 'decode_ms'):.0f}"` ms: a decode keeps the set's bytes as they are. With a
compaction every 100 records on the ingest thread (batches of 64 ending at each), it ingests
{eval}`f"{comp(1000, False, 'records_s'):,.0f}"` records/s from empty,
{eval}`f"{100 * comp(1000, False, 'compact_share'):.0f}"`% of its time compacting, against
{eval}`f"{comp(1000, False, 'fold_records_s'):,.0f}"` folding alone; at steady state
{eval}`f"{comp(100, True, 'records_s'):,.0f}"` and {eval}`f"{comp(1000, True, 'records_s'):,.0f}"`.

```{code-cell} ipython3
:tags: [remove-input]

table(compaction, caption="Per production rate and committed set: compactions every 100 records")
```

## What bounds it

Neither bound is the records: a record folds in about {eval}`f"{fold_us:.0f}"` µs. Building the
state rewrites the committed set, so at the default retention a reader that publishes every record
falls behind a log of 100 records a second, and every writer's read that folds a record pays the
same build (errata D16). A compaction encodes the whole state,
{eval}`f"{comp(1000, False, 'encode_ms'):.0f}"` ms for under 1 MB of it here, and with it the set,
which at its steady state is nearly all of the checkpoint (errata D14).
