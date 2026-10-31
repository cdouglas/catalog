---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Conflict witnesses on generated logs
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme
theme()

ENCODINGS = ["lamport", "lamport-minus-properties", "concern-vector", "blind-append",
             "partition-vector", "manifest-window"]
WORKLOADS = {"t1-fa": "fast appends", "t1-mix": "mix", "t1-txn": "catalog transactions"}

eff = q("""
    WITH c AS (SELECT * FROM trace_ingest t JOIN canonical_runs USING (run_id)
               WHERE list_contains(campaigns, 'g1-witness') AND NOT t.trace_oracle),
         o AS (SELECT * FROM trace_ingest t JOIN canonical_runs USING (run_id)
               WHERE list_contains(campaigns, 'g1-witness') AND t.trace_oracle)
    SELECT c.trace_label AS workload, c.trace_writers AS writers, c.trace_encoding AS encoding,
           c.entries, c.rejected, c.entries - c.rejected AS applied,
           (c.entries - c.rejected) / c.entries AS applied_share,
           o.entries AS oracle_entries, o.refused,
           o.refused / (o.entries + o.refused) AS refused_share,
           c.mean_window AS window
    FROM c JOIN o USING (trace_label, trace_writers, trace_encoding)""")
eff["workload_name"] = eff.workload.map(WORKLOADS)

def at(workload, writers, encoding, col, df=None):
    df = eff if df is None else df
    row = df[(df.workload == workload) & (df.writers == writers) & (df.encoding == encoding)]
    return float(row[col].iloc[0])

reasons = q("""
    SELECT t.trace_label AS workload, t.trace_writers AS writers, t.trace_encoding AS encoding,
           i.reason, i.category, i.records
    FROM ingest_rejections i JOIN trace_ingest t USING (run_id) JOIN canonical_runs USING (run_id)
    WHERE list_contains(campaigns, 'g1-witness') AND NOT t.trace_oracle""")

txn = q("""
    SELECT t.trace_writers AS writers, t.trace_encoding AS encoding,
           CAST(json_extract(r.manifest, '$.capture.manifest.summary.outcomes.catalog_txn.committed')
                AS BIGINT) AS committed,
           CAST(json_extract(r.manifest,
                '$.capture.manifest.summary.outcomes.catalog_txn.validation_failed') AS BIGINT)
               AS failed_client_side,
           CAST(json_extract(r.manifest,
                '$.capture.manifest.summary.outcomes.catalog_txn.retries_exhausted') AS BIGINT)
               AS retries_exhausted
    FROM trace_ingest t JOIN runs_raw r USING (run_id) JOIN canonical_runs USING (run_id)
    WHERE list_contains(campaigns, 'g1-witness') AND t.trace_label = 't1-txn'
      AND NOT t.trace_oracle""")
txn["client_share"] = txn.failed_client_side / (
    txn.committed + txn.failed_client_side + txn.retries_exhausted)

fold = q("""
    SELECT t.trace_label AS workload, t.trace_writers AS writers, t.trace_encoding AS encoding,
           CASE WHEN t.trace_oracle THEN 'oracle' ELSE 'contended' END AS trace,
           CASE WHEN f.run_id LIKE '%proto' THEN 'prototype' ELSE '8d9277a' END AS build,
           f.records_s, f.records_s_error,
           f.records_s * (t.entries - t.rejected) / t.entries AS goodput_s
    FROM trace_fold f JOIN trace_ingest t USING (trace_dir)
    JOIN canonical_runs c ON c.run_id = f.run_id
    WHERE list_contains(c.campaigns, 'g1-witness')""")
fold["workload_name"] = fold.workload.map(WORKLOADS)

def fold_at(workload, writers, encoding, trace, build, col="records_s"):
    row = fold[(fold.workload == workload) & (fold.writers == writers)
               & (fold.encoding == encoding) & (fold.trace == trace) & (fold.build == build)]
    return float(row[col].iloc[0]) if len(row) else float("nan")

txn16 = txn[txn.writers == 16]
```

Campaign `g1-witness` measures what each conflict encoding lets into the log, on logs no storage
or clock shapes: the log generator (`LogTrace`; NOTES.md "The log benchmark") runs writers that
each hold a `LogCatalog` over one in-memory shared log, and a seeded scheduler picks who runs at
each read and append, so a trace is a function of its configuration and seed. Three workloads on
16 tables (Zipf skew, an expiry every 2,000 entries): fast appends alone (`t1-fa`); a mix of fast
appends, validated overwrites, compactions, property changes and catalog transactions (`t1-mix`);
and catalog transactions of 4 tables racing fast appends (`t1-txn`). Each runs under every
encoding with 4 and 16 writers, twice: contended (a record that loses lands, replay rejects it, and
its writer retries) and oracle (a record that would not apply where it lands is refused before the
append, so every entry applies).

## What each encoding lets apply

The share of landed entries that apply is the log's efficiency: the rest are conflicts that took
a slot in the log, and a retry's entry after them.

```{code-cell} ipython3
:label: g1-applied
:tags: [remove-input]
:class: col-page-right

alt.Chart(eff).mark_bar().encode(
    x=alt.X("applied_share:Q", title="entries that applied (share of landed)",
            axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("encoding:N", sort=ENCODINGS, title=None),
    color=alt.Color("writers:O", title="writers"),
    yOffset="writers:O",
    row=alt.Row("workload_name:N", title=None, sort=list(WORKLOADS.values())))
```

```{code-cell} ipython3
:tags: [remove-input]

table(eff[["workload", "writers", "encoding", "entries", "rejected", "applied_share", "refused",
           "refused_share", "window"]].sort_values(
    ["workload", "writers", "encoding"],
    key=lambda s: s.map(ENCODINGS.index) if s.name == "encoding" else s))
```

Under fast appends the encodings split in two: lamport, lamport-minus-properties and concern-vector
reject the same entries (every append moves main, so concern-vector's `ref: advanced` is as coarse as
the table's version), {eval}`f"{1 - at('t1-fa', 16, 'lamport', 'applied_share'):.0%}"` of them at 16
writers; blind-append, partition-vector and manifest-window merge them, and reject only what races
an expiry ({eval}`f"{1 - at('t1-fa', 16, 'manifest-window', 'applied_share'):.1%}"`). In the mix,
manifest-window and partition-vector apply {eval}`f"{at('t1-mix', 16, 'manifest-window', 'applied_share'):.1%}"`
of entries at 16 writers against {eval}`f"{at('t1-mix', 16, 'lamport', 'applied_share'):.1%}"` for
lamport; blind-append, which merges only blind appends, is between. The oracle traces refuse at the
rates the contended ones reject, as they should: the generator's two modes agree.

## Why entries were rejected

```{code-cell} ipython3
:tags: [remove-input]

top = (reasons[reasons.writers == 16].sort_values("records", ascending=False)
       .groupby(["workload", "encoding"]).head(4))
table(top.sort_values(["workload", "encoding", "records"],
                      key=lambda s: s.map(ENCODINGS.index) if s.name == "encoding" else s,
                      ascending=[True, True, False]))
```

What the merging encodings still reject is mostly `whole: a read pin`: a catalog transaction's
read of a table that changed since, which its serializable isolation pins at the table's version.
In the mix, the rest are real conflicts (data removed or added in a validated overwrite's scope) and
compactions racing compactions (`merge: a manifest it removes is not in its branch's head`).

## Catalog transactions fail before the log

```{code-cell} ipython3
:tags: [remove-input]

table(txn.sort_values(["writers", "encoding"],
                      key=lambda s: s.map(ENCODINGS.index) if s.name == "encoding" else s))
```

At 16 writers, {eval}`f"{txn16.client_share.min():.0%}"`-{eval}`f"{txn16.client_share.max():.0%}"`
of catalog transactions end in a check before their writer appends (the serializable read set, or
an Iceberg requirement: `validation_failed`); no encoding sees them. As in c9-txn, the encodings'
lever on transactions is small until the read set is judged in the log, at a finer grain than a
table's version.

## The fold: what replay costs per entry

JMH (`IngestFoldBench.fold`, batches of 64, a steady-state committed set) folds each trace on two
builds: 8d9277a, the code that generated them, and the three fixes found by profiling these folds
(the prototype, committed as 9446029): manifest-window's window walked every window snapshot's whole inline
manifest list even for a blind append on v2, which checks nothing in it; the merge (`Landing`)
resolved the head's whole list for a blind append on v2, which reads nothing of it; and an expiry's
pool cleanup unioned every removed snapshot's list. Fast appends never merge manifests, so a hot
table's list grows to thousands of entries in `t1-fa`; the mix's compactions keep lists short.

```{code-cell} ipython3
:label: g1-fold
:tags: [remove-input]
:class: col-page-right

alt.Chart(fold[fold.trace == "oracle"]).mark_point(filled=True, size=60).encode(
    x=alt.X("records_s:Q", title="fold rate of the oracle trace (entries/s)"),
    y=alt.Y("encoding:N", sort=ENCODINGS, title=None),
    color=alt.Color("build:N", title="build"),
    shape=alt.Shape("writers:O", title="writers"),
    row=alt.Row("workload_name:N", title=None, sort=list(WORKLOADS.values())))
```

```{code-cell} ipython3
:tags: [remove-input]

table(fold.sort_values(["workload", "writers", "encoding", "trace", "build"],
                       key=lambda s: s.map(ENCODINGS.index) if s.name == "encoding" else s))
```

On 8d9277a, folding the fast-append oracle trace at 16 writers, manifest-window ran at
{eval}`f"{fold_at('t1-fa', 16, 'manifest-window', 'oracle', '8d9277a'):,.0f}"` entries/s against
lamport's {eval}`f"{fold_at('t1-fa', 16, 'lamport', 'oracle', '8d9277a'):,.0f}"`; with the prototype,
{eval}`f"{fold_at('t1-fa', 16, 'manifest-window', 'oracle', 'prototype'):,.0f}"` against
{eval}`f"{fold_at('t1-fa', 16, 'lamport', 'oracle', 'prototype'):,.0f}"`. The gap was the
implementation's, not the witness's. Replaying the 20 fast-append traces on both builds gave the
same rejections and byte-identical final states.

An oracle trace's fold rate does not bound its contended trace's goodput (entries that apply per
second of folding) where an entry's cost grows with the state: lamport's contended fast-append trace
at 16 writers applies {eval}`f"{at('t1-fa', 16, 'lamport', 'applied'):,.0f}"` of its entries, its
oracle trace all {eval}`f"{at('t1-fa', 16, 'lamport', 'oracle_entries'):,.0f}"`, so the contended
trace's tables end with about half the snapshots and shorter lists, and its goodput
({eval}`f"{fold_at('t1-fa', 16, 'lamport', 'contended', 'prototype', 'goodput_s'):,.0f}"`
entries/s on the prototype) exceeds its own oracle trace's rate. Compare encodings' fold costs at
equal state, and read the log's efficiency above as what each encoding buys.

Caveats: the JMH runs are one fork each on a laptop (errors in the table, often 10-20%); the ingest
runs of the campaign (one pass per trace on havoc beside other jobs) are noisier still. At 4 writers
the windows are short (under one entry on average): most entries meet no change.
