#!/usr/bin/env python3
"""Endive configurations matching a contention-driver campaign, for its predictions.

    bench/endive/gen.py bench/configs/c6-openloop.json [--out bench/results/endive/configs]

For each open-loop run of the campaign (expanded as ContentionBench expands it), writes an endive
TOML (~/work/catalog/endive, SPEC.md) of the same store, arrival process, operation mix, job
runtime and retry limit. Endive models Iceberg's commit protocol over one catalog object; the
nearest configuration to each conflict encoding:

- partition-vector: per-partition preconditions (32 partitions), conditional append on stores that
  have it (AppendCatalog), else CAS (CASCatalog, whose disjoint retries are free);
- lamport: one partition, so any commit to the table conflicts. For validated overwrites endive
  then also takes every overlap for a real conflict, where Iceberg's validation is per partition:
  its lamport predictions overstate their aborts.

Inline metadata: endive inlines table metadata (no metadata file I/O per attempt) and, in
manifest-list append mode, writes no manifest list; it still reads one per attempt, which an
inline-ML catalog does not. Its catalog object is a fixed size (catalog_bytes, default 256 KiB),
which only CAS latency reads. A run's label names what it matches:
c6e-<store>-<workload>-r<rate>-<encoding>, its seed the repetition.
"""
import argparse
import copy
import itertools
import json
import os
import sys

DEFAULTS = {"store": "s3x", "mix": {"fast_append": 1.0}, "arrival_rate": 0,
            "arrival_dist": "exponential", "arrival_sigma": 0.5, "runtime_median_ms": 0,
            "runtime_sigma": 0, "runtime_ms": {}, "retry_num": 4, "partitions": 32,
            "partitions_per_txn": 1, "repetition": 0, "seed": 1, "encoding": "lamport"}


def expand(file):
    base = copy.deepcopy(DEFAULTS)
    given = copy.deepcopy(file)
    vary = given.pop("vary", {})
    base.update(given)
    runs = [base]
    for key, values in vary.items():
        nxt = []
        for run in runs:
            for value in values:
                c = copy.deepcopy(run)
                if key.startswith("+") and isinstance(value, dict):
                    c.update(value)
                else:
                    c[key] = value
                nxt.append(c)
        runs = nxt
    return runs


def workload_name(mix):
    if set(mix) == {"fast_append"}:
        return "fa"
    vo = mix.get("validated_overwrite", 0)
    return "favo%d" % round(100 * vo)


def toml(run, catalog_bytes):
    store = run["store"]
    append = store in ("s3x", "azure", "azurex")
    partitions = run["partitions"] if run["encoding"] != "lamport" else 1
    rate = float(run["arrival_rate"])
    mix = run["mix"]
    runtime = run["runtime_median_ms"] or 1
    sigma = run["runtime_sigma"] or 0.01
    if run["arrival_dist"] != "lognormal":
        print("warning: endive's arrivals are lognormal (median 1/rate); run %s is %s"
              % (run.get("label"), run["arrival_dist"]), file=sys.stderr)
    label = "c6e-%s-%s-r%g-%s" % (store, workload_name(mix), rate, run["encoding"])
    lines = [
        "[simulation]",
        "duration_ms = 3600000",
        'output_path = "results.parquet"',
        "seed = %d" % (1000 + run["repetition"]),
        "",
        "[experiment]",
        'label = "%s"' % label,
        "",
        "[catalog]",
        "num_tables = 1",
        "num_groups = 1",
        'mode = "%s"' % ("append" if append else "cas"),
        "table_metadata_inlined = true",
        "initial_partition_size_bytes = %d" % (catalog_bytes // partitions),
        "commit_growth_bytes = 0",
        "",
        "[catalog.partition]",
        "num_partitions = %d" % partitions,
        "",
        "[partition]",
        "enabled = true",
        "partitions_per_txn = %d" % min(run["partitions_per_txn"], partitions),
        "",
        "[transaction]",
        "retry = %d" % run["retry_num"],
        "runtime.min = 1",
        "runtime.mean = %g" % runtime,
        "runtime.sigma = %g" % sigma,
        'inter_arrival.distribution = "exponential"',
        "inter_arrival.scale = %g" % (1000.0 / rate),
        "inter_arrival.sigma = %g" % run["arrival_sigma"],
        "operation_types.fast_append = %g" % mix.get("fast_append", 0),
        "operation_types.validated_overwrite = %g" % mix.get("validated_overwrite", 0),
        "real_conflict_probability = 0.0",
        'manifest_list_mode = "append"',
        "",
        "[storage]",
        'provider = "%s"' % store,
        "",
    ]
    return label, "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("campaign")
    p.add_argument("--out", default="bench/results/endive/configs")
    p.add_argument("--catalog-bytes", type=int, default=256 * 1024)
    a = p.parse_args()
    runs = [r for r in expand(json.load(open(a.campaign))) if float(r["arrival_rate"]) > 0]
    os.makedirs(a.out, exist_ok=True)
    for run in runs:
        label, text = toml(run, a.catalog_bytes)
        path = os.path.join(a.out, "%s-s%d.toml" % (label, run["repetition"]))
        with open(path, "w") as f:
            f.write(text)
    print("%d endive configurations in %s" % (len(runs), a.out))


if __name__ == "__main__":
    main()
