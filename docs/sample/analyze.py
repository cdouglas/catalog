#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Turn the committed sample artifacts into markdown tables + PNG figures.

Reads docs/sample/<example>/{orig,tm,tmml}/manifest.json (produced by the Java
SampleGenerator) and:
  * splices a data block into each <example>/README.md and the top-level
    README.md, between <!-- analysis:start --> / <!-- analysis:end --> markers;
  * renders figures into <example>/figures/.

Needs only the committed manifests -- no Java / regeneration. Reproducible.

    python3 docs/sample/analyze.py            # all examples, under docs/sample
    python3 docs/sample/analyze.py --root docs/sample --example 1tbl-50x50
"""

import argparse
import json
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.ticker import FuncFormatter, NullFormatter  # noqa: E402


def _kib_fmt(v, _pos):
    """Plain (non-scientific) KiB tick label with just enough precision."""
    if v <= 0:
        return "0"
    if v >= 100:
        return f"{v:.0f}"
    if v >= 10:
        return f"{v:.1f}".rstrip("0").rstrip(".")
    if v >= 1:
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return f"{v:.3f}".rstrip("0").rstrip(".")

MODES = ["orig", "orig-gz", "tm", "tmml"]
MODE_LABEL = {
    "orig": "orig (vanilla)",
    "orig-gz": "orig-gz (vanilla, gzip JSON)",
    "tm": "tm (inline TM)",
    "tmml": "tmml (inline TM+ML)",
}
MODE_SHORT = {"orig": "orig", "orig-gz": "orig-gz", "tm": "tm", "tmml": "tmml"}
# Catalog-layer categories (the data layer -- manifest-avro -- is equal across
# modes and excluded from the headline footprint).
CATALOG_LAYER = ["catalog", "table-metadata-json", "manifest-list-avro"]
CAT_LABEL = {
    "catalog": "catalog file (.lcat)",
    "table-metadata-json": "metadata.json",
    "manifest-list-avro": "manifest list (snap-*.avro)",
}
CAT_COLOR = {
    "catalog": "#4c72b0",
    "table-metadata-json": "#dd8452",
    "manifest-list-avro": "#55a868",
}


def human(n):
    n = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def ratio(base, val):
    """Reduction factor, with a decimal when the factor is small."""
    if not base or not val:
        return "—"
    r = base / val
    return f"{r:.1f}×" if r < 10 else f"{r:.0f}×"


def load(example_dir):
    """Return {mode: manifest dict} plus the config."""
    data = {}
    for mode in MODES:
        path = os.path.join(example_dir, mode, "manifest.json")
        if os.path.exists(path):
            with open(path) as f:
                data[mode] = json.load(f)
    cfg_path = os.path.join(example_dir, "config.json")
    cfg = json.load(open(cfg_path)) if os.path.exists(cfg_path) else {}
    return data, cfg


def cat_bytes(manifest, category):
    return manifest.get("byCategory", {}).get(category, {}).get("bytes", 0)


def cat_count(manifest, category):
    return manifest.get("byCategory", {}).get(category, {}).get("count", 0)


def catalog_layer_total(manifest):
    return sum(cat_bytes(manifest, c) for c in CATALOG_LAYER)


def catalog_layer_files(manifest):
    return sum(cat_count(manifest, c) for c in CATALOG_LAYER)


def metadata_versions(manifest):
    """orig: (version, bytes) pairs for each metadata.json, ordered by the
    NNNNN- version prefix. The version doubles as the commit number."""
    out = []
    for f in manifest.get("files", []):
        if f["category"] != "table-metadata-json":
            continue
        name = f["path"].rsplit("/", 1)[-1]
        m = re.match(r"(\d+)-", name)
        if m:
            out.append((int(m.group(1)), f["bytes"]))
    out.sort()
    return out


# --------------------------------------------------------------------------
# Markdown
# --------------------------------------------------------------------------


def example_block(data, cfg, has_per_commit):
    lines = []

    # Footprint by category x mode.
    lines.append("### Footprint by category")
    lines.append("")
    lines.append("| category | " + " | ".join(MODE_LABEL[m] for m in MODES if m in data) + " |")
    lines.append("|---|" + "---|" * len([m for m in MODES if m in data]))
    present = [m for m in MODES if m in data]
    for c in CATALOG_LAYER:
        row = [CAT_LABEL[c]]
        for m in present:
            b = cat_bytes(data[m], c)
            n = cat_count(data[m], c)
            row.append(f"{human(b)} ({n})" if b else "—")
        lines.append("| " + " | ".join(row) + " |")
    # data layer (equal) for context
    row = ["manifest files (.avro, *data layer*)"]
    for m in present:
        row.append(f"{human(cat_bytes(data[m], 'manifest-avro'))} ({cat_count(data[m], 'manifest-avro')})")
    lines.append("| " + " | ".join(row) + " |")
    # catalog-layer total
    base = catalog_layer_total(data["orig"]) if "orig" in data else None
    row = ["**catalog-layer total**"]
    for m in present:
        t = catalog_layer_total(data[m])
        frac = f" (**{ratio(base, t)}** smaller)" if base and t and m != "orig" else ""
        row.append(f"**{human(t)}**{frac}")
    lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    lines.append("Counts in parentheses are file counts. The data layer (manifest `.avro` files "
                 "plus the data files themselves, which the catalog never writes) is identical "
                 "across modes and excluded from the catalog-layer total.")
    lines.append("")

    # Per-commit append-log cost.
    lines.append("### Per-commit cost (append-log records)")
    lines.append("")
    lines.append("| mode | log records | median record | min | max |")
    lines.append("|---|---|---|---|---|")
    for m in present:
        sizes = data[m].get("logRecordSizes", [])
        if not sizes:
            lines.append(f"| {MODE_LABEL[m]} | 0 | — | — | — |")
            continue
        arr = sorted(sizes)
        med = arr[len(arr) // 2]
        lines.append(f"| {MODE_LABEL[m]} | {len(sizes)} | {med} B | {arr[0]} B | {arr[-1]} B |")
    lines.append("")
    if cfg:
        ck = cfg.get("checkpointTxns", 0)
        lg = cfg.get("logTxns", 0)
        setup = (f"The checkpoint compacts the {ck} setup transactions; "
                 if ck else "Nothing is compacted up front; ")
        lines.append(f"{setup}the log then holds {lg} update transactions plus one carry-over "
                     f"record left by the compaction that built the checkpoint (every CAS records "
                     f"its own txn id for dedup). In pointer mode (`orig`) each log record is a "
                     f"small table-pointer swap; the real per-commit cost lives in the external "
                     f"`metadata.json` + `snap-*.avro` rewritten in full every commit.")
        lines.append("")

    # Figures (under <root>/figures/, alongside this REPORT.md).
    name = cfg.get("name", "")
    lines.append("### Figures")
    lines.append("")
    lines.append(f"![Catalog-layer footprint by category](figures/{name}_footprint.png)")
    lines.append("")
    if has_per_commit:
        lines.append(f"![Per-commit catalog-layer write size](figures/{name}_per-commit.png)")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------


def _draw_footprint(ax, data):
    """Draw the stacked catalog-layer footprint onto ax. Each non-catalog segment
    is labelled with its file count; the byte total and the total file count
    (objects written, including the single catalog file) sit on top of each bar.
    Returns (handles, labels) for a shared legend."""
    present = [m for m in MODES if m in data]
    x = np.arange(len(present))
    bottoms = np.zeros(len(present))
    for c in CATALOG_LAYER:
        vals = np.array([cat_bytes(data[m], c) / 1024.0 for m in present])
        ax.bar(x, vals, bottom=bottoms, label=CAT_LABEL[c], color=CAT_COLOR[c], zorder=2)
        # File count per segment, centred in the segment. Skip the catalog file:
        # it is always exactly one and its segment is too thin to label.
        if c != "catalog":
            for i, m in enumerate(present):
                n = cat_count(data[m], c)
                if n > 0 and vals[i] > 0:
                    ax.text(x[i], bottoms[i] + vals[i] / 2.0, f"{n}", ha="center",
                            va="center", fontsize=8, color="white", fontweight="bold", zorder=4)
        bottoms += vals
    for i, m in enumerate(present):
        tot = catalog_layer_total(data[m])
        nfiles = catalog_layer_files(data[m])
        ax.text(x[i], tot / 1024.0, f"\n{human(tot)}\n{nfiles} file{'' if nfiles == 1 else 's'}",
                ha="center", va="bottom", fontsize=8, zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels([MODE_SHORT[m] for m in present])
    ax.set_ylabel("catalog-layer bytes (KiB)")
    ax.margins(y=0.22)
    return ax.get_legend_handles_labels()


def fig_footprint(data, out_path, title):
    fig, ax = plt.subplots(figsize=(8, 4.8))
    h, l = _draw_footprint(ax, data)
    ax.set_title(title)
    ax.legend(h, l, fontsize=8, loc="upper right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def fig_combined(examples, out_path):
    """One figure, a panel per example (2x2), each the stacked footprint."""
    ncols = 2
    nrows = (len(examples) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(13, 4.8 * nrows))
    axes = np.array(axes).reshape(-1)
    handles, labels = [], []
    for ax, (name, data) in zip(axes, examples):
        handles, labels = _draw_footprint(ax, data)
        ax.set_title(name)
    for ax in axes[len(examples):]:
        ax.axis("off")
    fig.suptitle("Catalog-layer footprint across all examples "
                 "(segment label = files in that category; top = bytes / total files)",
                 fontsize=12)
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9)
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def fig_mode_grouped(series, out_path, title):
    """Grouped stacked footprint: one group per mode along x, one stacked bar per
    workload within the group. Lets the same mode be compared across two workloads
    side by side -- e.g. the static base catalog vs that catalog plus a burst of
    appends -- so the incremental cost of the appends is read off the bar growth.

    `series` is a list of (workload_label, hatch, data) in draw order."""
    from matplotlib.patches import Patch

    present = [m for m in MODES if any(m in d for _, _, d in series)]
    x = np.arange(len(present))
    nser = len(series)
    width = 0.8 / nser

    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    for si, (slabel, hatch, data) in enumerate(series):
        offset = (si - (nser - 1) / 2.0) * width
        bottoms = np.zeros(len(present))
        for c in CATALOG_LAYER:
            vals = np.array([cat_bytes(data[m], c) / 1024.0 if m in data else 0.0
                             for m in present])
            ax.bar(x + offset, vals, width=width * 0.92, bottom=bottoms,
                   color=CAT_COLOR[c], hatch=hatch, edgecolor="white", linewidth=0.4,
                   label=CAT_LABEL[c] if si == 0 else None, zorder=2)
            bottoms += vals
        for i, m in enumerate(present):
            if m not in data:
                continue
            tot = catalog_layer_total(data[m])
            nfiles = catalog_layer_files(data[m])
            ax.text(x[i] + offset, tot / 1024.0,
                    f"{human(tot)}\n{nfiles}f", ha="center", va="bottom",
                    fontsize=7, zorder=4)

    ax.set_xticks(x)
    ax.set_xticklabels([MODE_SHORT[m] for m in present])
    ax.set_ylabel("catalog-layer bytes (KiB)")
    ax.margins(y=0.20)
    ax.set_title(title)
    # Two legends: category colours, and the workload hatches.
    cat_handles = [Patch(facecolor=CAT_COLOR[c], label=CAT_LABEL[c]) for c in CATALOG_LAYER]
    ser_handles = [Patch(facecolor="0.8", hatch=h, edgecolor="white", label=sl)
                   for sl, h, _ in series]
    leg1 = ax.legend(handles=cat_handles, fontsize=8, loc="upper right", title="category")
    ax.add_artist(leg1)
    ax.legend(handles=ser_handles, fontsize=8, loc="upper left", title="workload")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def fig_per_commit(data, cfg, out_path, title):
    """Per-commit catalog-layer write size. Only meaningful for single-table
    histories (orig's metadata.json is one growing file per commit) or for logs
    with more than a carry-over record. Returns whether a figure was drawn."""
    single_table = cfg.get("tables", 1) == 1
    # Snapshot commits run 1..(checkpointTxns+logTxns); orig writes a metadata.json
    # per version, the inline modes a log record per *appended* commit (the
    # checkpoint phase was compacted away, so their deltas start after it).
    n_ckpt = cfg.get("checkpointTxns", 0)

    # Collect series (x, bytes, label, color) first, so we can pick a scale.
    series = []
    if single_table:
        for m, color, label in [
            ("orig", CAT_COLOR["table-metadata-json"], "orig: metadata.json per commit"),
            ("orig-gz", "#8c564b", "orig-gz: gzip metadata.json per commit"),
        ]:
            if m in data:
                vs = metadata_versions(data[m])  # (version, bytes), version == commit no.
                if len(vs) > 2:
                    series.append(([v for v, _ in vs], [b for _, b in vs], label, color))
    for m, color in [("tm", "#4c72b0"), ("tmml", "#c44e52")]:
        if m in data:
            # Drop the leading carry-over record; the remaining N map to the
            # appended commits (n_ckpt+1 .. n_ckpt+N) on the shared commit axis.
            body = data[m].get("logRecordSizes", [])[1:]
            if len(body) > 2:
                xs = [n_ckpt + 1 + i for i in range(len(body))]
                series.append((xs, body, f"{m}: append-log record per commit", color))
    if not series:
        return False

    all_y = [y for _, ys, _, _ in series for y in ys]
    # Log only when the range is wide (>~1.3 decades); otherwise a linear KiB
    # axis keeps a narrow delta band readable instead of a sci-notation sliver.
    use_log = max(all_y) > 20 * max(min(all_y), 1)

    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    for xs, ys, label, color in series:
        ax.plot(xs, [y / 1024.0 for y in ys], label=label, color=color, lw=1.6)
    if n_ckpt > 0:
        ax.axvline(n_ckpt + 0.5, color="gray", ls="--", lw=1, alpha=0.6)
        ax.text(n_ckpt + 0.5, 1.0, " checkpoint │ appended log",
                transform=ax.get_xaxis_transform(), ha="left", va="top",
                fontsize=7, color="gray")
    if use_log:
        ax.set_yscale("log")
        ax.yaxis.set_minor_formatter(NullFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(_kib_fmt))
    ax.set_xlabel("commit number (table-metadata version)")
    ax.set_ylabel("catalog-layer bytes written (KiB" + (", log scale)" if use_log else ")"))
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(True, which="both", ls=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return True


# --------------------------------------------------------------------------
# Top-level summary
# --------------------------------------------------------------------------


def top_summary(root, examples):
    lines = []
    lines.append("Catalog-layer bytes (and file counts) per mode. Reduction is `tmml` vs the "
                 "fair vanilla baseline `orig-gz` (gzip JSON).")
    lines.append("")
    lines.append("| example | orig | orig-gz | tm | tmml | tmml vs orig-gz |")
    lines.append("|---|---|---|---|---|---|")
    for name in examples:
        data, _ = load(os.path.join(root, name))
        if "orig" not in data:
            continue

        def cell(mode):
            if mode not in data:
                return "—"
            n = catalog_layer_files(data[mode])
            return f"{human(catalog_layer_total(data[mode]))} ({n} file{'' if n == 1 else 's'})"

        gz = catalog_layer_total(data.get("orig-gz", {})) if "orig-gz" in data else 0
        tt = catalog_layer_total(data.get("tmml", {})) if "tmml" in data else 0
        lines.append(
            f"| [`{name}`]({name}/) | {cell('orig')} | {cell('orig-gz')} | {cell('tm')} "
            f"| {cell('tmml')} | **{ratio(gz, tt)}** |")
    lines.append("")
    lines.append("Catalog-layer footprint = catalog file + `metadata.json` + manifest lists "
                 "(`snap-*.avro`). Excludes the manifest `.avro` and data files, which are "
                 "identical across modes.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--example", default=None)
    args = ap.parse_args()

    root = args.root
    examples = sorted(
        d for d in os.listdir(root)
        if os.path.isdir(os.path.join(root, d))
        and os.path.exists(os.path.join(root, d, "config.json"))
    )
    if args.example:
        examples = [e for e in examples if e == args.example]

    fig_dir = os.path.join(root, "figures")
    os.makedirs(fig_dir, exist_ok=True)

    loaded = []
    for name in examples:
        print(f"== {name} ==")
        data, cfg = load(os.path.join(root, name))
        if not data:
            print("  (no manifests; skipping)")
            continue
        loaded.append((name, data, cfg))

    # Combined figure across all examples.
    combined_rel = None
    if loaded:
        fig_combined([(n, d) for n, d, _ in loaded],
                     os.path.join(fig_dir, "all-examples_footprint.png"))
        combined_rel = "figures/all-examples_footprint.png"

    # Base-vs-append figure: the many-tables checkpoint (256 tables, no log) next
    # to many-tables-append (same 256 tables + 1024 appends), grouped by mode so
    # the cost the appends add to each encoding's base footprint is visible.
    by_name = {n: d for n, d, _ in loaded}
    if "many-tables" in by_name and "many-tables-append" in by_name:
        fig_mode_grouped(
            [("many-tables (base: 256 tables)", "", by_name["many-tables"]),
             ("many-tables-append (+1024 appends)", "//", by_name["many-tables-append"])],
            os.path.join(fig_dir, "many-tables_base-vs-append.png"),
            "Catalog-layer footprint: base 256-table catalog vs +1024 appends, by mode")

    report = ["# Catalog-format footprint report", ""]
    report.append(f"Generated by `analyze.py` from the samples under `{root}`. "
                  "See `docs/sample/README.md` for what each example means and how to regenerate.")
    report.append("")
    report.append("## Summary")
    report.append("")
    report.append(top_summary(root, examples))
    report.append("")
    if combined_rel:
        report.append(f"![Catalog-layer footprint across all examples]({combined_rel})")
        report.append("")

    for name, data, cfg in loaded:
        fig_footprint(data, os.path.join(fig_dir, f"{name}_footprint.png"),
                      f"{name}: catalog-layer footprint")
        has_pc = fig_per_commit(data, cfg, os.path.join(fig_dir, f"{name}_per-commit.png"),
                                f"{name}: per-commit catalog-layer write size")
        report.append(f"## {name}")
        report.append("")
        if cfg.get("description"):
            report.append(cfg["description"])
            report.append("")
        report.append(example_block(data, cfg, has_pc))
        report.append("")

    report_path = os.path.join(root, "REPORT.md")
    with open(report_path, "w") as f:
        f.write("\n".join(report).rstrip() + "\n")
    print(f"wrote {report_path}")
    print("done.")


if __name__ == "__main__":
    main()
