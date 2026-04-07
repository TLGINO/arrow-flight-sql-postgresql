#!/usr/bin/env python3
"""TPC-H performance plots — auto-saves to substrait_test/tpch_performance.png."""

import math
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
SCALE_FACTORS = [1, 5, 10, 15, 20]

METHODS = {
    "pg_time_s":        ("PostgreSQL",      "#4c72b0", "o"),
    "arrow_time_s":     ("Arrow Flight SQL", "#dd8452", "s"),
    "substrait_time_s": ("Substrait",        "#55a868", "^"),
}


def load_data():
    frames = []
    for sf in SCALE_FACTORS:
        p = SCRIPT_DIR / "timing" / f"timing_tpch_sf{sf}.csv"
        if p.exists():
            frames.append(pd.read_csv(p))
        else:
            print(f"  skip {p.name} (not found)")
    if not frames:
        raise FileNotFoundError("no timing CSVs found")
    return pd.concat(frames, ignore_index=True)


def plot(data, out="tpch_performance.png"):
    plt.style.use("seaborn-v0_8-whitegrid")
    queries = sorted(data["query"].unique())
    n = len(queries)
    ncols = min(4, n)
    nrows = math.ceil(n / ncols)

    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(4.2 * ncols, 3.2 * nrows),
                             constrained_layout=True)
    fig.suptitle("TPC-H  —  pgsql vs arrow vs substrait",
                 fontsize=14, y=1.01)

    axes = [axes] if n == 1 else axes.flatten()

    for i, q in enumerate(queries):
        ax = axes[i]
        qd = data[data["query"] == q].sort_values("sf")

        for col, (label, color, marker) in METHODS.items():
            valid = qd[qd[col].notna()]
            if valid.empty:
                continue
            ax.plot(valid["sf"], valid[col],
                    marker=marker, color=color, label=label,
                    linewidth=1.6, markersize=5, alpha=0.9)

        ax.set_title(q, fontsize=10, pad=4)
        ax.set_xlabel("SF", fontsize=8)
        ax.set_ylabel("seconds", fontsize=8)
        ax.tick_params(labelsize=7)

        ax.set_xticks(SCALE_FACTORS)

        times = qd[list(METHODS)].stack()
        if len(times) and times.max() / max(times.min(), 1e-9) > 100:
            ax.set_yscale("log")

        ax.grid(True, alpha=0.3, linewidth=0.5)

    # single legend for the whole figure
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", fontsize=8,
               framealpha=0.9)

    for i in range(n, len(axes)):
        axes[i].set_visible(False)

    out_path = SCRIPT_DIR / out
    fig.savefig(out_path, dpi=200, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    print(f"saved {out_path}")


def main():
    data = load_data()
    print(f"{len(data)} points, SF={sorted(data['sf'].unique())}, "
          f"{len(data['query'].unique())} queries")
    plot(data)


if __name__ == "__main__":
    main()
