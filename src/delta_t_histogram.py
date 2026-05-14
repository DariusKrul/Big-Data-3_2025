#!/usr/bin/env python3
"""
Task 4: Calculate delta-t between subsequent points per vessel, plot histogram.

For each vessel in ais.vessels_filtered:
  - Sort its rows by Timestamp.
  - For each pair of consecutive rows, compute delta-t = t[i+1] - t[i] in ms.
  - Collect all delta-t values across all vessels.

Then plot a histogram.

The heavy lifting (sort + pair-diff per MMSI) runs server-side via an
aggregation that uses $setWindowFields with $shift to access the previous
row's timestamp. Only the resulting delta-t numbers come back to Python.

Usage:
    python delta_t_histogram.py
    python delta_t_histogram.py --max-seconds 60 --bins 60 --out hist.png
"""
from __future__ import annotations

import argparse
import time

import matplotlib

matplotlib.use("Agg")  # non-interactive backend; just write PNG
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
COLL_NAME = "vessels_filtered"


def compute_deltas() -> np.ndarray:
    """Return all delta-t values (in milliseconds) as a numpy array."""
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_NAME]

    # Per-vessel sort by Timestamp, then $shift to compare each row to the previous.
    pipeline = [
        # Filter out rows without a valid timestamp
        {"$match": {"Timestamp": {"$ne": None, "$exists": True}}},
        {"$setWindowFields": {
            "partitionBy": "$MMSI",
            "sortBy": {"Timestamp": 1},
            "output": {
                "_prev_ts": {"$shift": {"output": "$Timestamp", "by": -1}},
            },
        }},
        {"$match": {"_prev_ts": {"$ne": None}}},
        {"$project": {
            "_id": 0,
            "delta_ms": {"$subtract": ["$Timestamp", "$_prev_ts"]},
        }},
    ]

    print("Running delta-t aggregation server-side...")
    start = time.time()
    cursor = coll.aggregate(pipeline, allowDiskUse=True, batchSize=10000)

    # Stream into a list of int ms. Each Timestamp diff returns a number.
    deltas: list[int] = []
    for doc in cursor:
        d = doc["delta_ms"]
        # When BSON Date subtracts Date, MongoDB 7 returns the diff as a long (ms).
        if d is None:
            continue
        deltas.append(int(d))

    elapsed = time.time() - start
    arr = np.array(deltas, dtype=np.int64)
    print(f"  {len(arr):,} delta-t values computed in {elapsed:.1f}s.")
    return arr


def summarize(deltas: np.ndarray):
    """Print quick stats so we know what the distribution looks like."""
    if deltas.size == 0:
        print("No deltas to summarize.")
        return
    print("Delta-t summary (milliseconds):")
    pcts = [50, 75, 90, 95, 99, 99.9]
    qs = np.percentile(deltas, pcts)
    print(f"  count   {deltas.size:>15,}")
    print(f"  min     {deltas.min():>15,} ms")
    print(f"  max     {deltas.max():>15,} ms  ({deltas.max() / 1000 / 60:.1f} min)")
    print(f"  mean    {deltas.mean():>15,.1f} ms")
    for p, q in zip(pcts, qs):
        print(f"  p{p:<5} {q:>15,.0f} ms")


def plot_histogram(deltas: np.ndarray, max_seconds: int, bins: int, out_path: str):
    """Plot a histogram of delta-t values (clipped to max_seconds for readability)."""
    max_ms = max_seconds * 1000
    clipped = deltas[(deltas >= 0) & (deltas <= max_ms)]
    excluded = deltas.size - clipped.size

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(clipped / 1000.0, bins=bins, edgecolor="black", linewidth=0.3)
    ax.set_xlabel("Delta-t between consecutive points (seconds)")
    ax.set_ylabel("Number of point-pairs")
    ax.set_title(
        f"AIS delta-t distribution  "
        f"(n={clipped.size:,} pairs; "
        f"{excluded:,} pairs > {max_seconds}s excluded)"
    )
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    print(f"Histogram saved to {out_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=int, default=60,
                        help="Clip histogram x-axis to this many seconds (default: 60)")
    parser.add_argument("--bins", type=int, default=60,
                        help="Number of histogram bins (default: 60)")
    parser.add_argument("--out", default="delta_t_histogram.png",
                        help="Output PNG path (default: delta_t_histogram.png)")
    args = parser.parse_args()

    deltas = compute_deltas()
    summarize(deltas)
    if deltas.size > 0:
        plot_histogram(deltas, args.max_seconds, args.bins, args.out)


if __name__ == "__main__":
    main()
