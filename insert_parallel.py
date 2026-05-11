#!/usr/bin/env python3
"""
Task 2: Parallel insertion of AIS CSV data into the sharded MongoDB cluster.

Each worker process maintains its OWN MongoClient instance (per the assignment
requirement: "Use separate instances of the MongoClient for each parallel
thread or task"). The main process streams the CSV in chunks and dispatches
chunks to workers via multiprocessing.Pool.

Usage:
    python insert_parallel.py --csv path/to/aisdk-YYYY-MM-DD.csv
    python insert_parallel.py --csv data.csv --limit 1000000   # smoke test
    python insert_parallel.py --csv data.csv --workers 4 --chunk-size 50000
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import time
from typing import Iterable

import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError

# ---------------------------------------------------------------------------
# Connection settings
# ---------------------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
COLLECTION_NAME = "vessels"

# AIS columns that should be numeric. Empty strings / "Unknown" become None.
NUMERIC_COLUMNS = [
    "MMSI", "Latitude", "Longitude", "ROT", "SOG", "COG",
    "Heading", "IMO", "Width", "Length", "Draught",
]

# ---------------------------------------------------------------------------
# Per-worker MongoClient (lazy singleton)
# ---------------------------------------------------------------------------
# Each worker process gets its OWN client. We do NOT share a client across
# processes -- MongoClient is not fork-safe (its connection pool and
# monitoring threads don't survive fork). Lazy init means the client is
# created the first time the worker actually inserts something.
_worker_client: MongoClient | None = None


def _get_worker_collection():
    """Return this worker's collection handle, creating the client if needed."""
    global _worker_client
    if _worker_client is None:
        _worker_client = MongoClient(MONGO_URI)
    return _worker_client[DB_NAME][COLLECTION_NAME]


# ---------------------------------------------------------------------------
# Chunk processing
# ---------------------------------------------------------------------------
def _coerce_chunk(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame chunk to MongoDB-ready dicts.

    - Numeric columns: coerce to numbers, empty/invalid -> None.
    - Timestamp: parse to datetime so MongoDB stores BSON Date.
    - Drop rows missing MMSI (can't shard without the shard key).
    """
    # Numeric coercion
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Timestamp -- AIS files use "# Timestamp" with day-first format (DD/MM/YYYY)
    ts_col = next((c for c in df.columns if "Timestamp" in c), None)
    if ts_col is not None:
        df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=True, errors="coerce")
        if ts_col != "Timestamp":
            df = df.rename(columns={ts_col: "Timestamp"})

    # Drop rows with no MMSI -- mongos can't route inserts without the shard key
    df = df[df["MMSI"].notna()]

    # NaN -> None so they become real BSON nulls (cleaner $ne:null filtering later)
    return df.astype(object).where(pd.notna(df), None).to_dict("records")


def _insert_chunk(records: list[dict]) -> tuple[int, int]:
    """Insert a list of records using bulk_write. Returns (inserted, errors)."""
    if not records:
        return 0, 0
    coll = _get_worker_collection()
    ops = [InsertOne(r) for r in records]
    try:
        result = coll.bulk_write(ops, ordered=False)
        return result.inserted_count, 0
    except BulkWriteError as e:
        # Count succeeded vs failed; don't crash the whole pool
        inserted = e.details.get("nInserted", 0)
        errors = len(e.details.get("writeErrors", []))
        return inserted, errors


def _worker(records: list[dict]) -> tuple[int, int]:
    """Top-level worker entry point (must be picklable for Pool)."""
    return _insert_chunk(records)


# ---------------------------------------------------------------------------
# Main: stream CSV -> coerce -> dispatch to pool
# ---------------------------------------------------------------------------
def stream_chunks(
    csv_path: str, chunk_size: int, limit: int | None
) -> Iterable[list[dict]]:
    """Yield record-lists from the CSV, optionally stopping at `limit` rows."""
    rows_seen = 0
    reader = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
    for chunk in reader:
        if limit is not None and rows_seen + len(chunk) > limit:
            chunk = chunk.iloc[: limit - rows_seen]
        rows_seen += len(chunk)
        yield _coerce_chunk(chunk)
        if limit is not None and rows_seen >= limit:
            break


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Path to AIS CSV file")
    parser.add_argument(
        "--workers", type=int, default=max(1, os.cpu_count() - 1),
        help="Number of worker processes (default: CPUs - 1)"
    )
    parser.add_argument(
        "--chunk-size", type=int, default=50_000,
        help="Rows per chunk (default: 50000)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Insert at most N rows (for smoke testing)"
    )
    parser.add_argument(
        "--drop", action="store_true",
        help="Drop the target collection first"
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        sys.exit(f"CSV not found: {args.csv}")

    if args.drop:
        print(f"Dropping {DB_NAME}.{COLLECTION_NAME}...")
        MongoClient(MONGO_URI)[DB_NAME][COLLECTION_NAME].drop()

    print(
        f"Inserting from {args.csv}  "
        f"(workers={args.workers}, chunk_size={args.chunk_size}, "
        f"limit={args.limit or 'all'})"
    )

    start = time.time()
    total_inserted = 0
    total_errors = 0
    chunks_done = 0

    with mp.Pool(processes=args.workers) as pool:
        for inserted, errors in pool.imap_unordered(
            _worker, stream_chunks(args.csv, args.chunk_size, args.limit)
        ):
            total_inserted += inserted
            total_errors += errors
            chunks_done += 1
            elapsed = time.time() - start
            rate = total_inserted / elapsed if elapsed > 0 else 0
            print(
                f"  chunk {chunks_done:>4} | "
                f"inserted={total_inserted:>10,} | "
                f"errors={total_errors:>4} | "
                f"{rate:>8,.0f} docs/s | "
                f"{elapsed:>6.1f}s",
                flush=True,
            )

    elapsed = time.time() - start
    print(
        f"\nDone. {total_inserted:,} docs inserted, "
        f"{total_errors} errors, "
        f"in {elapsed:.1f}s ({total_inserted / elapsed:,.0f} docs/s)."
    )


if __name__ == "__main__":
    main()
