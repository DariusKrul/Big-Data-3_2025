#!/usr/bin/env python3
"""
Task 3: Filter noise from the inserted AIS data.

Two passes:
  1. ROW-LEVEL VALIDITY  -- drop rows with missing/invalid required fields
     (Navigational status, MMSI, Latitude, Longitude, ROT, SOG, COG, Heading)
  2. VESSEL-LEVEL DENSITY -- drop vessels with fewer than 100 valid points

Output collection: ais.vessels_filtered  (sharded on hashed MMSI)

Two execution modes:
  --mode server    Single server-side $merge pipeline. Fast baseline.
  --mode parallel  Client-side multiprocessing pool, one MongoClient per
                   worker, work split into MMSI-hash buckets. This is the
                   "assignment-style" path that demonstrates parallel
                   client-side processing with separate MongoClient instances.

Usage:
    python filter_noise.py --mode server --drop
    python filter_noise.py --mode parallel --workers 8 --drop
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import time
from pymongo import MongoClient, ASCENDING, InsertOne
from pymongo.errors import BulkWriteError

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
SOURCE_COLL = "vessels"
TARGET_COLL = "vessels_filtered"

REQUIRED_FIELDS = [
    "MMSI", "Latitude", "Longitude", "Navigational status",
    "ROT", "SOG", "COG", "Heading",
]

# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------
def validity_match() -> dict:
    """The $match stage that drops rows with missing/invalid required fields.

    Used by BOTH the server-side pipeline AND the parallel workers (so the
    two modes apply identical row-level validity criteria).
    """
    match: dict = {f: {"$ne": None, "$exists": True} for f in REQUIRED_FIELDS}
    match["Latitude"] = {"$ne": None, "$gte": -90, "$lte": 90}
    match["Longitude"] = {"$ne": None, "$gte": -180, "$lte": 180}
    match["MMSI"] = {"$gt": 0}
    return match


def ensure_indexes():
    """Create indexes used by the filtering pipeline."""
    client = MongoClient(MONGO_URI)
    src = client[DB_NAME][SOURCE_COLL]
    tgt = client[DB_NAME][TARGET_COLL]

    src.create_index([("MMSI", ASCENDING)])
    tgt.create_index([("MMSI", ASCENDING), ("Timestamp", ASCENDING)])


def shard_target_collection():
    """Shard the (empty) target collection on hashed MMSI before writing."""
    client = MongoClient(MONGO_URI)
    # Hashed shard key requires a matching index first
    client[DB_NAME][TARGET_COLL].create_index([("MMSI", "hashed")])
    try:
        client["admin"].command(
            "shardCollection", f"{DB_NAME}.{TARGET_COLL}",
            key={"MMSI": "hashed"},
        )
    except Exception as e:
        msg = str(e)
        if "already sharded" not in msg.lower():
            print(f"  shardCollection note: {msg}")


# --------------------------------------------------------------------------
# MODE 1: SERVER-SIDE -- single $merge pipeline
# --------------------------------------------------------------------------
def run_server_mode(min_points: int):
    """Filter using a server-side aggregation. Fast baseline."""
    client = MongoClient(MONGO_URI)
    src = client[DB_NAME][SOURCE_COLL]

    pipeline = [
        {"$match": validity_match()},
        # Tag each row with its vessel's total valid-row count
        {"$setWindowFields": {
            "partitionBy": "$MMSI",
            "output": {"_vessel_count": {"$count": {}}},
        }},
        {"$match": {"_vessel_count": {"$gte": min_points}}},
        {"$unset": "_vessel_count"},
        {"$merge": {
            "into": TARGET_COLL,
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]

    print(f"Running server-side filter (min_points={min_points})...")
    start = time.time()
    list(src.aggregate(pipeline, allowDiskUse=True))
    elapsed = time.time() - start

    src_count = src.estimated_document_count()
    tgt_count = client[DB_NAME][TARGET_COLL].estimated_document_count()
    print(
        f"Server mode done in {elapsed:.1f}s. "
        f"{src_count:,} -> {tgt_count:,} docs "
        f"({100 * tgt_count / max(src_count, 1):.1f}% retained)."
    )


# --------------------------------------------------------------------------
# MODE 2: PARALLEL CLIENT-SIDE -- worker pool, separate MongoClient per worker
# --------------------------------------------------------------------------
# Per-worker lazy client (one per process)
_worker_client: MongoClient | None = None


def _get_worker_client() -> MongoClient:
    global _worker_client
    if _worker_client is None:
        _worker_client = MongoClient(MONGO_URI)
    return _worker_client


def _process_mmsi_bucket(args: tuple[list[int], int]) -> tuple[int, int]:
    """Worker entrypoint. Returns (rows_copied, errors)."""
    mmsi_list, _bucket_id = args
    if not mmsi_list:
        return 0, 0

    client = _get_worker_client()
    src = client[DB_NAME][SOURCE_COLL]
    tgt = client[DB_NAME][TARGET_COLL]

    # Apply row-level validity AND restrict to this bucket's MMSIs.
    match = validity_match()
    match["MMSI"] = {"$in": mmsi_list}  # overrides the $gt:0 with $in (qualifying MMSIs only)

    cursor = src.find(match, {"_id": 0}).batch_size(5000)

    batch: list[InsertOne] = []
    BATCH_SIZE = 5000
    inserted = 0
    errors = 0

    for doc in cursor:
        batch.append(InsertOne(doc))
        if len(batch) >= BATCH_SIZE:
            try:
                result = tgt.bulk_write(batch, ordered=False)
                inserted += result.inserted_count
            except BulkWriteError as e:
                inserted += e.details.get("nInserted", 0)
                errors += len(e.details.get("writeErrors", []))
            batch = []

    if batch:
        try:
            result = tgt.bulk_write(batch, ordered=False)
            inserted += result.inserted_count
        except BulkWriteError as e:
            inserted += e.details.get("nInserted", 0)
            errors += len(e.details.get("writeErrors", []))

    return inserted, errors


def run_parallel_mode(min_points: int, workers: int):
    """Filter using a multiprocessing pool with one MongoClient per worker."""
    main_client = MongoClient(MONGO_URI)
    src = main_client[DB_NAME][SOURCE_COLL]

    # Step 1: identify qualifying vessels (those passing BOTH filters).
    print(f"Identifying qualifying vessels (min_points={min_points})...")
    start = time.time()
    qualifying = list(src.aggregate([
        {"$match": validity_match()},
        {"$group": {"_id": "$MMSI", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gte": min_points}}},
        {"$project": {"_id": 1}},
    ], allowDiskUse=True))
    mmsis = [d["_id"] for d in qualifying]
    print(f"  {len(mmsis):,} qualifying vessels found in {time.time() - start:.1f}s.")

    if not mmsis:
        print("Nothing to do.")
        return

    # Step 2: split MMSIs into worker buckets (round-robin)
    buckets: list[list[int]] = [[] for _ in range(workers)]
    for i, mmsi in enumerate(mmsis):
        buckets[i % workers].append(mmsi)
    print(f"Split into {workers} buckets of ~{len(mmsis) // workers} MMSIs each.")

    # Step 3: run the pool -- each worker uses its OWN MongoClient
    print("Copying valid rows to vessels_filtered (parallel)...")
    start = time.time()
    total_inserted = 0
    total_errors = 0

    with mp.Pool(processes=workers) as pool:
        for inserted, errors in pool.imap_unordered(
            _process_mmsi_bucket,
            [(b, i) for i, b in enumerate(buckets)],
        ):
            total_inserted += inserted
            total_errors += errors
            elapsed = time.time() - start
            print(
                f"  worker done | "
                f"inserted={total_inserted:>10,} | "
                f"errors={total_errors} | "
                f"{elapsed:.1f}s",
                flush=True,
            )

    elapsed = time.time() - start
    src_count = src.estimated_document_count()
    print(
        f"\nParallel mode done in {elapsed:.1f}s. "
        f"{src_count:,} -> {total_inserted:,} docs "
        f"({100 * total_inserted / max(src_count, 1):.1f}% retained), "
        f"{total_errors} errors."
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["server", "parallel"], default="parallel",
                        help="server: single $merge aggregation (baseline). "
                             "parallel: multiprocessing pool with separate "
                             "MongoClient per worker (assignment-style).")
    parser.add_argument("--min-points", type=int, default=100,
                        help="Min valid data points per vessel (default: 100)")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                        help="Worker processes for parallel mode")
    parser.add_argument("--drop", action="store_true",
                        help="Drop the target collection first")
    args = parser.parse_args()

    if args.drop:
        print(f"Dropping {DB_NAME}.{TARGET_COLL}...")
        MongoClient(MONGO_URI)[DB_NAME][TARGET_COLL].drop()

    ensure_indexes()
    shard_target_collection()

    if args.mode == "server":
        run_server_mode(args.min_points)
    else:
        run_parallel_mode(args.min_points, args.workers)


if __name__ == "__main__":
    main()
