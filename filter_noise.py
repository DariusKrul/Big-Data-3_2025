#!/usr/bin/env python3
"""
Task 3: Filter noise from the inserted AIS data.

Strategy (two-pass):

  Pass 1 -- ROW-LEVEL VALIDITY (parallelizable per MMSI bucket)
    Drop rows with missing/invalid required fields:
      - Navigational status, MMSI, Latitude, Longitude, ROT, SOG, COG, Heading
    Latitude must be in [-90, 90], Longitude in [-180, 180], MMSI > 0, etc.

  Pass 2 -- VESSEL-LEVEL DENSITY
    Group surviving rows by MMSI, drop vessels with fewer than 100 points.

Output collection: ais.vessels_filtered (sharded on hashed MMSI, same as source).

This file is currently a STUB -- it lays out the structure and the
key Mongo operations. Polish (parallel workers, indexes, progress
reporting, validation tuning) lands tomorrow.

Usage (planned):
    python filter_noise.py
    python filter_noise.py --min-points 100 --workers 4
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import time
from pymongo import MongoClient, ASCENDING, HASHED

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ais"
SOURCE_COLL = "vessels"
TARGET_COLL = "vessels_filtered"

# Fields that must be present and non-null for a row to be considered valid.
REQUIRED_FIELDS = [
    "MMSI", "Latitude", "Longitude", "Navigational status",
    "ROT", "SOG", "COG", "Heading",
]


def ensure_indexes():
    """Create indexes that speed up the filtering pipeline.

    - MMSI on the source collection: needed for the per-vessel grouping.
    - Compound (MMSI, Timestamp) on the target: needed for delta-t in Task 4.
    """
    client = MongoClient(MONGO_URI)
    src = client[DB_NAME][SOURCE_COLL]
    tgt = client[DB_NAME][TARGET_COLL]

    src.create_index([("MMSI", ASCENDING)])
    # Target collection will be sharded on hashed MMSI; we also want
    # a compound index for the time-sorted scans Task 4 will do.
    tgt.create_index([("MMSI", ASCENDING), ("Timestamp", ASCENDING)])


def shard_target_collection():
    """Shard the target collection on hashed MMSI before bulk insertion.

    Sharding an empty collection is fast; sharding a populated one triggers
    chunk migration. So: shard first, then write.
    """
    client = MongoClient(MONGO_URI)
    admin = client["admin"]
    try:
        admin.command(
            "shardCollection", f"{DB_NAME}.{TARGET_COLL}",
            key={"MMSI": "hashed"}
        )
    except Exception as e:
        # Already sharded or other benign error -- log and continue
        print(f"shardCollection note: {e}")


def filter_pipeline(min_points: int) -> list[dict]:
    """Build the aggregation pipeline that does both filtering passes server-side.

    Doing this as a $merge pipeline runs entirely on the cluster -- no data
    flows back to the client. For ~10M-row datasets that's a major win.

    For the ASSIGNMENT we still want to demonstrate parallel CLIENT-side work,
    so the polished version will use this pipeline as a fallback / baseline
    and have a parallel Python path that splits MMSI ranges across workers.
    """
    valid_match = {"$match": {f: {"$ne": None, "$exists": True} for f in REQUIRED_FIELDS}}
    # Add range sanity checks
    valid_match["$match"].update({
        "Latitude": {"$gte": -90, "$lte": 90, "$ne": None},
        "Longitude": {"$gte": -180, "$lte": 180, "$ne": None},
        "MMSI": {"$gt": 0},
    })

    return [
        valid_match,
        # Tag each surviving row with a count of how many other valid rows
        # share its MMSI. Rows from sparse vessels get count < min_points
        # and are filtered out in the next stage.
        {
            "$setWindowFields": {
                "partitionBy": "$MMSI",
                "output": {"_vessel_count": {"$count": {}}},
            }
        },
        {"$match": {"_vessel_count": {"$gte": min_points}}},
        {"$unset": "_vessel_count"},
        # Write straight to the target collection (replace if exists).
        {"$merge": {
            "into": TARGET_COLL,
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def run_server_side_filter(min_points: int):
    """Execute the filter as a single server-side aggregation."""
    client = MongoClient(MONGO_URI)
    src = client[DB_NAME][SOURCE_COLL]
    print(f"Running server-side filter pipeline (min_points={min_points})...")
    start = time.time()
    list(src.aggregate(filter_pipeline(min_points), allowDiskUse=True))
    elapsed = time.time() - start
    tgt_count = client[DB_NAME][TARGET_COLL].estimated_document_count()
    src_count = src.estimated_document_count()
    print(
        f"Done in {elapsed:.1f}s. "
        f"{src_count:,} -> {tgt_count:,} docs "
        f"({100 * tgt_count / src_count:.1f}% retained)."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-points", type=int, default=100,
                        help="Minimum data points per vessel (default: 100)")
    parser.add_argument("--workers", type=int, default=max(1, os.cpu_count() - 1),
                        help="Worker processes for parallel client-side mode")
    parser.add_argument("--mode", choices=["server", "parallel"], default="server",
                        help="server: single $merge pipeline (fast). "
                             "parallel: client-side multiprocessing (assignment-style).")
    parser.add_argument("--drop", action="store_true",
                        help="Drop the target collection first")
    args = parser.parse_args()

    if args.drop:
        MongoClient(MONGO_URI)[DB_NAME][TARGET_COLL].drop()

    ensure_indexes()
    shard_target_collection()

    if args.mode == "server":
        run_server_side_filter(args.min_points)
    else:
        # TODO (Monday): implement parallel client-side path.
        # Approach: split MMSIs into N buckets by hash, give each worker a
        # bucket, each worker reads/writes via its own MongoClient.
        raise NotImplementedError("Parallel mode lands tomorrow.")


if __name__ == "__main__":
    main()
