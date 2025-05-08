# etl_parallel.py  •  Phases 3 & 4 combined
"""
Usage examples
--------------
# 1. Load a limited raw slice (<= 200k rows) with 8 threads
python etl_parallel.py --stage raw   --threads 8 --row-cap 200000

# 2. Build the cleaned collection in parallel (reads raw)
python etl_parallel.py --stage clean --threads 8
"""
# ------------------------------ CONFIG --------------------------------------
CSV_PATH  = r"C:\Users\d.krulikovskis\Downloads\aisdk-2025-05-01\aisdk-2025-05-01.csv"
MONGO_URI = (
    "mongodb+srv://<username>:<password>"
    "@cluster0.npxpl2f.mongodb.net/?retryWrites=true&w=majority&appName=cluster0"
)

RAW_COLL   = "raw_vessels_limited"
CLEAN_COLL = "clean_vessels"
DB_NAME    = "ais"

CHUNKSIZE  = 50_000        # one CSV chunk
BATCH_SIZE = 1_000         # one Mongo bulk insert

# -----------------------------------------------------------------------------
import os, time, argparse, math
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict
from pathlib import Path
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

DTYPE_MAP = {
    "Type of mobile": "category", "MMSI": "int64",
    "Latitude": "float64", "Longitude": "float64",
    "Navigational status": "category", "ROT": "float64",
    "SOG": "float64", "COG": "float64", "Heading": "float64",
    "IMO": "string", "Callsign": "string", "Name": "string",
    "Ship type": "category", "Cargo type": "string",
    "Width": "float64", "Length": "float64",
    "Type of position fixing device": "category",
    "Draught": "float64", "Destination": "string",
    "ETA": "string", "Data source type": "category",
    "A": "string", "B": "string", "C": "string", "D": "string",
}

REQ = ["MMSI", "Timestamp", "Latitude", "Longitude"]  # **trimmed**

def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

# ---------- Phase 3 • parallel RAW loader -----------------------------------
def stream_csv(row_cap: int | None):
    rows_read = 0
    for chunk in pd.read_csv(
        CSV_PATH,
        dtype=DTYPE_MAP,
        parse_dates=["# Timestamp"],
        chunksize=CHUNKSIZE,
    ):
        chunk = (
            chunk.rename(columns={"# Timestamp": "Timestamp"})
                 .drop(columns=["A", "B", "C", "D"])
                 .dropna(subset=REQ)
        )
        if row_cap:
            if rows_read >= row_cap:
                break
            allowed = row_cap - rows_read
            chunk   = chunk.head(allowed)
        rows_read += len(chunk)
        yield chunk

def insert_worker(docs, coll_name):
    client = get_client()
    coll   = client[DB_NAME][coll_name]
    # split into 1k-doc bulks
    for s in range(0, len(docs), BATCH_SIZE):
        coll.insert_many(docs[s:s+BATCH_SIZE], ordered=False)

def load_raw_parallel(threads, row_cap):
    client = get_client()
    client[DB_NAME][RAW_COLL].drop()
    client[DB_NAME][RAW_COLL].create_index("MMSI")

    start = time.time()
    total = 0

    with ThreadPoolExecutor(max_workers=threads) as tp:
        futures = []
        for chunk in stream_csv(row_cap):
            docs = chunk.to_dict("records")
            total += len(docs)
            futures.append(tp.submit(insert_worker, docs, RAW_COLL))
        # wait for all workers
        for f in tqdm(as_completed(futures), total=len(futures), desc="Inserting"):
            f.result()

    elapsed = time.time() - start
    print(f"RAW ► Inserted {total:,} docs in {elapsed:,.1f}s "
          f"({total/elapsed:,.0f} docs/s)")

# ---------- Phase 4 • parallel CLEAN builder --------------------------------
def vessel_counts():
    """one-pass counter (single thread)"""
    counter = Counter()
    for chunk in pd.read_csv(CSV_PATH, usecols=["MMSI"], chunksize=CHUNKSIZE):
        counter.update(chunk["MMSI"])
    return {m for m, c in counter.items() if c >= 100}

def noise_filter(df, good_mmsi):
    df = df[df["MMSI"].isin(good_mmsi)]
    # lat/lon sanity
    df = df[(df["Latitude"].between(-90, 90)) & (df["Longitude"].between(-180, 180))]
    return df

def filter_worker(slice_ids, good_mmsi):
    client = get_client()
    src  = client[DB_NAME][RAW_COLL]
    dest = client[DB_NAME][CLEAN_COLL]

    bulk = []
    for _id in slice_ids:
        doc = src.find_one({"_id": _id})
        if doc and doc["MMSI"] in good_mmsi:
            bulk.append(doc)
    if bulk:
        dest.insert_many(bulk, ordered=False)

def build_clean_parallel(threads):
    client   = get_client()
    raw_coll = client[DB_NAME][RAW_COLL]
    clean    = client[DB_NAME][CLEAN_COLL]
    clean.drop()
    clean.create_index("MMSI")
    clean.create_index("Timestamp")

    good_mmsi = vessel_counts()
    print(f"{len(good_mmsi):,} vessels have ≥ 100 pts")

    # get all _ids so we can shard them
    all_ids = list(raw_coll.find({}, {"_id": 1}))
    slice_size = math.ceil(len(all_ids)/threads)
    id_slices  = [ [d["_id"] for d in all_ids[i:i+slice_size]]
                   for i in range(0, len(all_ids), slice_size)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=threads) as tp:
        futures = [tp.submit(filter_worker, ids, good_mmsi) for ids in id_slices]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Filtering"):
            pass
    elapsed = time.time() - start
    print(f"CLEAN ► Inserted {clean.count_documents({}):,} docs "
          f"in {elapsed:,.1f}s")

# ---------- CLI glue ---------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["raw", "clean"], required=True,
                    help="raw = phase 3, clean = phase 4")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--row-cap", type=int, default=None,
                    help="hard limit for phase 3 (stay under 512 MB)")
    args = ap.parse_args()

    if args.stage == "raw":
        load_raw_parallel(args.threads, args.row_cap)
    else:
        build_clean_parallel(args.threads)

if __name__ == "__main__":
    main()
