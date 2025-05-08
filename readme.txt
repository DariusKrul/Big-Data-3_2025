etl_parallel.py is a self-contained ETL script that does two things:

--stage raw
Streams a large AIS vessel-position CSV into MongoDB Atlas in parallel.
• Reads the file in ~50 k-row chunks → cleans core columns → pushes each chunk to a thread-pool (one MongoClient per worker) → bulk-inserts into ais.raw_vessels_limited.
• Flags: --threads (default = CPU count) and --row-cap to keep free-tier storage ≤ 512 MB.

--stage clean
Builds a filtered collection ready for analysis.
• First pass counts rows per MMSI to keep vessels with ≥ 100 points.
• Second pass copies only “good” rows (valid lat/long, good MMSI) into ais.clean_vessels, also in parallel, and adds MMSI + Timestamp indexes.


TYPICAL USAGE
# parallel raw load, capped at 200k rows, 8 threads
python etl_parallel.py --stage raw   --threads 8 --row-cap 200000

# build clean_vessels from the raw slice
python etl_parallel.py --stage clean --threads 8


Edit the two constants at the top—CSV_PATH and MONGO_URI—then run. The script prints total docs, elapsed time, and docs/s for easy benchmarking.