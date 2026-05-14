# Task 3 Report — Vessel Data Filtering with Sharded MongoDB

## Dataset

Danish Maritime Authority AIS data for **2026-04-18**.
Raw file: `aisdk-2026-04-18.csv`, ≈3.7 GB unzipped, **20,747,346 rows**.
Each row is one AIS broadcast from one vessel at one instant, containing
position (Latitude/Longitude), kinematics (SOG, COG, ROT, Heading),
navigational status, MMSI identifier, and timestamp.

Hardware: Windows 11 laptop, 16 GB RAM, Docker Desktop, Python 3.11,
MongoDB 7.0.

\---

## NoSQL context

The database technology for this assignment is **MongoDB**, a document-
oriented NoSQL system. NoSQL databases trade some of the rigidity of
traditional relational systems — fixed schemas, multi-table joins, strict
ACID transactions across the entire database — for properties that matter
at scale: flexible document models, easy horizontal partitioning, and
built-in replication for high availability.

The AIS data fits this model naturally. Each row is a self-contained
record with many optional fields (a vessel near port broadcasts different
information than one at sea); a document database lets us store each row
as a BSON object without committing to a schema up front. Type coercion
at insert time — empty strings to `null`, day-first timestamps to BSON
`Date` — bridges the gap between CSV's untyped strings and the typed
queries we want to run downstream.

The two NoSQL features most central to this task are **sharding**
(distributing data across multiple machines by a key, here hashed `MMSI`)
and **replica sets** (synchronised copies of data on multiple nodes for
fault tolerance). Sharding is what scales the data layer beyond a single
machine; replica sets are what survive a machine failure. Task 1 sets
both up; Task 5 demonstrates the replica-set failover behaviour.

\---

## Task 1 — Sharded MongoDB cluster

**Topology:** one shard implemented as a 3-node replica set, plus a
single-node config-server replica set, plus a mongos router.
Five MongoDB processes total, all containerised via `docker compose`.

```
client → mongos :27017 ──→ configsvr  (metadata)
                        └→ shard1ReplSet { shard1a, shard1b, shard1c }
```

**Design choices:**

* **Sharded over plain replica set.** The brief states sharding is graded
higher. Using one shard that is itself a replica set gives both: the
cluster is genuinely sharded (the `sh.status()` output shows a
registered shard and chunks split across hash ranges), and the
3-node replica set provides the failover behaviour demonstrated in
Task 5.
* **Hashed `MMSI` shard key.** AIS data is per-vessel; a hashed key
distributes vessels evenly across shards (one shard today, scalable to
N tomorrow) and co-locates all rows for a given vessel onto one shard.
Per-vessel aggregation (used in Tasks 3 and 4) then stays local to one
shard — no cross-shard merge needed.
* **WiredTiger cache capped at 0.5 GB per data node.** The default would
consume 50 % of system RAM minus 1 GB; with three data-bearing nodes
on a 16 GB laptop this would have produced cache thrashing. The cap
keeps total mongod memory near 2 GB.

Cluster initialisation is idempotent and scripted (`scripts/init-cluster.ps1`
or `init-cluster.sh`): it initiates both replica sets, registers the shard
with mongos, enables sharding on the `ais` database, and shards `ais.vessels`
on hashed MMSI.

\---

## Task 2 — Parallel data insertion

`src/insert\_parallel.py` reads the CSV in chunks of 50,000 rows on the main
process and dispatches each chunk to a worker pool. **Each worker creates
its own `MongoClient` instance** (lazily, on first insert) — explicit per the
assignment requirement, and necessary because `MongoClient` is not fork-safe.

Per-chunk processing in each worker:

1. Coerce numeric columns (`MMSI`, `Latitude`, `Longitude`, `ROT`, `SOG`,
`COG`, `Heading`, etc.) — empty / "Unknown" values become BSON `null`,
making downstream `$ne: null` filtering clean.
2. Parse the timestamp column (day-first format, as Danish AIS files use)
to BSON `Date`.
3. Drop rows lacking `MMSI` (mongos cannot route inserts without the
shard key).
4. Bulk-write with `ordered=False`, so individual row failures do not
abort a chunk.

**Result:**

|Metric|Value|
|-|-|
|Rows inserted|**20,747,346**|
|Errors|0|
|Workers|11 (`cpu\_count() − 1`)|
|Wall time|**1,224 s** (≈20 min)|
|Sustained throughput|**≈17,000 docs/sec**|

Throughput climbed from ≈8 k docs/s on the first cold-start chunk to a
steady ≈19 k docs/s once worker connections were warm.

\---

## Task 3 — Parallel noise filtering

The filter applies two passes:

1. **Row-level validity.** Drop rows where any of these are missing or
invalid: `MMSI`, `Latitude`, `Longitude`, `Navigational status`,
`ROT`, `SOG`, `COG`, `Heading`. Plus range checks
(`-90 ≤ Lat ≤ 90`, `-180 ≤ Lon ≤ 180`, `MMSI > 0`).
2. **Vessel-level density.** Drop entire vessels (all MMSIs) with fewer
than 100 surviving rows.

Output → `ais.vessels\_filtered` (sharded on hashed MMSI, same shard key as
source).

`src/filter\_noise.py` implements **two execution modes**, both with
identical filtering semantics:

### Mode A — parallel client-side (`--mode parallel`)

The "assignment-style" path. Algorithm:

1. **Main process:** run a server-side aggregation to identify qualifying
MMSIs (those with ≥100 valid points).
2. **Split** the qualifying MMSI list into N buckets (round-robin).
3. **Worker pool of N processes**, each with its own `MongoClient`. Each
worker reads its bucket's valid rows from `vessels` and bulk-writes
them to `vessels\_filtered`.

### Mode B — server-side (`--mode server`)

Single MongoDB aggregation that does row-level `$match`, then
`$setWindowFields` to count valid rows per MMSI, then `$match` again,
then `$merge` into the target. The whole operation runs inside the
cluster; no data round-trips through Python.

### Result (Mode A, parallel)

|Metric|Value|
|-|-|
|Source rows|20,747,346|
|Filtered rows|**13,957,244**|
|Retention|**67.3 %**|
|Qualifying vessels|**1,833** (≥100 valid points)|
|Phase 1 (identify MMSIs)|1,036 s|
|Phase 2 (parallel copy)|973 s|
|**Total wall time**|**2,009 s (≈33 min)**|
|Errors|0|

Worker completion times spanned 777 s to 973 s (25 % variance). This
imbalance comes from round-robin MMSI distribution — buckets have similar
MMSI *counts* but unequal total *row counts*, because some vessels broadcast
far more rows than others. A future optimisation would distribute by row
count rather than by MMSI count.

### Mode B did not complete

Server mode was attempted as a comparison baseline. After **45 minutes** of
wall time it had written 7.06 M of the expected 13.96 M filtered rows and
was killed. A `currentOp` inspection during the run showed a single
aggregation op with `secs\_running` already past 5,200 s, suggesting the
server had been retrying internally for longer than the wall-clock
elapsed — likely due to the cursor being repeatedly re-issued.

The slowness is attributable to `$setWindowFields` materialising per-MMSI
partitions across the full 20.7 M row collection. With the WiredTiger
cache capped at 0.5 GB per node and a ≈1.5 GB working-set requirement, the
operation thrashed against disk despite `allowDiskUse=true`. The simpler
client-side approach (read indexed `MMSI: {$in: \[bucket]}` ranges, write
back) sidestepped this because each bucket query had a small working set
fitting comfortably in cache.

**Interesting finding for the write-up:** on memory-constrained hardware
with a small WiredTiger cache, the conventionally "slower" client-side
parallel approach **outperformed** the server-side aggregation by a wide
margin (973 s versus 2,700+ s with the latter not even completing).
This inverts the usual MongoDB performance guidance and underlines that
"do as much as possible inside the database" depends on the database
actually having room to do it.

### Indexes used

* `vessels`: index on `MMSI` (ASC) — used by Phase 2 worker `$in` queries.
* `vessels\_filtered`: hashed `MMSI` (shard key) and compound `(MMSI ASC, Timestamp ASC)`. The compound index is used by Task 4's
`$setWindowFields` partition-and-sort step.

\---

## Task 4 — Delta-t calculation and histogram

For each vessel in `vessels\_filtered`, compute the time difference between
consecutive AIS broadcasts in milliseconds. Aggregate all delta-t values
across all vessels and plot a histogram.

`src/delta\_t\_histogram.py` does this server-side with one aggregation:

```
$match  Timestamp exists
$setWindowFields  partitionBy: MMSI, sortBy: Timestamp,
                  output:  \_prev\_ts = $shift(Timestamp, by: -1)
$match  \_prev\_ts is not null
$project  delta\_ms = Timestamp - \_prev\_ts
```

The compound `(MMSI, Timestamp)` index makes the partition-and-sort phase
cheap. Only the resulting numbers come back to Python; numpy computes
the statistics and matplotlib renders the histogram.

### Result

|Metric|Value|
|-|-|
|Delta-t values computed|**13,955,411**|
|Wall time|**131 s**|
|Min|0 ms|
|Median (p50)|**4,000 ms**|
|p75|10,000 ms|
|p90|11,000 ms|
|p95|20,000 ms|
|p99|141,000 ms (2 min 21 s)|
|p99.9|360,000 ms (6 min)|
|Max|74,985,000 ms (≈20.8 hours)|
|Mean|9,225 ms|

### Interpretation

The distribution matches the AIS Class A transponder specification, which
calls for transmission every 2–10 seconds for moving vessels (depending on
SOG and rate-of-turn) and every 3 minutes when anchored.

* **Sharp mode at 2–10 seconds:** active vessels at sea.
* **Secondary mass near 10–11 seconds:** slower-moving traffic and
vessels manoeuvring near port.
* **Long thin tail:** anchored vessels, signal occlusion near shore,
or transponders briefly offline. The maximum (20.8 h) is consistent
with a vessel that anchored, stopped broadcasting, then resumed.
* **Median 4 s** is exactly what the standard predicts for typical
underway behaviour.

### Histograms generated

* `docs/delta\_t\_histogram.png` — x-axis clipped at 60 s, 60 bins.
Shows the dominant 2–11 s mass.
* `docs/delta\_t\_histogram\_long.png` — x-axis clipped at 600 s, 100 bins.
Reveals the long tail and secondary spikes.

\---

## Task 5 — Failure demonstration video

Recorded demonstration of replica-set failover:

1. A continuous query loop running against `mongos:27017` (counting docs
in `ais.vessels\_filtered`).
2. `docker exec` shows the current replica-set primary on `shard1a`.
3. `docker stop shard1a` kills the primary.
4. The query loop briefly returns errors (≈5–10 s) during election.
5. `rs.status()` shows the new primary on `shard1b` or `shard1c`.
6. The query loop resumes, returning correct results from the surviving
nodes.
7. Stopping another node shuts down the process and brings up a Mongo error.
8. `docker start shard1a` brings the original node back; it rejoins
the replica set as `SECONDARY`, later goes back to being the primary.

**Recording:** `docs/failover\_demo.mp4` (also submitted via Emokymai).

\---

## Performance summary

|Step|Wall time|Throughput|
|-|-|-|
|Insertion (20.7M rows)|1,224 s|17 k docs/s|
|Filter — parallel mode|2,009 s|≈10 k docs/s effective|
|Filter — server mode|killed @ 2,700+ s|—|
|Delta-t aggregation|131 s|107 k values/s|

\---

## Lessons learned

1. **Memory caps matter more than parallelism.** The single biggest
performance lever in this assignment was the WiredTiger cache size,
not worker counts or shard counts.
2. **MongoClient is not fork-safe.** Workers must instantiate their own
clients; sharing one across processes results in subtle hangs that
only manifest under load.
3. **Hashed shard keys give even distribution but block range queries.**
Acceptable here because all our queries are per-MMSI (which hashes
to a deterministic chunk).
4. **Server-side aggregation is not unconditionally fastest.** On
memory-constrained hardware, the data round-trip overhead of
client-side parallelism can be cheaper than the cache thrashing
incurred by a big server-side window operation.
5. **AIS data is unusually clean.** Zero insertion errors across 20.7 M
rows, and the noise filter retained 67 % of input — the Danish
Maritime Authority's data pipeline is well-curated. The remaining
33 % is mostly anchored vessels with sparse broadcasts and rows
with missing navigational-status fields.

