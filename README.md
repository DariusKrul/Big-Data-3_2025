# Big Data Analysis — Task 3: Vessel Data Filtering with Sharded MongoDB

Sharded MongoDB cluster + parallel CSV ingestion + noise filtering + delta-t
histogram, on Danish Maritime Authority AIS vessel data.

**Course:** Big Data Analysis, Vilnius University (MSc Data Science)
**Assignment:** Task 3

---

## Cluster topology

```
         ┌─────────────┐
client → │   mongos    │ :27017   (only port exposed to host)
         └──────┬──────┘
                │
        ┌───────┴────────┐
        ▼                ▼
┌──────────────┐   ┌──────────────────────────────┐
│  configsvr   │   │  shard1ReplSet (3 nodes)     │
│  (replSet)   │   │  shard1a / shard1b / shard1c │
└──────────────┘   └──────────────────────────────┘
```

- **1 config server** (single-node replica set) — cluster metadata
- **1 shard** as a **3-node replica set** — actual data + failover
- **1 mongos router** — single client entrypoint

Sharding key for `ais.vessels` and `ais.vessels_filtered`: **hashed `MMSI`**.

---

## Quick start

```bash
# 1. Bring up the cluster
docker compose up -d

# 2. Initialize replica sets, register shard, enable sharding
bash scripts/init-cluster.sh

# 3. Verify
bash scripts/status.sh

# 4. Install Python deps
pip install -r requirements.txt

# 5. Smoke test: insert 100k rows
python src/insert_parallel.py --csv data/aisdk-YYYY-MM-DD.csv --limit 100000 --drop

# 6. Full insert
python src/insert_parallel.py --csv data/aisdk-YYYY-MM-DD.csv --drop

# 7. Filter noise
python src/filter_noise.py --drop

# 8. (Tuesday) Delta-t histogram
python src/delta_t_histogram.py
```

---

## Failover demo (Task 5)

```bash
# Watch the replica set
watch -n 1 'docker exec shard1a mongosh --port 27018 --quiet --eval "rs.status().members.forEach(m => print(m.name, m.stateStr))"'

# Kill the primary
docker stop shard1a

# Within ~10 seconds, shard1b or shard1c becomes the new primary.
# Queries via mongos:27017 keep working throughout.

# Bring it back
docker start shard1a
# It rejoins as a SECONDARY.
```

---

## File layout

```
big-data-task3/
├── docker-compose.yml      # 5 services: configsvr, shard1a/b/c, mongos
├── scripts/
│   ├── init-cluster.sh     # rs.initiate() + sh.addShard() + sh.shardCollection()
│   └── status.sh           # cluster health snapshot
├── src/
│   ├── insert_parallel.py  # Task 2: parallel CSV ingestion
│   ├── filter_noise.py     # Task 3: noise filtering
│   └── delta_t_histogram.py # Task 4: delta-t + histogram (TBD)
├── requirements.txt
└── README.md
```
