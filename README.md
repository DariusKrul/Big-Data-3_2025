# Big Data Analysis — Task 3: Vessel Data Filtering with Sharded MongoDB

Sharded MongoDB cluster, parallel CSV ingestion, parallel noise filtering, and
delta-t histogram analysis on Danish Maritime Authority AIS vessel data.

**Course:** Big Data Analysis, Vilnius University (MSc Data Science)
**Dataset:** `aisdk-2026-04-18.csv` (Danish Maritime AIS, ≈3.7 GB / 20.7M rows)

See **[REPORT.md](REPORT.md)** for findings, design decisions, and results.

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

- **1 config server** as a single-node replica set — cluster metadata
- **1 shard** as a **3-node replica set** — data + failover capability
- **1 mongos router** — single client entrypoint on port 27017
- **Shard key** for `ais.vessels` and `ais.vessels_filtered`: hashed `MMSI`

---

## Quick start

```powershell
# 1. Bring up the cluster (Docker Desktop must be running)
docker compose up -d

# 2. Initialize replica sets, register shard, enable sharding
.\scripts\init-cluster.ps1     # PowerShell
# or
bash scripts/init-cluster.sh   # Git Bash / WSL / Linux

# 3. Verify
.\scripts\status.ps1

# 4. Install Python deps
pip install -r requirements.txt

# 5. Smoke test: insert 100k rows
python src/insert_parallel.py --csv data/aisdk-2026-04-18.csv --limit 100000 --drop

# 6. Full insert
python src/insert_parallel.py --csv data/aisdk-2026-04-18.csv --drop

# 7. Filter noise (parallel mode = assignment-style)
python src/filter_noise.py --mode parallel --drop

# 8. Delta-t histogram
python src/delta_t_histogram.py
python src/delta_t_histogram.py --max-seconds 600 --bins 100 --out delta_t_histogram_long.png
```

---

## Failover demo (Task 5)

```powershell
# In window 1: watch the replica set state
while ($true) {
  docker exec shard1a mongosh --port 27018 --quiet --eval `
    'rs.status().members.forEach(m => print(m.name, m.stateStr))'
  Start-Sleep -Seconds 2
  Clear-Host
}

# In window 2: kill the current primary
docker stop shard1a

# Within ~10 seconds shard1b or shard1c is elected new PRIMARY.
# Queries via mongos:27017 keep working throughout.

# Bring shard1a back -- it rejoins as a SECONDARY
docker start shard1a
```

See [REPORT.md](REPORT.md) §5 for the recorded demonstration video reference.

---

## File layout

```
big-data-task3/
├── data/                      # AIS CSV files (gitignored — populate locally)
├── docs/
│   ├── delta_t_histogram.png      # 60-second clip histogram
│   ├── delta_t_histogram_long.png # 600-second clip histogram
│   └── failover_demo.mp4          # Task 5 video
├── docker-compose.yml         # configsvr + shard1a/b/c + mongos
├── scripts/
│   ├── init-cluster.ps1       # PowerShell (Windows)
│   ├── init-cluster.sh        # Bash (Git Bash / WSL / Linux)
│   ├── status.ps1
│   └── status.sh
├── src/
│   ├── insert_parallel.py     # Task 2: parallel CSV → MongoDB
│   ├── filter_noise.py        # Task 3: noise filtering (server + parallel modes)
│   └── delta_t_histogram.py   # Task 4: delta-t + histogram
├── requirements.txt
├── REPORT.md                  # findings & analysis
└── README.md
```

---

## Notes

- AIS CSV files are excluded from git (see `.gitignore`). Put your downloaded
  file in `data/` locally.
- Tested on Windows 11 with Docker Desktop, Python 3.11, MongoDB 7.
- The cluster uses unauthenticated connections — fine for local development;
  do NOT expose port 27017 to the public internet.
