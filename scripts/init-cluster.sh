#!/usr/bin/env bash
# Initialize the sharded cluster after `docker compose up -d`.
# Idempotent — safe to re-run.
#
# Usage:  bash scripts/init-cluster.sh

set -euo pipefail

echo "==> Step 1/4: Initiate config server replica set"
docker exec configsvr mongosh --port 27019 --quiet --eval '
  try {
    rs.status();
    print("config replica set already initialized");
  } catch (e) {
    rs.initiate({
      _id: "configReplSet",
      configsvr: true,
      members: [{ _id: 0, host: "configsvr:27019" }]
    });
    print("config replica set initiated");
  }
'

echo "==> Step 2/4: Initiate shard1 replica set"
docker exec shard1a mongosh --port 27018 --quiet --eval '
  try {
    rs.status();
    print("shard1 replica set already initialized");
  } catch (e) {
    rs.initiate({
      _id: "shard1ReplSet",
      members: [
        { _id: 0, host: "shard1a:27018", priority: 2 },
        { _id: 1, host: "shard1b:27018", priority: 1 },
        { _id: 2, host: "shard1c:27018", priority: 1 }
      ]
    });
    print("shard1 replica set initiated");
  }
'

echo "==> Waiting 15s for primary elections to settle..."
sleep 15

echo "==> Step 3/4: Register shard1 with mongos"
docker exec mongos mongosh --port 27017 --quiet --eval '
  const result = sh.addShard("shard1ReplSet/shard1a:27018,shard1b:27018,shard1c:27018");
  printjson(result);
'

echo "==> Step 4/4: Enable sharding on `ais` database and shard the `vessels` collection"
docker exec mongos mongosh --port 27017 --quiet --eval '
  // Enable sharding on the database
  sh.enableSharding("ais");

  // Shard the `vessels` collection on hashed MMSI.
  // Hashed gives even distribution; using MMSI as the key keeps each
  // vessel`s data on a single shard, which makes per-vessel aggregation cheap.
  // Note: we have only 1 shard right now so chunks all live there, but
  // the cluster is *ready* to scale to N shards by just adding them.
  try {
    sh.shardCollection("ais.vessels", { MMSI: "hashed" });
    print("ais.vessels sharded on hashed MMSI");
  } catch (e) {
    print("ais.vessels shard config: " + e.message);
  }
'

echo ""
echo "==> Cluster ready. Connect with:  mongodb://localhost:27017"
echo "==> Quick check:                  bash scripts/status.sh"
