# Initialize the sharded cluster after `docker compose up -d`.
# Idempotent -- safe to re-run.
#
# Usage (from project root):
#   .\scripts\init-cluster.ps1
#
# If you get an execution policy error, run this once in PowerShell:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

$ErrorActionPreference = "Stop"

Write-Host "==> Step 1/4: Initiate config server replica set" -ForegroundColor Cyan
docker exec configsvr mongosh --port 27019 --quiet --eval @'
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
'@

Write-Host "==> Step 2/4: Initiate shard1 replica set" -ForegroundColor Cyan
docker exec shard1a mongosh --port 27018 --quiet --eval @'
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
'@

Write-Host "==> Waiting 15s for primary elections to settle..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

Write-Host "==> Step 3/4: Register shard1 with mongos" -ForegroundColor Cyan
docker exec mongos mongosh --port 27017 --quiet --eval @'
const result = sh.addShard("shard1ReplSet/shard1a:27018,shard1b:27018,shard1c:27018");
printjson(result);
'@

Write-Host "==> Step 4/4: Enable sharding on `ais` and shard `vessels`" -ForegroundColor Cyan
docker exec mongos mongosh --port 27017 --quiet --eval @'
sh.enableSharding("ais");
try {
  sh.shardCollection("ais.vessels", { MMSI: "hashed" });
  print("ais.vessels sharded on hashed MMSI");
} catch (e) {
  print("ais.vessels shard config: " + e.message);
}
'@

Write-Host ""
Write-Host "==> Cluster ready. Connect with: mongodb://localhost:27017" -ForegroundColor Green
Write-Host "==> Quick check: .\scripts\status.ps1" -ForegroundColor Green
