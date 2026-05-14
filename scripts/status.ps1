# Quick health check of the cluster.

Write-Host "==> Container status" -ForegroundColor Cyan
docker compose ps

Write-Host ""
Write-Host "==> Shard status (sh.status)" -ForegroundColor Cyan
docker exec mongos mongosh --port 27017 --quiet --eval "sh.status()"

Write-Host ""
Write-Host "==> Shard1 replica set status" -ForegroundColor Cyan
docker exec shard1a mongosh --port 27018 --quiet --eval @'
const s = rs.status();
print("Set: " + s.set);
s.members.forEach(m => print("  " + m.name + "  " + m.stateStr));
'@
