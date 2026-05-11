#!/usr/bin/env bash
# Quick health check of the cluster.

echo "==> Container status"
docker compose ps

echo ""
echo "==> Shard status (sh.status)"
docker exec mongos mongosh --port 27017 --quiet --eval 'sh.status()'

echo ""
echo "==> Shard1 replica set status"
docker exec shard1a mongosh --port 27018 --quiet --eval '
  const s = rs.status();
  print("Set: " + s.set);
  s.members.forEach(m => print("  " + m.name + "  " + m.stateStr));
'
