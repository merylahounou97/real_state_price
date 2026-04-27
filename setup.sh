#!/bin/bash

echo "-------------------------------------------------------"
echo "🚀 INITIALISATION DE LA DATA STACK"
echo "-------------------------------------------------------"

# 1. Reset complet
docker compose down
docker compose up -d --build

echo "⏳ Attente du démarrage de base (45s)..."
sleep 45

# 2. Configuration PostgreSQL
echo "🐘 Postgres : Création des bases..."
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE superset;" || true
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE airflow;" || true

# 3. Permission Socket Docker pour Airflow
echo "🐳 Docker : Permission socket..."
docker exec -i airflow chmod 666 /var/run/docker.sock

# 4. Attente spécifique pour l'Overlord Druid 
echo "⏳ Attente de l'Overlord Druid (90s)..."
sleep 90

# 5. Vérification que l'Overlord Druid est prêt
echo "🔍 Vérification de l'Overlord Druid..."
DRUID_READY=false
for i in $(seq 1 10); do
  LEADER=$(docker exec druid wget -qO- http://localhost:8081/druid/indexer/v1/leader 2>/dev/null)
  if [ -n "$LEADER" ]; then
    echo "✅ Overlord Druid prêt ! Leader : $LEADER"
    DRUID_READY=true
    break
  fi
  echo "   Tentative $i/10 — Overlord pas encore prêt, attente 15s..."
  sleep 15
done

if [ "$DRUID_READY" = false ]; then
  echo "⚠️  Overlord Druid toujours pas prêt après 10 tentatives."
  echo "   Consulte les logs : docker exec druid tail -50 /opt/druid/var/sv/coordinator-overlord.log"
fi

echo "-------------------------------------------------------"
echo "✅ SETUP TERMINÉ !"
echo "-------------------------------------------------------"
echo "👉 Airflow  : http://localhost:8080 (admin/admin)"
echo "👉 Druid    : http://localhost:8888"
echo "-------------------------------------------------------"