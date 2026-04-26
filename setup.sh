#!/bin/bash

echo "-------------------------------------------------------"
echo "🚀 INITIALISATION DE LA DATA STACK"
echo "-------------------------------------------------------"

# 1. Reset complet avec nettoyage des volumes
docker-compose down
docker-compose up -d --build

echo "⏳ Attente du démarrage global (60s)..."
sleep 45

# 2. Configuration PostgreSQL
echo "🐘 Postgres : Création des bases..."
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE superset;" || true
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE airflow;" || true

# 3. Initialisation de Superset
echo "📊 Superset : Initialisation..."
docker exec -i superset superset db upgrade
docker exec -i superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@fab.org --password admin
docker exec -i superset superset init

# 4. Permission Socket Docker pour Airflow
echo "🐳 Docker : Permission socket..."
docker exec -i airflow chmod 666 /var/run/docker.sock


echo "-------------------------------------------------------"
echo "✅ SETUP TERMINÉ ET PRÊT !"
echo "-------------------------------------------------------"
echo "👉 Airflow  : http://localhost:8080 (admin/admin)"
echo "👉 Superset : http://localhost:8088 (admin/admin)"
echo "👉 Druid    : http://localhost:8888"
echo "-------------------------------------------------------"