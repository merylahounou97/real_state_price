#!/bin/bash

echo "-------------------------------------------------------"
echo "🚀 INITIALISATION DE LA DATA STACK (MODE SCRIPT)"
echo "-------------------------------------------------------"

# 1. Lancement des services
docker-compose up -d

echo "⏳ Attente du démarrage (45s)..."
sleep 45

# 2. Configuration PostgreSQL
echo "🐘 Postgres : Création des bases de données..."
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE superset;" || echo "Superset DB existe déjà."
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE airflow;" || echo "Airflow DB existe déjà."

# 3. Initialisation de Superset
echo "📊 Superset : Configuration..."
docker exec -i superset superset db upgrade
docker exec -i superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@fab.org \
    --password admin
docker exec -i superset superset init

# 4. Initialisation de Airflow
echo "🌬️ Airflow : Configuration..."
docker exec -i airflow airflow db init
docker exec -i airflow airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@fab.org \
    --password admin

echo "-------------------------------------------------------"
echo "✅ SETUP TERMINÉ !"
echo "-------------------------------------------------------"

echo "-------------------------------------------------------"
echo "✅ TOUT EST PRÊT !"
echo "-------------------------------------------------------"
echo "👉 Airflow  : http://localhost:8080 (admin/admin)"
echo "👉 Superset : http://localhost:8088 (admin/admin)"
echo "👉 Druid    : http://localhost:8888"
echo "-------------------------------------------------------"