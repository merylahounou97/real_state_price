#!/bin/bash

echo "-------------------------------------------------------"
echo "📊 DÉMARRAGE DE SUPERSET"
echo "-------------------------------------------------------"

# 1. Démarrage du conteneur Superset
echo "🚀 Démarrage du conteneur Superset..."
docker compose --profile viz up -d --build superset

echo "⏳ Attente du démarrage de Superset (20s)..."
sleep 20

# 2. Initialisation de la base de données
echo "🗄️  Initialisation de la base de données Superset..."
docker exec -i superset superset db upgrade

# 3. Création de l'admin
echo "👤 Création de l'utilisateur admin..."
docker exec -i superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@fab.org \
  --password admin 2>/dev/null || echo "   (utilisateur admin déjà existant, on continue)"

# 4. Initialisation de Superset
echo "⚙️  Initialisation de Superset..."
docker exec -i superset superset init

echo "-------------------------------------------------------"
echo "✅ SUPERSET PRÊT !"
echo "-------------------------------------------------------"
echo "👉 Superset : http://localhost:8088 (admin/admin)"
echo "Druid uri : druid://druid:8082/druid/v2/sql"
echo "-------------------------------------------------------"