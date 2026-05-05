#!/bin/bash

# Dossier local à vider 
DATA_DIR="./src/data"

echo "-------------------------------------------------------"
echo "🚀 RESET COMPLET ET INITIALISATION"
echo "-------------------------------------------------------"

# 1. Nettoyage radical
echo "🧹 Nettoyage des anciens conteneurs et volumes..."
# -v supprime les volumes nommés (Postgres, Druid logs, etc.)
docker compose down -v 

echo "🗑️ Vidage du dossier de données local : $DATA_DIR"
# On recrée un dossier propre pour éviter les erreurs de montage
if [ -d "$DATA_DIR" ]; then
    # On vide le contenu sans supprimer le dossier lui-même pour garder les droits
    rm -rf "${DATA_DIR:?}"/* echo "✅ Dossier $DATA_DIR vidé."
else
    mkdir -p "$DATA_DIR"
    echo "📂 Dossier $DATA_DIR créé."
fi

# 2. Redémarrage
echo "🏗️ Reconstruction et démarrage..."
docker compose up -d --build

echo "⏳ Attente du démarrage de base (45s)..."
sleep 45

# 3. Configuration PostgreSQL
echo "🐘 Postgres : Création des bases..."
# On ajoute une petite boucle car Postgres peut être lent à accepter les connexions
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE superset;" || true
docker exec -i druid-postgres psql -U druid -d metadata -c "CREATE DATABASE airflow;" || true

# 4. Permission Socket Docker pour Airflow
echo "🐳 Docker : Permission socket..."
docker exec -i airflow chmod 666 /var/run/docker.sock

# 5. Attente spécifique pour l'Overlord Druid 
echo "⏳ Attente de l'Overlord Druid (90s)..."
sleep 90

# 6. Vérification que l'Overlord Druid est prêt
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
  echo "⚠️  Overlord Druid toujours pas prêt."
fi


echo "-------------------------------------------------------"
echo "✅ SETUP TERMINÉ !"
echo "-------------------------------------------------------"
echo "👉 Airflow  : http://localhost:8080 (admin/admin)"
echo "👉 Druid    : http://localhost:8888"
echo "-------------------------------------------------------"