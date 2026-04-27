#!/bin/bash
set -e

echo "==> Démarrage de Druid..."
mkdir -p /opt/druid/var/druid/indexing-logs /opt/druid/var/druid/segments /opt/druid/var/sv
cd /opt/druid

CONF="conf/druid/single-server/micro-quickstart"

bin/run-druid coordinator-overlord "$CONF" \
  >> /opt/druid/var/sv/coordinator-overlord.log 2>&1 &

echo "==> Coordinator/Overlord démarré, attente 60s..."
sleep 60

bin/run-druid broker "$CONF" \
  >> /opt/druid/var/sv/broker.log 2>&1 &

bin/run-druid router "$CONF" \
  >> /opt/druid/var/sv/router.log 2>&1 &

bin/run-druid historical "$CONF" \
  >> /opt/druid/var/sv/historical.log 2>&1 &

sleep 10

bin/run-druid middleManager "$CONF" \
  >> /opt/druid/var/sv/middleManager.log 2>&1 &

echo "==> Tous les services démarrés, suivi des logs Overlord..."
tail -f /opt/druid/var/sv/coordinator-overlord.log