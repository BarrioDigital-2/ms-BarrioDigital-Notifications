#!/bin/bash
# ========== ENTRYPOINT DEL CLUSTER RABBITMQ ==========
# Lo usan los dos nodos:
# - Ambos escriben la MISMA Erlang cookie (requisito para formar cluster).
# - Sin JOIN_CLUSTER_HOST: el nodo arranca normal (rabbit1, nodo semilla).
# - Con JOIN_CLUSTER_HOST: el nodo arranca, espera al nodo semilla y se une
#   (stop_app -> join_cluster -> start_app). Si ya pertenece al cluster
#   (reinicio con volumen persistente), no vuelve a unirse.
set -euo pipefail

COOKIE_FILE=/var/lib/rabbitmq/.erlang.cookie

# ========== 1. COOKIE COMPARTIDA ==========
# La imagen enlaza /root/.erlang.cookie a este archivo,
# asi rabbitmqctl (ejecutado como root) usa la misma cookie que el servidor.
echo -n "${ERLANG_COOKIE:?Falta la variable ERLANG_COOKIE}" > "$COOKIE_FILE"
chown rabbitmq:rabbitmq "$COOKIE_FILE"
chmod 400 "$COOKIE_FILE"

# ========== 2. NODO SEMILLA ==========
if [ -z "${JOIN_CLUSTER_HOST:-}" ]; then
  exec docker-entrypoint.sh rabbitmq-server
fi

# ========== 3. NODO QUE SE UNE AL CLUSTER ==========
# Arranca en segundo plano para poder administrarlo con rabbitmqctl
docker-entrypoint.sh rabbitmq-server &
SERVER_PID=$!

echo "[CLUSTER] Esperando que el nodo local arranque..."
until rabbitmq-diagnostics -q check_running >/dev/null 2>&1; do sleep 2; done

SEED_NODE="rabbit@${JOIN_CLUSTER_HOST}"

if rabbitmqctl -q cluster_status | grep -q "$SEED_NODE"; then
  echo "[CLUSTER] Ya pertenece al cluster de $SEED_NODE, no se vuelve a unir."
else
  echo "[CLUSTER] Esperando a $SEED_NODE..."
  until rabbitmq-diagnostics -q -n "$SEED_NODE" check_running >/dev/null 2>&1; do sleep 2; done

  echo "[CLUSTER] Uniendose al cluster de $SEED_NODE..."
  rabbitmqctl stop_app
  rabbitmqctl join_cluster "$SEED_NODE"
  rabbitmqctl start_app
  echo "[CLUSTER] Nodo unido al cluster."
fi

# El contenedor vive mientras viva el servidor
wait "$SERVER_PID"
