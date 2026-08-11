#!/bin/sh
# Registers (or re-registers) the Debezium PostgreSQL connector against Kafka Connect.
# Runs as a one-shot init container so the whole stack can be brought up with a single
# `docker compose up -d`, with no manual curl step.
set -eu

CONNECT_URL="${DEBEZIUM_URL:-http://debezium:8083}"
CONNECTOR_NAME="${DEBEZIUM_CONNECTOR_NAME:-kiva-postgres-connector}"
TEMPLATE="/config/debezium_postgres_source.json.template"
RENDERED="/tmp/debezium_postgres_source.json"

echo "[register_connector] installing curl + gettext (envsubst)..."
apk add --no-cache curl gettext >/dev/null 2>&1

echo "[register_connector] waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  echo "[register_connector]   not ready yet, retrying in 3s..."
  sleep 3
done

# Render Postgres credentials from the environment into the connector config so it
# always matches whatever is set in .env, instead of hardcoding them in source control.
envsubst < "$TEMPLATE" > "$RENDERED"

EXISTING_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")
if [ "$EXISTING_STATUS" = "200" ]; then
  echo "[register_connector] connector '${CONNECTOR_NAME}' is already registered. Skipping."
  exit 0
fi

echo "[register_connector] registering connector '${CONNECTOR_NAME}'..."
HTTP_CODE=$(curl -s -o /tmp/register_response.json -w "%{http_code}" \
  -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  "${CONNECT_URL}/connectors/" -d @"$RENDERED")

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "[register_connector] connector registered successfully (HTTP ${HTTP_CODE})."
  exit 0
fi

echo "[register_connector] FAILED to register connector (HTTP ${HTTP_CODE}):"
cat /tmp/register_response.json
exit 1
